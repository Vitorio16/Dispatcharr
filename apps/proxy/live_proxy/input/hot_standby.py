"""Small coordinator for profile selection, reservations and reader handoff.

All source changes hold the same cooperative lock. Only the manager run loop
promotes; the background greenlet only warms/drains/replaces the spare.
"""
import time
import json
from types import SimpleNamespace

import gevent
from gevent.lock import RLock
from django.db import close_old_connections

from ..config_helper import ConfigHelper
from ..constants import ChannelMetadataField as Field
from ..redis_keys import RedisKeys
from ..standby_reservation import reserve_standby, release_standby, promote_standby
from ..utils import get_logger
from .standby import StandbyHTTPReader, supported_http_ts

logger = get_logger()


def _switch_event(**fields):
    from core.utils import log_system_event
    log_system_event('stream_switch', **fields)


class HotStandby:
    def __init__(self, manager):
        from apps.channels.models import Channel
        self.manager = manager
        self.redis = manager.buffer.redis_client
        self.lock = RLock()
        self.candidate = None
        self.task = None
        self.closed = False
        self._search_after = 0
        self.channel_pk = Channel.objects.only('id').get(uuid=manager.channel_id).pk
        raw = self.redis.get(RedisKeys.stream_profile(manager.current_stream_id))
        manager.current_profile_id = int(raw) if raw else None
        # Recover a token left by a killed predecessor. Reservation records have
        # no TTL: expiring a token alone would permanently leak its counters.
        release_standby(self.redis, manager.channel_id, stale_for_worker=manager.worker_id)

    @property
    def ready(self):
        candidate = self.candidate
        return bool(candidate and candidate.reader and candidate.reader.ready)

    def start(self):
        if self.task is None and not self.closed:
            self.task = gevent.spawn(self._watch)

    def _fail(self, pair):
        self.manager.failed_combinations[pair] = (
            time.monotonic() + ConfigHelper.get('HOT_STANDBY_COOLDOWN', 60))
        self.manager.tried_combinations.add(pair)

    def _candidates(self):
        from ..url_utils import get_profile_failover_candidates
        manager = self.manager
        now = time.monotonic()
        return [entry for entry in get_profile_failover_candidates(
            manager.channel_id, manager.current_stream_id, manager.current_profile_id)
            if manager.failed_combinations.get((entry['stream_id'], entry['profile_id']), 0) <= now]

    def _reserve(self, entry):
        from apps.m3u.models import M3UAccountProfile
        from ..url_utils import get_stream_info_for_switch
        manager = self.manager
        stream_id, profile_id = entry['stream_id'], entry['profile_id']
        profile = M3UAccountProfile.objects.select_related('m3u_account__server_group').get(
            pk=profile_id, is_active=True)
        token = reserve_standby(self.redis, manager.channel_id, manager.worker_id, stream_id, profile)
        if not token:
            return None
        candidate = SimpleNamespace(stream_id=stream_id, profile_id=profile_id,
                                    token=token, reader=None, info=None, announced=False,
                                    state='RESERVED')
        # Publish the local token before resolution, so finally/stop can recover
        # it even when a query or credential transform fails.
        self.candidate = candidate
        try:
            info = get_stream_info_for_switch(manager.channel_id, stream_id, profile_id)
            if 'error' in info or not info.get('url'):
                raise ValueError('Unresolved standby source')
            if not supported_http_ts(info['url'], info['transcode']):
                self._discard()
                return None  # Legacy failover still supports FFmpeg sources.
            candidate.info = info
            return candidate
        except Exception:
            self._fail((stream_id, profile_id))
            self._discard()
            return None

    def _discard(self):
        candidate = self.candidate
        if candidate is None:
            return
        if candidate.reader:
            candidate.reader.stop()
        # Keep local token if Redis fails; the next pass/teardown retries. A
        # False result also means a concurrent teardown already consumed it.
        release_standby(self.redis, self.manager.channel_id, candidate.token)
        self.candidate = None

    def _tick(self):
        manager = self.manager
        with self.lock:
            if self.closed or manager.stop_requested or not manager.running:
                return
            # Fail closed for the extra connection if Redis ownership is unknown.
            if (self.redis.get(RedisKeys.channel_owner(manager.channel_id)) not in
                    (manager.worker_id, manager.worker_id.encode())
                    or self.redis.exists(RedisKeys.channel_stopping(manager.channel_id))):
                self._discard()
                self.closed = True
                return
            candidate = self.candidate
            if candidate:
                reader = candidate.reader
                age = time.monotonic() - reader.started_at
                expired = (not reader.ready and
                           (reader.last_media_time is not None or age >= reader.warmup_timeout))
                if reader.failed or expired:
                    logger.warning('Hot standby failed channel=%s stream=%s profile=%s',
                                   manager.channel_id, candidate.stream_id, candidate.profile_id)
                    self._fail((candidate.stream_id, candidate.profile_id))
                    self._discard()
                    self._search_after = 0
                elif reader.ready and not candidate.announced:
                    candidate.state = 'READY'
                    candidate.announced = True
                    logger.info('Hot standby ready channel=%s stream=%s profile=%s',
                                manager.channel_id, candidate.stream_id, candidate.profile_id)
                return
            if not supported_http_ts(manager.url, manager.transcode) or manager.url_switching:
                return
            if time.monotonic() < getattr(self, '_search_after', 0):
                return
            self._search_after = time.monotonic() + ConfigHelper.get('HOT_STANDBY_SEARCH_INTERVAL', 5)
            # Recover our own committed-but-unacknowledged reserve after a
            # temporary Redis outage. No reader owns it when candidate is None.
            orphan = self.redis.get(RedisKeys.channel_standby(manager.channel_id))
            if orphan and json.loads(orphan)['worker'] == manager.worker_id:
                release_standby(self.redis, manager.channel_id,
                                orphan.decode() if isinstance(orphan, bytes) else orphan)
            previous_stream = manager.current_stream_id
            for entry in self._candidates():
                if self.closed or manager.stop_requested:
                    return
                if entry['stream_id'] != previous_stream:
                    logger.info('Same-stream profiles exhausted; moving standby search to next stream channel=%s stream=%s',
                                manager.channel_id, entry['stream_id'])
                    previous_stream = entry['stream_id']
                candidate = self._reserve(entry)
                if candidate is None:
                    continue
                logger.info('Hot standby warming channel=%s stream=%s profile=%s',
                            manager.channel_id, candidate.stream_id, candidate.profile_id)
                try:
                    candidate.reader = StandbyHTTPReader(
                        candidate.info['url'], candidate.info['user_agent'], manager.chunk_size,
                        warmup_timeout=ConfigHelper.get('HOT_STANDBY_WARMUP_TIMEOUT', 10),
                        idle_timeout=ConfigHelper.get('HOT_STANDBY_IDLE_TIMEOUT', 10))
                    candidate.reader.start()
                    candidate.state = 'WARMING'
                    return
                except Exception:
                    self._fail((candidate.stream_id, candidate.profile_id))
                    self._discard()

    def _watch(self):
        try:
            while not self.closed and self.manager.running and not self.manager.stop_requested:
                try:
                    self._tick()
                except Exception as exc:
                    logger.warning('Hot standby maintenance failed channel=%s (%s)',
                                   self.manager.channel_id, type(exc).__name__)
                    try:
                        with self.lock:
                            self._discard()
                    except Exception:
                        # Keep the token and retry after Redis recovers, instead
                        # of terminating the only owner of the local cleanup.
                        gevent.sleep(1)
                finally:
                    close_old_connections()
                gevent.sleep(0.25)
        finally:
            try:
                with self.lock:
                    self._discard()
            except Exception:
                logger.warning('Hot standby release deferred to recovery channel=%s',
                               self.manager.channel_id)
            close_old_connections()

    def _handoff(self, candidate, *, hot):
        manager = self.manager
        old_stream, old_profile = manager.current_stream_id, manager.current_profile_id
        info = candidate.info
        metadata = {
            Field.STREAM_ID: str(candidate.stream_id), Field.M3U_PROFILE: str(candidate.profile_id),
            Field.URL: info['url'], Field.USER_AGENT: info['user_agent'],
            Field.STREAM_PROFILE: str(info['stream_profile']), Field.STREAM_NAME: info['stream_name'],
            Field.STREAM_SWITCH_TIME: str(time.time()),
            Field.STREAM_SWITCH_REASON: 'hot_standby_promotion' if hot else 'profile_failover',
        }
        # Close the old input before releasing its slot, and before any new
        # bytes can be written. No call to update_url: it would reserve again.
        manager._close_socket()
        if self.closed or manager.stop_requested:
            return False
        manager.buffer.mark_discontinuity()
        def committed():
            def text(value):
                return value.decode() if isinstance(value, bytes) else value
            try:
                return (
                    self.redis.get(RedisKeys.channel_standby(manager.channel_id)) is None
                    and text(self.redis.get(RedisKeys.channel_owner(manager.channel_id))) == manager.worker_id
                    and text(self.redis.get(RedisKeys.channel_stream(self.channel_pk))) == str(candidate.stream_id)
                    and text(self.redis.hget(RedisKeys.channel_metadata(manager.channel_id), Field.M3U_PROFILE))
                    == str(candidate.profile_id))
            except Exception:
                # Unknown transaction outcome: never reconnect the old URL using
                # a possibly released slot. End this manager and leave durable
                # tokens for coordinated teardown/recovery when Redis returns.
                manager.running = False
                if candidate.reader:
                    candidate.reader.stop()
                raise

        try:
            transferred = promote_standby(
                self.redis, manager.channel_id, self.channel_pk, manager.worker_id,
                candidate.token, old_stream, old_profile, metadata)
        except Exception:
            # EXEC can succeed while its reply is lost. Read back the active
            # ownership token before deciding which cleanup owns this slot.
            transferred = committed()
            if not transferred:
                raise
        # redis-py can surface a lost watched EXEC reply as WatchError. The
        # shared helper may then return False after re-reading a consumed token.
        if not transferred and not committed():
            return False
        # Token is now the ACTIVE metadata token; cleanup must never release it
        # through the standby path, including if attaching the fd fails.
        self.candidate = None
        self._search_after = 0
        manager.current_stream_id = candidate.stream_id
        manager.current_profile_id = candidate.profile_id
        manager.url, manager.user_agent = info['url'], info['user_agent']
        manager.transcode = False
        manager.tried_combinations.add((candidate.stream_id, candidate.profile_id))
        manager.tried_stream_ids.add(candidate.stream_id)
        self._fail((old_stream, old_profile))
        manager._clear_connection_failure_history()
        manager.needs_reconnect = manager.needs_stream_switch = False
        manager._startup_failover_exhausted = False
        manager.buffering_timeout_error_stream_id = manager._NO_BUFFERING_TIMEOUT_ERROR
        manager._smoothed_output_bitrate = None
        manager._last_bitrate_db_save_time = 0
        manager._bitrate_warmup_samples = 10
        manager.connection_start_time = manager.last_data_time = time.time()
        manager._connection_awaiting_data = not hot
        manager.healthy = True
        if hot:
            try:
                manager.socket = candidate.reader.promote()
                manager.http_reader = candidate.reader
                manager.connected = True
            except Exception:
                candidate.reader.stop()
                manager.connected = False
                # Assignment is committed; normal reconnect owns the slot now.
        try:
            _switch_event(channel_id=manager.channel_id,
                             channel_name=manager.channel_name, reason=metadata[Field.STREAM_SWITCH_REASON],
                             previous_stream_id=old_stream, stream_id=candidate.stream_id,
                             previous_m3u_profile_id=old_profile, m3u_profile_id=candidate.profile_id)
        except Exception:
            logger.warning('Could not log standby switch event channel=%s', manager.channel_id)
        logger.info('Hot standby promoted channel=%s stream=%s profile=%s hot=%s',
                    manager.channel_id, candidate.stream_id, candidate.profile_id, hot)
        logger.info('Preparing replacement standby channel=%s', manager.channel_id)
        return True

    def promote(self):
        with self.lock:
            candidate = self.candidate
            if (self.closed or self.manager.stop_requested or not candidate
                    or not candidate.reader or not candidate.reader.ready):
                return False
            logger.info('Promoting hot standby channel=%s stream=%s profile=%s',
                        self.manager.channel_id, candidate.stream_id, candidate.profile_id)
            candidate.state = 'PROMOTING'
            try:
                if self._handoff(candidate, hot=True):
                    return True
            except Exception as exc:
                logger.warning('Hot standby promotion failed channel=%s (%s)',
                               self.manager.channel_id, type(exc).__name__)
            self._discard()
            return False

    def cold_switch(self):
        """Same ordering/accounting when no spare has finished warming."""
        with self.lock:
            if self.closed or self.manager.stop_requested:
                return False
            self._fail((self.manager.current_stream_id, self.manager.current_profile_id))
            self._discard()
            for entry in self._candidates():
                candidate = self._reserve(entry)
                if candidate:
                    try:
                        if self._handoff(candidate, hot=False):
                            return True
                    finally:
                        self._discard()
            return False

    def close(self):
        with self.lock:
            self.closed = True
            self._discard()
        if self.task and self.task is not gevent.getcurrent():
            self.task.kill(block=True, timeout=1)
