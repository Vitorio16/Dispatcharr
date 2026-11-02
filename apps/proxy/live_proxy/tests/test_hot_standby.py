"""Portable lifecycle tests; runnable with unittest, without PostgreSQL/Linux.

The transaction double queues writes until EXEC and implements WATCH for both
strings and hashes. Failures can be injected before EXEC or after a lost reply.
"""
import copy
import json
import time
import unittest
import os
from types import SimpleNamespace
from unittest.mock import Mock, patch

from gevent.lock import RLock
from redis.exceptions import WatchError

from apps.m3u.connection_pool import reserve_profile_slot
from apps.proxy.live_proxy.constants import ChannelMetadataField as Field
from apps.proxy.live_proxy.redis_keys import RedisKeys
from apps.proxy.live_proxy.standby_reservation import (
    reserve_standby, release_standby, promote_standby, recover_stale_standbys,
)
from apps.proxy.live_proxy.input.standby import StandbyHTTPReader, supported_http_ts
from apps.proxy.live_proxy.input.hot_standby import HotStandby


class MemoryRedis:
    def __init__(self):
        self.data = {}
        self.before_exec = self.after_exec = None

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = str(value).encode() if not isinstance(value, bytes) else value

    def exists(self, key):
        return key in self.data

    def delete(self, key):
        return self.data.pop(key, None) is not None

    def incr(self, key):
        value = int(self.get(key) or 0) + 1
        self.set(key, value)
        return value

    def decr(self, key):
        value = int(self.get(key) or 0) - 1
        self.set(key, value)
        return value

    def hget(self, key, field):
        return self.data.get(key, {}).get(field)

    def hset(self, key, mapping):
        bucket = self.data.setdefault(key, {})
        bucket.update({k: str(v).encode() for k, v in mapping.items()})

    def pipeline(self):
        return MemoryPipeline(self)

    def scan_iter(self, match, count=100):
        import fnmatch
        return iter([key for key in self.data if fnmatch.fnmatch(key, match)])


class MemoryPipeline:
    def __init__(self, redis):
        self.redis = redis
        self.watched = {}
        self.ops = []
        self.in_multi = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def watch(self, *keys):
        for key in keys:
            self.watched[key] = copy.deepcopy(self.redis.data.get(key))

    def multi(self):
        self.in_multi = True

    def __getattr__(self, name):
        def command(*args, **kwargs):
            if self.in_multi:
                self.ops.append((name, args, kwargs))
                return self
            return getattr(self.redis, name)(*args, **kwargs)
        return command

    def execute(self):
        if self.redis.before_exec:
            callback, self.redis.before_exec = self.redis.before_exec, None
            callback(self)
        if any(self.redis.data.get(k) != v for k, v in self.watched.items()):
            raise WatchError()
        for name, args, kwargs in self.ops:
            getattr(self.redis, name)(*args, **kwargs)
        if self.redis.after_exec:
            callback, self.redis.after_exec = self.redis.after_exec, None
            callback(self)


def profile(pk, maximum=4):
    return SimpleNamespace(id=pk, max_streams=maximum,
                           m3u_account=SimpleNamespace(server_group=None))


class ReservationFixture:
    def setUp(self):
        self.redis = MemoryRedis()
        self.channel = 'test-channel'
        self.redis.set(RedisKeys.channel_owner(self.channel), 'worker')
        reserve_profile_slot(profile(1), self.redis)
        self.redis.set(RedisKeys.channel_stream(9), 100)
        self.redis.set(RedisKeys.stream_profile(100), 1)
        self.redis.hset(RedisKeys.channel_metadata(self.channel),
                        {Field.STREAM_ID: 100, Field.M3U_PROFILE: 1})

    def reserve(self, pk=2, maximum=4, stream=100):
        return reserve_standby(self.redis, self.channel, 'worker', stream, profile(pk, maximum))

    def count(self, pk):
        return int(self.redis.get(f'profile_connections:{pk}') or 0)

    def promote(self, token):
        return promote_standby(self.redis, self.channel, 9, 'worker', token, 100, 1,
                               {Field.STREAM_ID: 100, Field.M3U_PROFILE: 2})


class ReservationTests(ReservationFixture, unittest.TestCase):
    def test_unlimited_active_reservation_is_also_counted(self):
        self.assertEqual(reserve_profile_slot(profile(3, 0), self.redis), (True, 1, None))
        self.assertEqual(self.count(3), 1)

    def test_lost_reserve_reply_recovers_owned_token(self):
        self.redis.after_exec = Mock(side_effect=ConnectionError('lost reserve reply'))
        token = self.reserve()
        self.assertIsNotNone(token)
        self.assertEqual(self.count(2), 1)
        release_standby(self.redis, self.channel, token)
        self.assertEqual(self.count(2), 0)

    def test_dead_worker_janitor_reclaims_orphan_even_without_channel_metadata(self):
        token = self.reserve()
        record = json.loads(token)
        record['created_at'] -= 61
        self.redis.set(RedisKeys.channel_standby(self.channel), json.dumps(record))
        self.redis.delete(RedisKeys.channel_metadata(self.channel))
        recover_stale_standbys(self.redis)
        self.assertEqual(self.count(2), 0)
        recover_stale_standbys(self.redis)
        self.assertEqual(self.count(2), 0)

    def test_janitor_does_not_reclaim_live_worker(self):
        token = self.reserve()
        record = json.loads(token)
        record['created_at'] -= 61
        self.redis.set(RedisKeys.channel_standby(self.channel), json.dumps(record))
        self.redis.set(RedisKeys.worker_heartbeat('worker'), 1)
        recover_stale_standbys(self.redis)
        self.assertEqual(self.count(2), 1)

    def test_local_janitor_recovers_failed_cleanup_without_waiting_for_worker_death(self):
        token = self.reserve()
        record = json.loads(token)
        record['created_at'] -= 61
        token = json.dumps(record)
        self.redis.set(RedisKeys.channel_standby(self.channel), token)
        self.redis.set(RedisKeys.worker_heartbeat('worker'), 1)
        recover_stale_standbys(self.redis, local_worker_id='worker', local_tokens={token})
        self.assertEqual(self.count(2), 1)
        recover_stale_standbys(self.redis, local_worker_id='worker', local_tokens=set())
        self.assertEqual(self.count(2), 0)

    def test_reservation_is_separate_and_counted(self):
        token = self.reserve()
        self.assertEqual(json.loads(token)['profile'], 2)
        self.assertEqual(self.count(2), 1)
        self.assertEqual(self.redis.get(RedisKeys.stream_profile(100)), b'1')
        self.assertIsNone(self.reserve(3))  # Only one owned spare per channel.

    def test_stop_and_double_cleanup_release_once(self):
        token = self.reserve()
        self.assertTrue(release_standby(self.redis, self.channel, token))
        self.assertFalse(release_standby(self.redis, self.channel, token))
        self.assertEqual(self.count(2), 0)
        self.assertEqual(self.count(1), 1)

    def test_same_stream_promotion_transfers_without_double_count(self):
        token = self.reserve()
        self.assertTrue(self.promote(token))
        self.assertEqual((self.count(1), self.count(2)), (0, 1))
        self.assertEqual(self.redis.get(RedisKeys.stream_profile(100)), b'2')
        self.assertIsNone(self.redis.get(RedisKeys.channel_standby(self.channel)))
        self.assertFalse(release_standby(self.redis, self.channel, token))
        self.assertFalse(self.promote(token))
        self.assertEqual((self.count(1), self.count(2)), (0, 1))
        self.assertIsNotNone(self.reserve(3))
        self.assertEqual(self.count(3), 1)

    def test_promotion_failure_before_exec_cannot_remove_token_partway(self):
        token = self.reserve()
        def fail(pipe):
            # DELETE and active SETs are queued, not executed individually.
            self.assertIn(('delete', (RedisKeys.channel_standby(self.channel),), {}), pipe.ops)
            self.assertIsNotNone(self.redis.get(RedisKeys.channel_standby(self.channel)))
            raise RuntimeError('injected after queued token removal')
        self.redis.before_exec = fail
        with self.assertRaises(RuntimeError):
            self.promote(token)
        self.assertEqual((self.count(1), self.count(2)), (1, 1))
        release_standby(self.redis, self.channel, token)
        self.assertEqual((self.count(1), self.count(2)), (1, 0))

    def test_exec_reply_lost_cleanup_does_not_release_active_slot(self):
        token = self.reserve()
        self.redis.after_exec = Mock(side_effect=ConnectionError('lost EXEC reply'))
        with self.assertRaises(ConnectionError):
            self.promote(token)
        self.assertFalse(release_standby(self.redis, self.channel, token))
        self.assertEqual((self.count(1), self.count(2)), (0, 1))

    def test_failed_reserve_does_not_leak_counter(self):
        self.redis.before_exec = Mock(side_effect=RuntimeError('before commit'))
        with self.assertRaises(RuntimeError):
            self.reserve()
        self.assertEqual(self.count(2), 0)
        self.assertIsNone(self.redis.get(RedisKeys.channel_standby(self.channel)))

    def test_owner_loss_blocks_promotion_and_reservation(self):
        token = self.reserve()
        self.redis.set(RedisKeys.channel_owner(self.channel), 'replacement')
        self.assertFalse(self.promote(token))
        self.assertIsNone(self.reserve(3))
        self.assertTrue(release_standby(self.redis, self.channel, token))
        self.assertEqual(self.count(2), 0)

    def test_stale_predecessor_can_be_recovered_but_current_owner_cannot(self):
        self.reserve()
        self.assertFalse(release_standby(self.redis, self.channel, stale_for_worker='worker'))
        self.redis.set(RedisKeys.channel_owner(self.channel), 'replacement')
        self.assertTrue(release_standby(self.redis, self.channel, stale_for_worker='replacement'))
        self.assertEqual(self.count(2), 0)

    def test_teardown_wins_watch_race_without_double_release(self):
        token = self.reserve()
        self.redis.before_exec = lambda _: release_standby(self.redis, self.channel, token)
        self.assertFalse(self.promote(token))
        self.assertEqual((self.count(1), self.count(2)), (1, 0))

    def test_shutdown_blocks_reservation(self):
        self.redis.set(RedisKeys.channel_stopping(self.channel), 'true')
        self.assertIsNone(self.reserve())
        self.assertEqual(self.count(2), 0)

    def test_exhausted_capacity_skips_reservation(self):
        reserve_profile_slot(profile(2, 1), self.redis)
        self.assertIsNone(self.reserve(2, 1))
        self.assertEqual(self.count(2), 1)

    def test_zero_means_unlimited(self):
        self.redis.set('profile_connections:2', 100)
        token = self.reserve(2, 0)
        self.assertIsNotNone(token)
        self.assertEqual(self.count(2), 101)
        release_standby(self.redis, self.channel, token)
        self.assertEqual(self.count(2), 100)

    def test_shared_credential_limit_is_enforced_and_released(self):
        import apps.m3u.connection_pool as pool
        pooled = profile(2, 1)
        pooled.m3u_account.server_group = SimpleNamespace(id=5)
        with patch.object(pool, '_held_credential_slot_key', return_value='shared-login'):
            self.redis.set('shared-login', 1)
            token = reserve_standby(self.redis, self.channel, 'worker', 100, pooled)
            self.assertIsNone(token)
            self.assertEqual(self.count(2), 0)
            self.redis.set('shared-login', 0)
            token = reserve_standby(self.redis, self.channel, 'worker', 100, pooled)
            self.assertEqual(self.redis.get('shared-login'), b'1')
            release_standby(self.redis, self.channel, token)
            self.assertEqual(self.redis.get('shared-login'), b'0')


PACKET = bytes([0x47, 0x00, 0x10, 0x10]) + bytes(184)


class ReaderTests(unittest.TestCase):
    def reader(self):
        reader = StandbyHTTPReader('https://host/live/private/secret/1.ts')
        reader.running = True
        return reader

    def test_no_data_is_never_ready(self):
        self.assertFalse(self.reader().ready)

    def test_invalid_media_and_partial_packets_are_not_ready(self):
        reader = self.reader()
        reader._accept(b'<html>unauthorized</html>' * 1000)
        self.assertFalse(reader.ready)
        self.assertLess(len(reader._pending), 564)

    def test_real_ts_data_marks_ready_and_is_discarded(self):
        reader = self.reader()
        with patch('os.write') as write:
            reader._accept(PACKET * 3)
        self.assertTrue(reader.ready)
        write.assert_not_called()

    def test_fragmented_data_and_promotion_keep_packet_boundary(self):
        reader = self.reader()
        reader._accept(PACKET * 3 + PACKET[:37])
        reader.active = True
        with patch('os.write', side_effect=lambda _, data: len(data)) as write:
            reader._accept(PACKET[37:] + PACKET)
        self.assertEqual(write.call_args.args[1], PACKET * 2)
        self.assertEqual(len(reader._pending), 0)

    def test_ready_expires_when_stream_stalls(self):
        reader = self.reader()
        reader._accept(PACKET * 3)
        reader.last_media_time -= reader.idle_timeout + 1
        self.assertFalse(reader.ready)

    def test_does_not_reconnect_on_promotion(self):
        reader = self.reader()
        reader._accept(PACKET * 3)
        reader.pipe_read = 42
        source = Mock()
        with patch('os.fdopen', return_value=source), patch('requests.Session') as session:
            self.assertIs(reader.promote(), source)
        session.assert_not_called()
        self.assertTrue(reader.active)
        self.assertIsNone(reader.pipe_read)

    def test_unsupported_transports_and_transcode_are_excluded(self):
        for url in ('http://host/playlist.m3u8', 'rtsp://host/x', 'udp://host/x'):
            self.assertFalse(supported_http_ts(url))
        self.assertFalse(supported_http_ts('https://host/live.ts', True))
        self.assertTrue(supported_http_ts('https://host/live.ts'))

    def test_errors_never_log_credentials(self):
        reader = self.reader()
        with patch('requests.Session', side_effect=RuntimeError(reader.url)):
            with self.assertLogs('live_proxy', level='WARNING') as logs:
                reader._run()
        self.assertNotIn('private', '\n'.join(logs.output))
        self.assertNotIn('secret', '\n'.join(logs.output))
        self.assertTrue(reader.failed)


class CooperativeHTTPTests(unittest.TestCase):
    def test_real_http_response_survives_promotion(self):
        from gevent.monkey import is_module_patched
        if not is_module_patched('socket'):
            self.skipTest('Run in a separate process with gevent.monkey.patch_all()')
        import gevent
        from gevent.pywsgi import WSGIServer
        requests_seen = []
        def application(environ, start_response):
            requests_seen.append(environ['PATH_INFO'])
            start_response('200 OK', [('Content-Type', 'video/mp2t')])
            while True:
                yield PACKET * 3
                gevent.sleep(0.02)
        server = WSGIServer(('127.0.0.1', 0), application, log=None)
        server.start()
        reader = StandbyHTTPReader(f'http://127.0.0.1:{server.server_port}/live.ts', chunk_size=564)
        source = None
        try:
            reader.start()
            with gevent.Timeout(3):
                while not reader.ready:
                    gevent.sleep(0.01)
                response = reader.response
                source = reader.promote()
                gevent.sleep(0.03)
                self.assertEqual(os.read(source.fileno(), 564), PACKET * 3)
                self.assertIs(reader.response, response)
                self.assertEqual(requests_seen, ['/live.ts'])
        finally:
            reader.stop()
            if source:
                source.close()
            server.stop(timeout=1)


class CoordinatorTests(ReservationFixture, unittest.TestCase):
    # Reuse the seeded accounting fixture for end-to-end handoff checks.
    def controller(self):
        manager = SimpleNamespace(
            channel_id=self.channel, current_stream_id=100, current_profile_id=1,
            worker_id='worker', channel_name='Channel', url='http://host/a.ts',
            transcode=False, url_switching=False, running=True, stop_requested=False,
            failed_combinations={}, tried_combinations=set(), tried_stream_ids={100},
            _close_socket=Mock(), _clear_connection_failure_history=Mock(),
            buffer=SimpleNamespace(mark_discontinuity=Mock()),
            _NO_BUFFERING_TIMEOUT_ERROR=object(), chunk_size=8192)
        controller = HotStandby.__new__(HotStandby)
        controller.manager, controller.redis = manager, self.redis
        controller.lock, controller.closed = RLock(), False
        controller.task, controller.channel_pk = None, 9
        controller.candidate = None
        return controller

    def ready_candidate(self, controller):
        token = self.reserve()
        reader = Mock(ready=True, failed=False, started_at=time.monotonic(),
                      warmup_timeout=10, last_media_time=time.monotonic())
        candidate = SimpleNamespace(stream_id=100, profile_id=2, token=token,
                                    reader=reader, state='READY', announced=True,
                                    info=dict(url='http://host/b.ts', user_agent='agent',
                                              stream_profile=1, stream_name='A'))
        controller.candidate = candidate
        return candidate

    @patch('apps.proxy.live_proxy.input.hot_standby._switch_event')
    def test_handoff_preserves_source_marks_discontinuity_and_counts(self, event):
        controller = self.controller()
        candidate = self.ready_candidate(controller)
        self.assertTrue(controller.promote())
        self.assertIs(controller.manager.socket, candidate.reader.promote.return_value)
        candidate.reader.stop.assert_not_called()
        controller.manager.buffer.mark_discontinuity.assert_called_once()
        self.assertEqual(controller.manager.current_profile_id, 2)
        self.assertEqual((self.count(1), self.count(2)), (0, 1))
        self.assertIsNone(controller.candidate)
        self.assertEqual(event.call_args.kwargs['previous_stream_id'], 100)
        self.assertEqual(event.call_args.kwargs['stream_id'], 100)
        self.assertEqual(event.call_args.kwargs['reason'], 'hot_standby_promotion')

    @patch('apps.proxy.live_proxy.input.hot_standby._switch_event')
    def test_lost_exec_reply_reconciles_committed_promotion(self, _):
        controller = self.controller()
        candidate = self.ready_candidate(controller)
        self.redis.after_exec = Mock(side_effect=ConnectionError('lost reply'))
        self.assertTrue(controller.promote())
        self.assertEqual(controller.manager.current_profile_id, 2)
        candidate.reader.stop.assert_not_called()
        self.assertEqual((self.count(1), self.count(2)), (0, 1))

    @patch('apps.proxy.live_proxy.input.hot_standby._switch_event')
    def test_watcherror_after_exec_reconciles_committed_promotion(self, _):
        controller = self.controller()
        candidate = self.ready_candidate(controller)
        self.redis.after_exec = Mock(side_effect=WatchError('lost watched EXEC reply'))
        self.assertTrue(controller.promote())
        self.assertEqual(controller.manager.current_profile_id, 2)
        candidate.reader.stop.assert_not_called()
        self.assertEqual((self.count(1), self.count(2)), (0, 1))

    def test_unknown_exec_outcome_stops_instead_of_reconnecting_unreserved_old_source(self):
        controller = self.controller()
        candidate = self.ready_candidate(controller)
        original_get = self.redis.get
        def disconnect_after_commit(_):
            self.redis.get = Mock(side_effect=ConnectionError('Redis unavailable'))
            raise ConnectionError('lost reply')
        self.redis.after_exec = disconnect_after_commit
        with self.assertRaises(ConnectionError):
            controller.promote()
        self.assertFalse(controller.manager.running)
        candidate.reader.stop.assert_called()
        self.redis.get = original_get
        controller.close()
        self.assertEqual((self.count(1), self.count(2)), (0, 1))
        self.assertEqual(self.redis.get(RedisKeys.stream_profile(100)), b'2')

    def test_failed_handoff_cleans_standby_without_double_release(self):
        controller = self.controller()
        candidate = self.ready_candidate(controller)
        self.redis.before_exec = Mock(side_effect=RuntimeError('failure before commit'))
        self.assertFalse(controller.promote())
        controller.close()
        candidate.reader.stop.assert_called()
        self.assertEqual((self.count(1), self.count(2)), (1, 0))

    def test_attach_failure_leaves_slot_owned_by_active_assignment(self):
        controller = self.controller()
        candidate = self.ready_candidate(controller)
        candidate.reader.promote.side_effect = OSError('fd closed')
        with patch('apps.proxy.live_proxy.input.hot_standby._switch_event'):
            self.assertTrue(controller.promote())
        controller.close()
        self.assertFalse(controller.manager.connected)
        self.assertEqual(controller.manager.current_profile_id, 2)
        self.assertEqual((self.count(1), self.count(2)), (0, 1))

    def test_failed_standby_does_not_touch_active(self):
        controller = self.controller()
        candidate = self.ready_candidate(controller)
        candidate.reader.failed = True
        controller._tick()
        controller.manager._close_socket.assert_not_called()
        self.assertEqual((self.count(1), self.count(2)), (1, 0))
        self.assertGreater(controller.manager.failed_combinations[(100, 2)], time.monotonic())

    def test_ownership_loss_cleans_spare(self):
        controller = self.controller()
        self.ready_candidate(controller)
        self.redis.set(RedisKeys.channel_owner(self.channel), 'new-worker')
        controller._tick()
        self.assertTrue(controller.closed)
        self.assertEqual((self.count(1), self.count(2)), (1, 0))

    def test_warmup_timeout_is_not_ready(self):
        controller = self.controller()
        candidate = self.ready_candidate(controller)
        candidate.reader.ready = False
        candidate.reader.last_media_time = None
        candidate.reader.started_at -= 11
        controller._tick()
        self.assertIsNone(controller.candidate)
        self.assertEqual(self.count(2), 0)

    def test_close_twice_stops_and_releases_spare_once(self):
        controller = self.controller()
        candidate = self.ready_candidate(controller)
        controller.close()
        controller.close()
        candidate.reader.stop.assert_called_once()
        self.assertEqual((self.count(1), self.count(2)), (1, 0))


if __name__ == '__main__':
    unittest.main()
