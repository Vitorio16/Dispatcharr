"""Channel-scoped, crash-recoverable ownership of an existing pool slot.

No URLs or credentials live in the ledger. All counter mutations use the shared
connection pool. WATCH guards make cleanup and promotion mutually exclusive.
"""
import json
import time
import logging
from uuid import uuid4

from apps.m3u.connection_pool import reserve_profile_slot, release_profile_slot
from .constants import ChannelMetadataField as Field
from .redis_keys import RedisKeys

logger = logging.getLogger(__name__)


def _text(value):
    return value.decode() if isinstance(value, bytes) else value


def reserve_standby(redis, channel_id, worker_id, stream_id, profile):
    key = RedisKeys.channel_standby(channel_id)
    owner = RedisKeys.channel_owner(channel_id)
    stopping = RedisKeys.channel_stopping(channel_id)
    token = json.dumps(dict(token=uuid4().hex, worker=worker_id, created_at=time.time(),
                           stream=int(stream_id), profile=profile.id), sort_keys=True)

    def guard(pipe):
        pipe.watch(key, owner, stopping)
        return (pipe.get(key) is None and _text(pipe.get(owner)) == worker_id
                and not pipe.exists(stopping))

    try:
        reserved, _, _ = reserve_profile_slot(
            profile, redis, guard=guard, commit=lambda pipe: pipe.set(key, token))
    except Exception:
        # A dropped EXEC reply must not leave an unowned local reservation.
        if _text(redis.get(key)) == token:
            return token
        raise
    if reserved or _text(redis.get(key)) == token:
        return token
    return None


def release_standby(redis, channel_id, token=None, *, stale_for_worker=None, only_if_dead=False):
    """Consume exactly this token; missing/already-promoted tokens are harmless.

    Without a token, release the current ledger (coordinated channel teardown).
    stale_for_worker only recovers a predecessor after ownership has changed.
    """
    key = RedisKeys.channel_standby(channel_id)
    raw = _text(redis.get(key))
    if not isinstance(raw, str) or not raw.startswith('{') or (token is not None and raw != token):
        return False
    record = json.loads(raw)
    owner = RedisKeys.channel_owner(channel_id)

    def guard(pipe):
        pipe.watch(key)
        if _text(pipe.get(key)) != raw:
            return False
        if stale_for_worker is not None:
            pipe.watch(owner)
            if (_text(pipe.get(owner)) != stale_for_worker
                    or record['worker'] == stale_for_worker):
                return False
        if only_if_dead:
            heartbeat = RedisKeys.worker_heartbeat(record['worker'])
            pipe.watch(owner, heartbeat)
            if (time.time() - record['created_at'] < 60 or pipe.exists(heartbeat)):
                return False
        return True

    return release_profile_slot(record['profile'], redis, guard=guard,
                                commit=lambda pipe: pipe.delete(key))


def recover_stale_standbys(redis):
    """Heartbeat-based janitor for ledgers orphaned by hard worker termination.

    Invoked by the existing proxy cleanup loop, even when the feature is off.
    No TTL can silently erase an unreleased slot. A minute grace period exceeds
    the normal 30-second worker heartbeat TTL and protects startup races.
    """
    for key in redis.scan_iter(match='channel_standby:*', count=100):
        channel_id = _text(key).split(':', 1)[1]
        try:
            release_standby(redis, channel_id, only_if_dead=True)
        except (ValueError, TypeError, KeyError):
            # Preserve an invalid ledger for investigation; do not let one bad
            # record prevent the existing worker cleanup loop doing its work.
            logger.warning('Invalid hot standby reservation channel=%s', channel_id)


def promote_standby(redis, channel_id, channel_pk, worker_id, token,
                    old_stream_id, old_profile_id, metadata):
    """Transfer an already reserved slot; atomically release the old active slot.

    Watching the metadata hash fences the legacy HDEL teardown token too. A
    concurrent shutdown wins or promotion wins, never both for the old slot.
    """
    key = RedisKeys.channel_standby(channel_id)
    owner = RedisKeys.channel_owner(channel_id)
    stopping = RedisKeys.channel_stopping(channel_id)
    active = RedisKeys.channel_stream(channel_pk)
    old_profile = RedisKeys.stream_profile(old_stream_id)
    meta = RedisKeys.channel_metadata(channel_id)
    record = json.loads(token)

    def guard(pipe):
        pipe.watch(key, owner, stopping, active, old_profile, meta)
        return (_text(pipe.get(key)) == token
                and _text(pipe.get(owner)) == worker_id
                and not pipe.exists(stopping)
                and _text(pipe.get(active)) == str(old_stream_id)
                and _text(pipe.get(old_profile)) == str(old_profile_id)
                and _text(pipe.hget(meta, Field.M3U_PROFILE)) == str(old_profile_id))

    def commit(pipe):
        pipe.delete(key)
        if old_stream_id != record['stream']:
            pipe.delete(old_profile)
        pipe.set(active, record['stream'])
        pipe.set(RedisKeys.stream_profile(record['stream']), record['profile'])
        pipe.hset(meta, mapping=metadata)

    return release_profile_slot(old_profile_id, redis, guard=guard, commit=commit)
