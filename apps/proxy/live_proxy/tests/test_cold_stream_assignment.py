"""Normal (non-standby) switches must move the whole active assignment."""
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import TestCase

from apps.channels.models import Channel, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile, ServerGroup
from apps.m3u.connection_pool import (
    get_credential_connection_count, get_profile_connection_count, reserve_profile_slot,
)
from apps.m3u.tests.test_connection_pool import FakeRedis
from apps.proxy.live_proxy.constants import ChannelMetadataField as Field
from apps.proxy.live_proxy.input.hot_standby import HotStandby
from apps.proxy.live_proxy.redis_keys import RedisKeys
from apps.proxy.live_proxy.tests.test_health_reconnect import _make_manager


class ColdStreamAssignmentTests(TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.group = ServerGroup.objects.create(name='cold-switch-group')
        self.profiles = []
        self.streams = []
        for name in ('A', 'B'):
            account = M3UAccount.objects.create(
                name=f'Provider {name}', account_type='XC', username=f'user-{name}',
                password=f'password-{name}', server_url='http://provider.example',
                server_group=self.group, max_streams=3)
            profile = account.profiles.get(is_default=True)
            self.profiles.append(profile)
            self.streams.append(Stream.objects.create(
                name=f'Stream {name}', m3u_account=account,
                url=f'http://provider.example/{name}.ts'))
        self.old_profile, self.new_profile = self.profiles
        self.old_stream, self.new_stream = self.streams
        self.channel = Channel.objects.create(channel_number=701, name='Cold switch')
        self.channel.streams.add(*self.streams)
        self.metadata = RedisKeys.channel_metadata(str(self.channel.uuid))
        self.assertTrue(reserve_profile_slot(self.old_profile, self.redis)[0])
        self.redis.set(RedisKeys.channel_stream(self.channel.id), self.old_stream.id)
        self.redis.set(RedisKeys.stream_profile(self.old_stream.id), self.old_profile.id)
        self.redis.hset(self.metadata, mapping={
            Field.STREAM_ID: str(self.old_stream.id), Field.M3U_PROFILE: str(self.old_profile.id)})
        self.manager = _make_manager(
            channel_id=str(self.channel.uuid), worker_id='worker',
            url=self.old_stream.url, current_stream_id=self.old_stream.id,
            current_profile_id=self.old_profile.id, socket=None,
            tried_stream_ids={self.old_stream.id},
            buffer=SimpleNamespace(channel_id=str(self.channel.uuid), redis_client=self.redis,
                                   mark_discontinuity=Mock(), stopping=False))
        self.enterContext(patch('core.utils.RedisClient.get_client', return_value=self.redis))
        # The manager deliberately closes its ORM connection after a switch;
        # don't close TestCase's transaction-wrapped SQLite connection here.
        self.enterContext(patch('django.db.connection'))
        self.enterContext(patch('apps.proxy.live_proxy.input.manager.log_system_event'))

    def assert_assignment(self, stream, profile):
        self.assertEqual(int(self.redis.get(RedisKeys.channel_stream(self.channel.id))), stream.id)
        self.assertEqual(int(self.redis.get(RedisKeys.stream_profile(stream.id))), profile.id)
        if stream.id != self.old_stream.id:
            self.assertIsNone(self.redis.get(RedisKeys.stream_profile(self.old_stream.id)))
        self.assertEqual(self.redis.hget(self.metadata, Field.STREAM_ID), str(stream.id))
        self.assertEqual(self.redis.hget(self.metadata, Field.M3U_PROFILE), str(profile.id))
        self.assertEqual(self.manager.current_stream_id, stream.id)
        self.assertEqual(self.manager.current_profile_id, profile.id)

    def switch(self, stream=None, profile=None):
        stream = stream or self.new_stream
        profile = profile or self.new_profile
        return self.manager.update_url(stream.url, stream.id, profile.id, reason='max_retries_exceeded')

    def test_normal_failover_moves_assignment_and_initializes_hot_standby(self):
        info = dict(url=self.new_stream.url, user_agent='agent', transcode=False,
                    stream_profile=1, stream_name=self.new_stream.name)
        with patch('apps.proxy.live_proxy.input.manager.get_alternate_streams', return_value=[
                dict(stream_id=self.new_stream.id, profile_id=self.new_profile.id)]), \
                patch('apps.proxy.live_proxy.input.manager.get_stream_info_for_switch', return_value=info) as resolve:
            self.assertTrue(self.manager._try_next_stream(reason='max_retries_exceeded'))
        resolve.assert_called_once_with(str(self.channel.uuid), self.new_stream.id, self.new_profile.id)
        self.assert_assignment(self.new_stream, self.new_profile)
        self.assertEqual(get_profile_connection_count(self.old_profile, self.redis), 0)
        self.assertEqual(get_profile_connection_count(self.new_profile, self.redis), 1)
        self.assertEqual(get_credential_connection_count(self.old_profile, self.redis), 0)
        self.assertEqual(get_credential_connection_count(self.new_profile, self.redis), 1)
        # Force initialization to obtain the profile from the new stream key,
        # not from the manager's previous in-memory value or metadata fallback.
        self.manager.current_profile_id = None
        standby = HotStandby(self.manager)
        self.assertEqual(self.manager.current_profile_id, self.new_profile.id)
        standby.close()

    def test_manual_switch_updates_keys_metadata_and_manager_together(self):
        self.assertTrue(self.switch())
        self.assert_assignment(self.new_stream, self.new_profile)

    def test_same_profile_different_stream_does_not_reserve_again(self):
        other = Stream.objects.create(name='Other A', m3u_account=self.old_profile.m3u_account,
                                      url='http://provider.example/other-A.ts')
        self.channel.streams.add(other)
        self.assertTrue(self.switch(other, self.old_profile))
        self.assert_assignment(other, self.old_profile)
        self.assertEqual(get_profile_connection_count(self.old_profile, self.redis), 1)
        self.assertEqual(get_credential_connection_count(self.old_profile, self.redis), 1)

    def test_same_stream_profile_switch_still_transfers_one_slot(self):
        alt = M3UAccountProfile.objects.create(
            m3u_account=self.old_profile.m3u_account, name='alternate A',
            is_active=True, is_default=False, max_streams=3)
        self.assertTrue(self.manager.update_url('http://provider.example/alternate-A.ts',
                                                self.old_stream.id, alt.id))
        self.assert_assignment(self.old_stream, alt)
        self.assertEqual(get_profile_connection_count(self.old_profile, self.redis), 0)
        self.assertEqual(get_profile_connection_count(alt, self.redis), 1)
        self.assertEqual(get_credential_connection_count(alt, self.redis), 1)

    def test_switch_preserves_other_reservations_and_repeated_transfer_is_idempotent(self):
        self.assertTrue(reserve_profile_slot(self.old_profile, self.redis)[0])
        self.assertTrue(reserve_profile_slot(self.new_profile, self.redis)[0])
        self.assertTrue(self.switch())
        self.assertTrue(self.channel.update_stream_profile(self.new_profile.id, self.new_stream.id))
        self.assertEqual(get_profile_connection_count(self.old_profile, self.redis), 1)
        self.assertEqual(get_profile_connection_count(self.new_profile, self.redis), 2)
        self.assertEqual(get_credential_connection_count(self.old_profile, self.redis), 1)
        self.assertEqual(get_credential_connection_count(self.new_profile, self.redis), 2)
        self.assert_assignment(self.new_stream, self.new_profile)

    def test_capacity_failure_does_not_advance_manager_or_assignment(self):
        self.new_profile.max_streams = 1
        self.new_profile.save()
        self.assertTrue(reserve_profile_slot(self.new_profile, self.redis)[0])
        with patch.object(self.manager, '_close_connection') as close:
            self.assertFalse(self.switch())
        close.assert_not_called()
        self.assert_assignment(self.old_stream, self.old_profile)
        self.assertIsNone(self.redis.get(RedisKeys.stream_profile(self.new_stream.id)))
        self.assertEqual(self.manager.url, self.old_stream.url)
        self.assertEqual(get_profile_connection_count(self.old_profile, self.redis), 1)
        self.assertEqual(get_profile_connection_count(self.new_profile, self.redis), 1)
        self.assertEqual(get_credential_connection_count(self.old_profile, self.redis), 1)

    def test_shutdown_releases_new_assignment_once(self):
        self.assertTrue(self.switch())
        self.assertTrue(self.channel.release_stream())
        self.assertFalse(self.channel.release_stream())
        self.assertEqual(get_profile_connection_count(self.old_profile, self.redis), 0)
        self.assertEqual(get_profile_connection_count(self.new_profile, self.redis), 0)
        self.assertEqual(get_credential_connection_count(self.new_profile, self.redis), 0)

    def test_assignment_error_does_not_advance_url_or_manager_ids(self):
        with patch.object(Channel, 'update_stream_profile', side_effect=RuntimeError('Redis unavailable')), \
                patch.object(self.manager, '_close_connection') as close:
            self.assertFalse(self.switch())
        close.assert_not_called()
        self.assert_assignment(self.old_stream, self.old_profile)
        self.assertEqual(self.manager.url, self.old_stream.url)
        self.assertEqual(get_profile_connection_count(self.old_profile, self.redis), 1)
        self.assertEqual(get_profile_connection_count(self.new_profile, self.redis), 0)
