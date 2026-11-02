"""Profile ordering and StreamManager integration using the real ORM interfaces."""
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from apps.channels.models import Channel
from apps.proxy.live_proxy.url_utils import get_profile_failover_candidates
from apps.proxy.live_proxy.input.manager import StreamManager
from apps.proxy.live_proxy.tests.test_health_reconnect import _make_manager
from apps.proxy.live_proxy.tests import test_hot_standby as lifecycle
from apps.proxy.live_proxy.tests.test_hot_standby import profile, PACKET


class CandidateSelectionTests(SimpleTestCase):
    def setUp(self):
        self.channel = Mock(spec=Channel)
        self.accounts = []
        self.streams = []
        for stream_id, profile_ids in ((30, (31, 32)), (10, (11, 12, 13)), (20, (21, 22))):
            account = Mock(is_active=True)
            account.profiles.filter.return_value.order_by.return_value = [
                SimpleNamespace(id=pk) for pk in profile_ids]
            self.accounts.append(account)
            self.streams.append(SimpleNamespace(id=stream_id, m3u_account=account))
        self.channel.streams.select_related.return_value.order_by.return_value = self.streams
        self.enterContext(patch('apps.proxy.live_proxy.url_utils.get_stream_object', return_value=self.channel))
        self.enterContext(patch('core.utils.RedisClient.get_client', return_value=Mock()))
        self.capacity = self.enterContext(patch('apps.proxy.live_proxy.url_utils.pool_has_capacity_for_profile', return_value=True))

    def pairs(self, stream=10, current=11):
        return [(entry['stream_id'], entry['profile_id'])
                for entry in get_profile_failover_candidates('channel', stream, current)]

    def test_current_stream_profiles_precede_other_providers(self):
        self.assertEqual(self.pairs(), [(10, 12), (10, 13), (20, 21), (20, 22), (30, 31), (30, 32)])

    def test_current_profile_is_excluded_but_other_profiles_remain(self):
        pairs = self.pairs(current=12)
        self.assertEqual(pairs[:2], [(10, 11), (10, 13)])
        self.assertNotIn((10, 12), pairs)

    def test_channelstream_order_is_used_and_wraps(self):
        self.assertEqual(self.pairs(stream=20, current=21),
                         [(20, 22), (30, 31), (30, 32), (10, 11), (10, 12), (10, 13)])
        self.channel.streams.select_related.return_value.order_by.assert_called_with('channelstream__order')

    def test_capacity_exhausted_profile_is_skipped(self):
        self.capacity.side_effect = lambda profile, _: profile.id != 12
        self.assertEqual(self.pairs()[0], (10, 13))

    def test_inactive_account_is_excluded_and_profiles_filter_active(self):
        self.accounts[1].is_active = False
        self.assertFalse(any(stream == 10 for stream, _ in self.pairs()))
        self.accounts[0].profiles.filter.assert_called_with(is_active=True)


class ManagerStandbyIntegrationTests(SimpleTestCase):
    def test_stop_closes_active_reader_and_standby_controller(self):
        buffer = SimpleNamespace(channel_id='channel', redis_client=None, stopping=False)
        manager = StreamManager('channel', 'http://host/a.ts', buffer,
                                stream_id=100, channel_name='Channel')
        active = Mock()
        controller = Mock()
        manager.http_reader = active
        manager.hot_standby = controller
        manager.stop()
        manager.stop()
        active.stop.assert_called_once()
        self.assertEqual(controller.close.call_count, 2)
        self.assertFalse(manager.running)
        self.assertTrue(buffer.stopping)

    def test_active_eof_promotes_without_establishing_connection(self):
        manager = _make_manager()
        events = []
        def fetch():
            events.append('fetch')
            if len(events) == 1:
                manager.connected = False
                return False
            manager.running = False
            return True
        def promote():
            events.append('promote')
            manager.connected = True
            return True
        manager.fetch_chunk = fetch
        manager.hot_standby = SimpleNamespace(promote=Mock(side_effect=promote))
        with patch('apps.proxy.live_proxy.input.manager.gevent.sleep'), \
                patch.object(manager, '_establish_http_connection') as establish:
            manager._process_stream_data()
        self.assertEqual(events, ['fetch', 'promote', 'fetch'])
        establish.assert_not_called()

    def test_without_standby_existing_reconnect_signal_exits_read_loop(self):
        manager = _make_manager(needs_reconnect=True)
        manager.fetch_chunk = Mock()
        manager._process_stream_data()
        self.assertFalse(manager.connected)
        manager.fetch_chunk.assert_not_called()

    def test_active_input_must_contain_valid_ts_before_starting_standby(self):
        manager = _make_manager()
        with patch('apps.proxy.live_proxy.input.manager.ConfigHelper.get', return_value=True), \
                patch.object(manager, '_start_hot_standby') as start:
            manager._observe_hot_standby_input(b'<html>not media</html>')
            start.assert_not_called()
            manager._observe_hot_standby_input(PACKET * 3)
            start.assert_called_once()

    def test_promoted_source_is_not_reopened_by_establish(self):
        manager = _make_manager()
        manager.socket = Mock()
        manager.http_reader = SimpleNamespace(active=True)
        with patch('apps.proxy.live_proxy.input.http_streamer.HTTPStreamReader') as reader:
            self.assertTrue(manager._establish_http_connection())
        reader.assert_not_called()


class LifecycleSelectionTests(SimpleTestCase):
    def setUp(self):
        fixture = lifecycle.CoordinatorTests()
        fixture.setUp()
        self.fixture = fixture
        self.controller = fixture.controller()
        self.entries = [dict(stream_id=100, profile_id=2), dict(stream_id=100, profile_id=3)]
        self.enterContext(patch('apps.proxy.live_proxy.url_utils.get_profile_failover_candidates', return_value=self.entries))

    def test_failed_pair_cools_down_then_becomes_eligible_again(self):
        self.controller._fail((100, 2))
        self.assertEqual(self.controller._candidates(), [self.entries[1]])
        self.controller.manager.failed_combinations[(100, 2)] = time.monotonic() - 1
        self.assertEqual(self.controller._candidates(), self.entries)

    def test_url_resolution_failure_releases_reservation(self):
        with patch('apps.m3u.models.M3UAccountProfile.objects') as profiles, \
                patch('apps.proxy.live_proxy.url_utils.get_stream_info_for_switch', return_value={'error': 'transform failed'}):
            profiles.select_related.return_value.get.return_value = profile(2)
            self.assertIsNone(self.controller._reserve(self.entries[0]))
        self.assertEqual(self.fixture.count(2), 0)
        self.assertIn((100, 2), self.controller.manager.failed_combinations)

    def test_failed_reader_start_releases_slot_and_tries_next_profile(self):
        info = dict(url='http://host/b.ts', transcode=False, user_agent='agent')
        with patch('apps.m3u.models.M3UAccountProfile.objects') as profiles, \
                patch('apps.proxy.live_proxy.url_utils.get_stream_info_for_switch', return_value=info), \
                patch('apps.proxy.live_proxy.input.hot_standby.StandbyHTTPReader') as reader:
            profiles.select_related.return_value.get.side_effect = [profile(2), profile(3)]
            reader.return_value.start.side_effect = [OSError('pipe creation failed'), None]
            self.controller._tick()
        self.assertEqual(self.controller.candidate.profile_id, 3)
        self.assertEqual((self.fixture.count(1), self.fixture.count(2), self.fixture.count(3)), (1, 0, 1))
        self.controller.manager._close_socket.assert_not_called()
        self.assertIn((100, 2), self.controller.manager.failed_combinations)

    def test_warms_replacement_after_promotion(self):
        candidate = self.fixture.ready_candidate(self.controller)
        with patch('apps.proxy.live_proxy.input.hot_standby._switch_event'):
            self.assertTrue(self.controller.promote())
        self.entries[:] = [dict(stream_id=100, profile_id=3)]
        info = dict(url='http://host/c.ts', transcode=False, user_agent='agent')
        with patch('apps.m3u.models.M3UAccountProfile.objects') as profiles, \
                patch('apps.proxy.live_proxy.url_utils.get_stream_info_for_switch', return_value=info) as resolve, \
                patch('apps.proxy.live_proxy.input.hot_standby.StandbyHTTPReader') as reader:
            profiles.select_related.return_value.get.return_value = profile(3)
            self.controller._tick()
        resolve.assert_called_once_with('test-channel', 100, 3)
        reader.return_value.start.assert_called_once()
        candidate.reader.stop.assert_not_called()
        self.assertEqual((self.fixture.count(1), self.fixture.count(2), self.fixture.count(3)), (0, 1, 1))
        self.assertEqual(self.controller.candidate.state, 'WARMING')
