"""Late readiness must survive the OUTER recovery paths through run()."""
import os
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from apps.proxy.live_proxy.constants import UpstreamState
from apps.proxy.live_proxy.input.manager import StreamManager
from apps.proxy.live_proxy.input.standby import StandbyHTTPReader
from apps.proxy.live_proxy.tests.test_health_reconnect import _make_manager
from apps.proxy.live_proxy.tests import test_hot_standby as lifecycle


class OuterRecoveryTests(SimpleTestCase):
    def setUp(self):
        self.fixture = lifecycle.CoordinatorTests()
        self.fixture.setUp()
        self.controller = self.fixture.controller()
        fields = vars(self.controller.manager).copy()
        # Use real manager methods, including reconnect/switch/close, rather
        # than the lifecycle fixture's _close_socket stub.
        fields.pop('_close_socket')
        fields.pop('_clear_connection_failure_history')
        self.manager = _make_manager(**fields)
        self.manager.stopping = False
        self.manager.buffer.stopping = False
        self.manager.current_response = self.manager.current_session = None
        self.manager.transcode_process = None
        self.manager.upstream_state = UpstreamState.CONNECTED
        self.controller.manager = self.manager
        self.manager.hot_standby = self.controller
        self.old_reader = Mock(running=True)
        self.old_reader.stop.side_effect = lambda: setattr(self.old_reader, 'running', False)
        self.old_pipe = Mock()
        self.manager.http_reader, self.manager.socket = self.old_reader, self.old_pipe

        self.candidate = self.fixture.ready_candidate(self.controller)
        # Use a real StandbyHTTPReader and real pipe for the handoff. Network
        # reading is independently tested against the local HTTP server.
        self.reader = StandbyHTTPReader('http://host/b.ts')
        self.reader.pipe_read, self.reader.pipe_write = os.pipe()
        self.reader.running = True
        self.original_fd = self.reader.pipe_read
        self.candidate.reader = self.reader
        self.addCleanup(self.reader.stop)
        self.addCleanup(self.close_manager_pipe)
        self.enterContext(patch('apps.proxy.live_proxy.input.hot_standby._switch_event'))
        self.enterContext(patch.object(self.manager, '_monitor_health'))
        self.enterContext(patch.object(self.manager, '_ensure_owner_or_stop', return_value=True))
        self.enterContext(patch.object(self.manager, '_close_all_connections'))
        self.enterContext(patch('apps.proxy.live_proxy.input.manager.close_old_connections'))
        self.enterContext(patch('apps.proxy.live_proxy.input.manager.log_system_event'))
        self.enterContext(patch('apps.proxy.live_proxy.server.ProxyServer.get_instance',
                                return_value=SimpleNamespace(_live_stream_managers={})))

    def close_manager_pipe(self):
        source = getattr(self.manager, 'socket', None)
        if source is not None:
            source.close()

    def resume_and_assert_hot(self):
        self.assertIs(self.manager.http_reader, self.reader)
        self.assertEqual(self.manager.socket.fileno(), self.original_fd)
        self.assertEqual(self.manager.upstream_state, UpstreamState.HOT_PROMOTED)
        self.assertEqual(int(self.old_reader.running) + int(self.reader.running), 1)
        self.resumed_pipe = self.manager.socket
        self.manager.running = False

    def exercise_late_readiness(self, flag):
        setattr(self.manager, flag, True)
        self.assertFalse(self.reader.ready)
        StreamManager._process_stream_data(self.manager)
        self.assertFalse(self.manager.connected)
        # The spare becomes READY only after the data loop has already left.
        self.reader._accept(lifecycle.PACKET * 3)
        self.assertTrue(self.reader.ready)
        with patch.object(self.manager, '_establish_http_connection') as establish, \
                patch.object(self.manager, '_process_stream_data',
                             side_effect=self.resume_and_assert_hot) as process:
            self.manager.run()
        process.assert_called_once()
        establish.assert_not_called()
        self.old_reader.stop.assert_called_once()
        self.old_pipe.close.assert_called_once()
        self.assertIs(self.manager.http_reader, self.reader)
        self.assertIs(self.manager.socket, self.resumed_pipe)
        self.assertTrue(self.reader.running)
        self.assertEqual((self.fixture.count(1), self.fixture.count(2)), (0, 1))

    def test_outer_reconnect_preserves_late_ready_source(self):
        with patch.object(self.manager, '_attempt_reconnect',
                          wraps=self.manager._attempt_reconnect) as recover:
            self.exercise_late_readiness('needs_reconnect')
        recover.assert_called_once()

    def test_outer_next_stream_preserves_late_ready_source(self):
        with patch.object(self.manager, '_try_next_stream',
                          wraps=self.manager._try_next_stream) as recover:
            self.exercise_late_readiness('needs_stream_switch')
        recover.assert_called_once_with(reason='health_monitor')

    def test_max_retries_next_stream_resumes_promoted_source(self):
        self.manager.max_retries = 1
        self.manager.connected = False
        def fail_connection():
            self.reader._accept(lifecycle.PACKET * 3)
            return False
        with patch.object(self.manager, '_establish_http_connection',
                          side_effect=fail_connection) as establish, \
                patch.object(self.manager, '_process_stream_data',
                             side_effect=self.resume_and_assert_hot) as process:
            self.manager.run()
        establish.assert_called_once()  # Failed old input; never the promoted one.
        process.assert_called_once()
        self.assertIs(self.manager.socket, self.resumed_pipe)

    def test_inner_retry_preserves_spare_ready_after_data_loop_returns(self):
        calls = []
        def process():
            calls.append('process')
            if len(calls) == 1:
                self.manager.needs_reconnect = True
                StreamManager._process_stream_data(self.manager)
                self.assertFalse(self.manager.connected)
                self.reader._accept(lifecycle.PACKET * 3)
            else:
                self.resume_and_assert_hot()
        self.manager.connected = False
        def establish():
            self.manager.connected = True
            return True
        with patch.object(self.manager, '_establish_http_connection', side_effect=establish) as opener, \
                patch.object(self.manager, '_process_stream_data', side_effect=process):
            self.manager.run()
        opener.assert_called_once()
        self.assertEqual(len(calls), 2)
        self.assertIs(self.manager.socket, self.resumed_pipe)

    def test_cold_switch_requires_normal_connection_establishment(self):
        self.manager.needs_stream_switch = True
        new_pipe, new_reader = Mock(), Mock(running=True)
        def reserve(_):
            candidate = SimpleNamespace(
                stream_id=100, profile_id=3, reader=None,
                token=self.fixture.reserve(3),
                info=dict(url='http://host/c.ts', user_agent='agent',
                          stream_profile=1, stream_name='A'))
            self.controller.candidate = candidate
            return candidate
        def establish():
            self.assertEqual(self.manager.upstream_state, UpstreamState.CONNECTION_REQUIRED)
            self.manager.socket, self.manager.http_reader = new_pipe, new_reader
            self.manager.connected = True
            return True
        def process():
            self.assertEqual(self.manager.upstream_state, UpstreamState.CONNECTED)
            self.assertIs(self.manager.socket, new_pipe)
            self.assertIs(self.manager.http_reader, new_reader)
            self.assertFalse(self.reader.running)
            self.manager.running = False
        with patch.object(self.controller, '_candidates', return_value=[{}]), \
                patch.object(self.controller, '_reserve', side_effect=reserve), \
                patch.object(self.manager, '_establish_http_connection', side_effect=establish) as opener, \
                patch.object(self.manager, '_process_stream_data', side_effect=process):
            self.manager.run()
        opener.assert_called_once()
        self.assertEqual((self.fixture.count(1), self.fixture.count(2), self.fixture.count(3)), (0, 0, 1))

    def test_conventional_outer_reconnect_is_not_opened_twice(self):
        # No spare: _attempt_reconnect itself opens the conventional source.
        self.controller._discard()
        self.manager.needs_reconnect = True
        new_pipe = Mock()
        def establish():
            self.manager.socket = new_pipe
            self.manager.connected = True
            return True
        def process():
            self.assertEqual(self.manager.upstream_state, UpstreamState.CONNECTED)
            self.assertIs(self.manager.socket, new_pipe)
            self.manager.running = False
        with patch.object(self.manager, '_wait_for_existing_processes_to_close', return_value=True), \
                patch.object(self.manager, '_establish_http_connection', side_effect=establish) as opener, \
                patch.object(self.manager, '_process_stream_data', side_effect=process):
            self.manager.run()
        opener.assert_called_once()
