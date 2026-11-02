"""Cooperative HTTP MPEG-TS reader with a discard-until-promoted pipe.

Only StreamManager consumes the pipe and writes the existing StreamBuffer.
There is no standby media queue: incomplete packet tails are bounded to 187
bytes once synchronized. The provider response is preserved during promotion.
"""
import os
import time

import gevent
from gevent import select
import requests
from requests.adapters import HTTPAdapter

from ..constants import TS_PACKET_SIZE
from ..utils import get_logger

logger = get_logger()


def supported_http_ts(url, transcode=False):
    from urllib.parse import urlsplit
    from ..utils import detect_stream_type
    from ..constants import StreamType
    return (not transcode and urlsplit(url).scheme in ('http', 'https')
            and detect_stream_type(url) not in (StreamType.HLS, StreamType.RTSP, StreamType.UDP))


class StandbyHTTPReader:
    def __init__(self, url, user_agent=None, chunk_size=8192, *,
                 warmup_timeout=10, idle_timeout=10):
        self.url = url
        self.user_agent = user_agent
        self.chunk_size = chunk_size
        self.warmup_timeout = warmup_timeout
        self.idle_timeout = idle_timeout
        self.session = self.response = self.greenlet = None
        self.pipe_read = self.pipe_write = None
        self.running = False
        self.active = False
        self.failed = False
        self.last_media_time = None
        self.started_at = None
        self._pending = bytearray()
        self._synchronized = False

    @property
    def ready(self):
        return (self.running and not self.failed and self.last_media_time is not None
                and time.monotonic() - self.last_media_time < self.idle_timeout)

    def start(self):
        self.pipe_read, self.pipe_write = os.pipe()
        os.set_blocking(self.pipe_write, False)
        self.running = True
        self.started_at = time.monotonic()
        self.greenlet = gevent.spawn(self._run)

    @staticmethod
    def _valid_header(data, offset):
        return (data[offset] == 0x47 and not data[offset + 1] & 0x80
                and data[offset + 3] & 0x30 != 0)

    def _packets(self, data):
        self._pending.extend(data)
        size = TS_PACKET_SIZE
        if not self._synchronized:
            # Require three complete packets, not an isolated 0x47 in HTML.
            for offset in range(max(0, len(self._pending) - 3 * size + 1)):
                if all(self._valid_header(self._pending, offset + n * size) for n in range(3)):
                    del self._pending[:offset]
                    self._synchronized = True
                    break
            if not self._synchronized:
                # Retain only enough overlap for the next synchronization scan.
                self._pending = self._pending[-(3 * size - 1):]
                return b''
        end = len(self._pending) // size * size
        if any(not self._valid_header(self._pending, offset) for offset in range(0, end, size)):
            raise ValueError('Invalid MPEG-TS packet framing')
        packets = bytes(self._pending[:end])
        del self._pending[:end]
        return packets

    def _accept(self, chunk):
        packets = self._packets(chunk)
        if not packets:
            return
        self.last_media_time = time.monotonic()
        if not self.active:
            return  # Discard complete packets; never feed the active buffer.
        offset = 0
        while self.running and offset < len(packets):
            try:
                offset += os.write(self.pipe_write, packets[offset:])
            except BlockingIOError:
                select.select([], [self.pipe_write], [], 0.1)

    def _run(self):
        try:
            self.session = requests.Session()
            adapter = HTTPAdapter(max_retries=0, pool_connections=1, pool_maxsize=1)
            self.session.mount('http://', adapter)
            self.session.mount('https://', adapter)
            # A wall-clock warm-up watchdog lives in the coordinator as well.
            self.response = self.session.get(
                self.url, headers={'User-Agent': self.user_agent} if self.user_agent else {},
                stream=True, timeout=(self.warmup_timeout, self.idle_timeout))
            self.response.raise_for_status()
            if self.response.status_code != 200:
                raise ValueError('Unexpected upstream status')
            for chunk in self.response.iter_content(chunk_size=self.chunk_size):
                if not self.running:
                    break
                if chunk:
                    self._accept(chunk)
                gevent.sleep(0)
        except Exception as exc:
            # Exception text from requests may contain the full credential URL.
            logger.warning('Hot standby reader failed (%s)', type(exc).__name__)
        finally:
            self.failed = True
            self.running = False
            self._close_upstream()
            self._close_fd('pipe_write')

    def promote(self):
        """Transfer the read fd to StreamManager without another HTTP request."""
        if not self.ready:
            raise RuntimeError('Standby is not ready')
        result = os.fdopen(self.pipe_read, 'rb', buffering=0)
        self.pipe_read = None
        self.active = True
        return result

    def _close_fd(self, name):
        fd = getattr(self, name)
        setattr(self, name, None)
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

    def _close_upstream(self):
        for name in ('response', 'session'):
            resource = getattr(self, name)
            setattr(self, name, None)
            if resource is not None:
                try:
                    resource.close()
                except Exception:
                    pass

    def stop(self):
        self.running = False
        if self.greenlet and self.greenlet is not gevent.getcurrent():
            self.greenlet.kill(block=True, timeout=1)
        self._close_upstream()
        self._close_fd('pipe_write')
        self._close_fd('pipe_read')
