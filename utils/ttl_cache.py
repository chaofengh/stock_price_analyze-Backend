import threading
import time
from collections import OrderedDict

_MISSING = object()


class TTLCache:
    def __init__(self, ttl_seconds: int, max_size: int = 256):
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._lock = threading.Lock()
        self._data = OrderedDict()

    def get(self, key, default=_MISSING):
        now = time.time()
        with self._lock:
            entry = self._data.get(key, _MISSING)
            if entry is _MISSING:
                return default
            value, expires_at = entry
            if expires_at <= now:
                self._data.pop(key, None)
                return default
            self._data.move_to_end(key)
            return value

    def set(self, key, value):
        expires_at = time.time() + self.ttl_seconds
        with self._lock:
            self._data[key] = (value, expires_at)
            self._data.move_to_end(key)
            if len(self._data) > self.max_size:
                self._data.popitem(last=False)

    def get_or_set(self, key, factory):
        value = self.get(key, _MISSING)
        if value is not _MISSING:
            return value
        value = factory()
        self.set(key, value)
        return value
