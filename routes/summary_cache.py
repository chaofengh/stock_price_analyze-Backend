import threading
import time


class CacheBucket:
    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds
        self._data = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        now = time.time()
        with self._lock:
            cached = self._data.get(key)
            if not cached:
                return None
            payload, expires_at = cached
            if expires_at <= now:
                self._data.pop(key, None)
                return None
            return payload

    def set(self, key: str, payload):
        with self._lock:
            self._data[key] = (payload, time.time() + self.ttl_seconds)


class AsyncCache(CacheBucket):
    def __init__(self, ttl_seconds: int):
        super().__init__(ttl_seconds)
        self._inflight = set()
        self._inflight_lock = threading.Lock()

    def start_inflight(self, key: str) -> bool:
        with self._inflight_lock:
            if key in self._inflight:
                return False
            self._inflight.add(key)
            return True

    def finish_inflight(self, key: str):
        with self._inflight_lock:
            self._inflight.discard(key)


SUMMARY_CACHE = AsyncCache(ttl_seconds=60)
OVERVIEW_CACHE = CacheBucket(ttl_seconds=600)
PEERS_CACHE = AsyncCache(ttl_seconds=300)
FUNDAMENTALS_CACHE = AsyncCache(ttl_seconds=600)
PEER_AVG_CACHE = AsyncCache(ttl_seconds=600)
