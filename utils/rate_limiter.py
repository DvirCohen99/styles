"""
Token-bucket rate limiter + polite request helper.
Prevents IP bans and respects server resources.
"""
import time
import random
import threading
from collections import defaultdict
from utils.logger import get_logger

log = get_logger("rate_limiter")


class RateLimiter:
    """Per-domain rate limiter using minimum delay between requests."""

    def __init__(self, min_delay: float = 3.0, max_delay: float = 8.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._last_request: dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()

    def wait(self, domain: str = "default") -> None:
        """Block until it's polite to make the next request to this domain."""
        with self._lock:
            elapsed = time.time() - self._last_request[domain]
            delay = random.uniform(self.min_delay, self.max_delay)
            remaining = delay - elapsed
            if remaining > 0:
                log.debug(f"Rate limit: waiting {remaining:.1f}s for {domain}")
                time.sleep(remaining)
            self._last_request[domain] = time.time()


class GeminiRateLimiter:
    """Sliding-window limiter for Gemini free tier (15 RPM)."""

    def __init__(self, rpm: int = 14):  # conservative: 14 of 15 allowed
        self.rpm = rpm
        self._timestamps: list[float] = []
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            window_start = now - 60.0
            self._timestamps = [t for t in self._timestamps if t > window_start]
            if len(self._timestamps) >= self.rpm:
                oldest = self._timestamps[0]
                sleep_for = 60.0 - (now - oldest) + 0.5
                if sleep_for > 0:
                    log.debug(f"Gemini RPM limit: sleeping {sleep_for:.1f}s")
                    time.sleep(sleep_for)
                self._timestamps = [t for t in self._timestamps if t > time.time() - 60]
            self._timestamps.append(time.time())
