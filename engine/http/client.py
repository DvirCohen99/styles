"""
Robust HTTP client for scraping.

Features:
- Automatic retries with exponential backoff (tenacity)
- Rotating user agents
- Rate limiting per domain
- SSL error reporting
- Timeout / connection error handling
- Response caching (optional, in-memory)
- Clear error surfaces (no swallowed failures)
"""
from __future__ import annotations

import ssl
import time
import random
import logging
import hashlib
from typing import Optional
from urllib.parse import urlparse

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

log = logging.getLogger("engine.http")

# ──────────────────────────────────────────────────────────────────────────────
# User agent pool
# ──────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
]


class RateLimiter:
    """Per-domain rate limiter with configurable min/max delay."""

    def __init__(self, min_delay: float = 2.0, max_delay: float = 6.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._last_request: dict[str, float] = {}

    def wait(self, domain: str) -> None:
        now = time.monotonic()
        last = self._last_request.get(domain, 0)
        elapsed = now - last
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request[domain] = time.monotonic()


class ScrapingError(Exception):
    """Base scraping exception — always surfaces the cause."""
    def __init__(self, message: str, url: str = "", status_code: int = 0, cause: Optional[Exception] = None):
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.cause = cause


class SSLError(ScrapingError):
    pass


class TimeoutError(ScrapingError):
    pass


class NotFoundError(ScrapingError):
    pass


class BlockedError(ScrapingError):
    """HTTP 403 / 429 — likely bot-blocked."""
    pass


class HTTPClient:
    """
    Shared HTTP client for all source adapters.

    Usage:
        client = HTTPClient(base_headers={"Referer": "https://example.com"})
        resp = client.get("https://example.com/products/shirt")
        text = resp.text
    """

    def __init__(
        self,
        min_delay: float = 2.0,
        max_delay: float = 6.0,
        timeout: float = 25.0,
        max_retries: int = 3,
        base_headers: Optional[dict] = None,
        verify_ssl: bool = True,
        use_cache: bool = False,
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self._rate_limiter = RateLimiter(min_delay, max_delay)
        self._cache: dict[str, httpx.Response] = {} if use_cache else {}
        self._use_cache = use_cache

        default_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
        }
        if base_headers:
            default_headers.update(base_headers)

        self._client = httpx.Client(
            headers=default_headers,
            timeout=httpx.Timeout(timeout, connect=10.0),
            follow_redirects=True,
            verify=verify_ssl,
            http2=True,
        )
        self._verify_ssl = verify_ssl

    def _rotate_ua(self) -> None:
        self._client.headers["User-Agent"] = random.choice(USER_AGENTS)

    @property
    def _retrying_get(self):
        """Build a tenacity-wrapped inner get method."""
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=2, min=4, max=30),
            retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
            before_sleep=before_sleep_log(log, logging.WARNING),
            reraise=True,
        )
        def _inner(url: str, **kwargs) -> httpx.Response:
            return self._client.get(url, **kwargs)
        return _inner

    def get(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        allow_redirects: bool = True,
    ) -> httpx.Response:
        """
        Perform a GET request with:
        - rate limiting per domain
        - UA rotation
        - tenacity retries on timeout/connection errors
        - clear error mapping
        """
        domain = urlparse(url).netloc

        # Cache hit
        if self._use_cache and url in self._cache:
            return self._cache[url]

        self._rate_limiter.wait(domain)
        self._rotate_ua()

        try:
            resp = self._retrying_get(url, params=params, headers=headers or {})
        except httpx.TimeoutException as e:
            raise TimeoutError(f"Timeout fetching {url}", url=url, cause=e) from e
        except httpx.ConnectError as e:
            msg = str(e)
            if "SSL" in msg or "certificate" in msg.lower():
                raise SSLError(f"SSL error on {url}: {msg}", url=url, cause=e) from e
            raise ScrapingError(f"Connection error on {url}: {msg}", url=url, cause=e) from e
        except ssl.SSLError as e:
            raise SSLError(f"SSL error on {url}: {e}", url=url, cause=e) from e
        except Exception as e:
            raise ScrapingError(f"Request failed for {url}: {e}", url=url, cause=e) from e

        if resp.status_code == 404:
            raise NotFoundError(f"404 Not Found: {url}", url=url, status_code=404)
        if resp.status_code == 403:
            raise BlockedError(f"403 Forbidden (bot-blocked?): {url}", url=url, status_code=403)
        if resp.status_code == 429:
            raise BlockedError(f"429 Rate-limited: {url}", url=url, status_code=429)
        if resp.status_code >= 500:
            raise ScrapingError(
                f"Server error {resp.status_code}: {url}",
                url=url, status_code=resp.status_code,
            )

        if self._use_cache:
            self._cache[url] = resp

        return resp

    def get_json(self, url: str, **kwargs) -> dict | list:
        """GET + JSON parse."""
        resp = self.get(url, **kwargs)
        try:
            return resp.json()
        except Exception as e:
            raise ScrapingError(
                f"JSON decode failed for {url}: {e}", url=url, cause=e
            ) from e

    def head(self, url: str) -> httpx.Response:
        """HEAD request for quick availability checks."""
        domain = urlparse(url).netloc
        self._rate_limiter.wait(domain)
        try:
            return self._client.head(url, timeout=10.0)
        except Exception as e:
            raise ScrapingError(f"HEAD failed for {url}: {e}", url=url, cause=e) from e

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
