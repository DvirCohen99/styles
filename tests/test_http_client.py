"""
Tests for HTTP client — error handling, rate limiting, UA rotation.
Does not make live HTTP calls; uses mock responses.
"""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from engine.http.client import (
    HTTPClient,
    ScrapingError,
    NotFoundError,
    BlockedError,
    TimeoutError,
    SSLError,
    RateLimiter,
)


class TestRateLimiter:
    def test_first_request_no_wait(self):
        """First request to a domain should not incur wait."""
        import time
        limiter = RateLimiter(min_delay=0.05, max_delay=0.1)
        start = time.monotonic()
        limiter.wait("example.com")
        elapsed = time.monotonic() - start
        # First request shouldn't wait (no prior request)
        assert elapsed < 0.5

    def test_second_request_waits(self):
        """Second request within min_delay triggers a wait."""
        import time
        limiter = RateLimiter(min_delay=0.2, max_delay=0.3)
        limiter.wait("example.com")   # First
        start = time.monotonic()
        limiter.wait("example.com")   # Should wait ~0.2-0.3s
        elapsed = time.monotonic() - start
        assert elapsed >= 0.1          # At least some wait

    def test_different_domains_independent(self):
        """Rate limiting is per-domain."""
        import time
        limiter = RateLimiter(min_delay=0.5, max_delay=1.0)
        limiter.wait("site-a.com")   # First request to a
        start = time.monotonic()
        limiter.wait("site-b.com")   # First request to b — should not wait for a
        elapsed = time.monotonic() - start
        assert elapsed < 1.0


class TestHTTPClientErrors:
    def test_404_raises_not_found(self):
        """_retrying_get is a property; patch _client.get instead."""
        client = HTTPClient(min_delay=0.0, max_delay=0.0)
        import httpx
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 404
        with patch.object(client._client, 'get', return_value=mock_resp):
            with pytest.raises(NotFoundError):
                client.get("https://example.com/missing")
        client.close()

    def test_client_initializes(self):
        client = HTTPClient(min_delay=0.1, max_delay=0.2)
        assert client is not None
        client.close()

    def test_user_agent_rotation(self):
        from engine.http.client import USER_AGENTS
        assert len(USER_AGENTS) >= 5
        # All should be non-empty strings
        for ua in USER_AGENTS:
            assert isinstance(ua, str)
            assert len(ua) > 20

    def test_error_mapping_404(self):
        """404 responses raise NotFoundError."""
        client = HTTPClient(min_delay=0.0, max_delay=0.0)
        import httpx
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 404

        with patch.object(client._client, 'get', return_value=mock_resp):
            with pytest.raises(NotFoundError):
                client.get("https://example.com/missing")
        client.close()

    def test_error_mapping_403(self):
        """403 responses raise BlockedError."""
        client = HTTPClient(min_delay=0.0, max_delay=0.0)
        import httpx
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 403

        with patch.object(client._client, 'get', return_value=mock_resp):
            with pytest.raises(BlockedError):
                client.get("https://example.com/blocked")
        client.close()

    def test_error_mapping_429(self):
        """429 responses raise BlockedError."""
        client = HTTPClient(min_delay=0.0, max_delay=0.0)
        import httpx
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 429

        with patch.object(client._client, 'get', return_value=mock_resp):
            with pytest.raises(BlockedError):
                client.get("https://example.com/rate-limited")
        client.close()

    def test_error_mapping_500(self):
        """500 responses raise ScrapingError."""
        client = HTTPClient(min_delay=0.0, max_delay=0.0)
        import httpx
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 500

        with patch.object(client._client, 'get', return_value=mock_resp):
            with pytest.raises(ScrapingError):
                client.get("https://example.com/server-error")
        client.close()

    def test_200_returns_response(self):
        """200 responses are returned normally."""
        client = HTTPClient(min_delay=0.0, max_delay=0.0)
        import httpx
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.text = "<html>OK</html>"

        with patch.object(client._client, 'get', return_value=mock_resp):
            resp = client.get("https://example.com/ok")
            assert resp.status_code == 200
        client.close()

    def test_context_manager(self):
        with HTTPClient(min_delay=0.0, max_delay=0.0) as client:
            assert client is not None
        # No exception on exit

    def test_scraping_error_has_url(self):
        err = ScrapingError("test error", url="https://example.com/x", status_code=503)
        assert err.url == "https://example.com/x"
        assert err.status_code == 503
