"""
Tests for CLI — argument parsing and dispatch.
"""
import pytest
import sys
from unittest.mock import patch, MagicMock

from engine.cli.commands import build_parser, cli


class TestBuildParser:
    def setup_method(self):
        self.parser = build_parser()

    def test_list_sources_command(self):
        args = self.parser.parse_args(["list-sources"])
        assert args.command == "list-sources"

    def test_healthcheck_command(self):
        args = self.parser.parse_args(["healthcheck", "renuar"])
        assert args.command == "healthcheck"
        assert args.source == "renuar"

    def test_discover_products_command(self):
        args = self.parser.parse_args(["discover-products", "castro", "--limit", "50"])
        assert args.command == "discover-products"
        assert args.source == "castro"
        assert args.limit == 50

    def test_scrape_product_command(self):
        args = self.parser.parse_args([
            "scrape-product", "renuar",
            "https://www.renuar.co.il/products/test"
        ])
        assert args.command == "scrape-product"
        assert args.source == "renuar"
        assert "renuar.co.il" in args.url

    def test_scrape_source_command(self):
        args = self.parser.parse_args(["scrape-source", "renuar", "--limit", "100"])
        assert args.command == "scrape-source"
        assert args.limit == 100

    def test_export_json_command(self):
        args = self.parser.parse_args(["export-json", "zara"])
        assert args.command == "export-json"
        assert args.source == "zara"

    def test_no_command_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            cli([])
        assert exc_info.value.code == 0


class TestCLIDispatch:
    def test_list_sources_runs(self):
        """list-sources should not crash."""
        with patch("builtins.print"):
            cli(["list-sources"])  # Should not raise

    def test_unknown_source_exits(self):
        with pytest.raises(SystemExit):
            cli(["healthcheck", "nonexistent_source_xyz"])

    def test_healthcheck_calls_adapter(self):
        mock_result = MagicMock()
        mock_result.status = "ok"
        mock_result.reachable = True
        mock_result.discovery_ok = True
        mock_result.parse_ok = True
        mock_result.response_time_ms = 123.0
        mock_result.sample_product_url = None
        mock_result.sample_product_name = None
        mock_result.error = None
        mock_result.warnings = []

        mock_adapter = MagicMock()
        mock_adapter.healthcheck.return_value = mock_result

        with patch("engine.cli.commands._get_adapter", return_value=mock_adapter):
            with patch("builtins.print"):
                # CLI calls sys.exit(0) on success — catch it
                with pytest.raises(SystemExit) as exc_info:
                    cli(["healthcheck", "renuar"])
                assert exc_info.value.code == 0
                mock_adapter.healthcheck.assert_called_once()

    def test_discover_products_calls_adapter(self):
        mock_adapter = MagicMock()
        mock_adapter.discover_product_urls.return_value = [
            "https://example.com/p/1",
            "https://example.com/p/2",
        ]

        with patch("engine.cli.commands._get_adapter", return_value=mock_adapter):
            with patch("builtins.print"):
                cli(["discover-products", "renuar", "--limit", "2"])
                mock_adapter.discover_product_urls.assert_called_once_with(limit=2)

    def test_scrape_product_calls_adapter(self):
        from engine.schemas.product import NormalizedProduct
        from engine.schemas.result import ParseResult
        from engine.schemas.product import RawProductPayload

        mock_product = MagicMock(spec=NormalizedProduct)
        mock_product.product_name = "Test"
        mock_product.current_price = 149.0
        mock_product.currency = "ILS"
        mock_product.source_site = "renuar"
        mock_product.is_on_sale = False
        mock_product.stock_status = "in_stock"
        mock_product.extraction_confidence = 0.95
        mock_product.image_count = 2
        mock_product.colors_available = ["Red"]
        mock_product.sizes_available = ["M"]
        mock_product.to_json_dict.return_value = {"product_name": "Test"}

        mock_result = MagicMock(spec=ParseResult)
        mock_result.success = True
        mock_result.product = mock_product
        mock_result.warnings = []
        mock_result.extraction_method = "api"

        mock_payload = MagicMock(spec=RawProductPayload)

        mock_adapter = MagicMock()
        mock_adapter.fetch_product_page.return_value = mock_payload
        mock_adapter.parse_product.return_value = mock_result

        with patch("engine.cli.commands._get_adapter", return_value=mock_adapter):
            with patch("builtins.print"):
                cli([
                    "scrape-product", "renuar",
                    "https://www.renuar.co.il/products/test"
                ])
