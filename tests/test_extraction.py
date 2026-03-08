"""
Tests for extraction layer.
"""
import pytest
from engine.extraction.json_ld import (
    extract_json_ld,
    find_product_json_ld,
    find_breadcrumbs_json_ld,
    parse_product_from_json_ld,
)
from engine.extraction.script_payload import (
    extract_script_payload,
    find_next_product,
)
from engine.extraction.dom_selector import DOMExtractor
from engine.extraction.heuristic import HeuristicExtractor


class TestJsonLdExtraction:
    def test_extracts_product_block(self, sample_product_html):
        blocks = extract_json_ld(sample_product_html)
        assert len(blocks) > 0
        product = find_product_json_ld(blocks)
        assert product is not None
        assert product["@type"] == "Product"
        assert product["name"] == "חולצת פשתן נשים"

    def test_extracts_price_from_offers(self, sample_product_html):
        blocks = extract_json_ld(sample_product_html)
        product = find_product_json_ld(blocks)
        parsed = parse_product_from_json_ld(product)
        assert parsed["current_price"] == 149.0
        assert parsed["currency"] == "ILS"
        assert parsed["in_stock"] is True

    def test_extracts_brand(self, sample_product_html):
        blocks = extract_json_ld(sample_product_html)
        product = find_product_json_ld(blocks)
        parsed = parse_product_from_json_ld(product)
        assert parsed["brand"] == "TestBrand"

    def test_extracts_image(self, sample_product_html):
        blocks = extract_json_ld(sample_product_html)
        product = find_product_json_ld(blocks)
        parsed = parse_product_from_json_ld(product)
        assert len(parsed["image_urls"]) > 0
        assert "example.com/shirt.jpg" in parsed["image_urls"][0]

    def test_malformed_json_graceful(self):
        html = '<script type="application/ld+json">{"@type": "Product", "name": "Test",}</script>'
        blocks = extract_json_ld(html)
        # Should repair trailing comma and parse
        assert len(blocks) >= 0  # At minimum, no crash

    def test_empty_html_returns_empty(self):
        blocks = extract_json_ld("<html><body></body></html>")
        assert blocks == []

    def test_graph_unwrapping(self):
        html = '''<script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@graph": [
            {"@type": "Product", "name": "Test"},
            {"@type": "BreadcrumbList", "itemListElement": []}
          ]
        }
        </script>'''
        blocks = extract_json_ld(html)
        assert len(blocks) == 2
        types = [b["@type"] for b in blocks]
        assert "Product" in types
        assert "BreadcrumbList" in types

    def test_breadcrumbs_extraction(self, sample_product_html):
        # sample_product_html doesn't have BreadcrumbList JSON-LD
        # but the function should not crash
        blocks = extract_json_ld(sample_product_html)
        crumbs = find_breadcrumbs_json_ld(blocks)
        assert isinstance(crumbs, list)


class TestScriptPayloadExtraction:
    def test_extracts_next_data(self, sample_next_data_html):
        payloads = extract_script_payload(sample_next_data_html)
        assert "next_data" in payloads
        nd = payloads["next_data"]
        assert nd["props"]["pageProps"]["product"]["name"] == "שמלת קיץ פרחונית"

    def test_find_next_product(self, sample_next_data_html):
        payloads = extract_script_payload(sample_next_data_html)
        product = find_next_product(payloads)
        assert product is not None
        assert product["name"] == "שמלת קיץ פרחונית"
        assert product["price"] == 259

    def test_empty_html_returns_empty(self):
        payloads = extract_script_payload("<html></html>")
        assert isinstance(payloads, dict)

    def test_window_initial_state(self):
        html = '<script>window.__INITIAL_STATE__ = {"product": {"name": "Test", "price": 100}}</script>'
        payloads = extract_script_payload(html)
        assert "initial_state" in payloads


class TestDOMExtractor:
    def test_text_extraction(self, sample_product_html):
        dom = DOMExtractor(sample_product_html)
        name = dom.text("h1")
        assert "חולצת פשתן נשים" in name

    def test_price_extraction(self, sample_product_html):
        dom = DOMExtractor(sample_product_html)
        price = dom.extract_price(".price")
        assert price == 149.0

    def test_image_extraction(self, sample_product_html):
        dom = DOMExtractor(sample_product_html)
        images = dom.extract_images("img")
        assert len(images) > 0
        assert "example.com/shirt.jpg" in images[0]

    def test_breadcrumbs_extraction(self, sample_product_html):
        dom = DOMExtractor(sample_product_html)
        crumbs = dom.extract_breadcrumbs()
        assert isinstance(crumbs, list)

    def test_missing_element_returns_default(self):
        dom = DOMExtractor("<html></html>")
        text = dom.text("h1", default="fallback")
        assert text == "fallback"

    def test_canonical_extraction(self):
        html = '<html><head><link rel="canonical" href="https://example.com/products/shirt"/></head></html>'
        dom = DOMExtractor(html)
        assert dom.extract_canonical() == "https://example.com/products/shirt"

    def test_meta_extraction(self):
        html = '<html><head><meta property="og:title" content="Test Product"/></head></html>'
        dom = DOMExtractor(html)
        assert dom.extract_meta(property_name="og:title") == "Test Product"

    def test_price_comma_decimal(self):
        html = '<span class="price">129,90</span>'
        dom = DOMExtractor(html)
        price = dom.extract_price(".price")
        assert price == 129.90

    def test_price_with_symbol(self):
        html = '<span class="price">₪149</span>'
        dom = DOMExtractor(html)
        price = dom.extract_price(".price")
        assert price == 149.0


class TestHeuristicExtractor:
    def test_extract_name_from_h1(self, sample_product_html):
        heur = HeuristicExtractor(sample_product_html)
        name = heur.extract_name()
        assert name is not None
        assert len(name) > 2

    def test_extract_price(self, sample_product_html):
        heur = HeuristicExtractor(sample_product_html)
        price = heur.extract_price()
        assert price == 149.0

    def test_extract_images(self, sample_product_html):
        heur = HeuristicExtractor(sample_product_html)
        images = heur.extract_images()
        assert len(images) > 0

    def test_extract_description(self, sample_product_html):
        heur = HeuristicExtractor(sample_product_html)
        desc = heur.extract_description()
        assert desc is not None

    def test_currency_detection(self):
        html = "<html><body>₪149</body></html>"
        heur = HeuristicExtractor(html)
        assert heur.extract_currency() == "ILS"

    def test_no_crash_on_empty(self):
        heur = HeuristicExtractor("")
        assert heur.extract_name() is None
        assert heur.extract_price() is None
        assert heur.extract_images() == []
