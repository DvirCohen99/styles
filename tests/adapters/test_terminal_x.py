"""
Tests for Terminal X adapter — Magento 2 platform.
"""
import pytest
from engine.adapters.terminal_x import TerminalXAdapter
from engine.schemas.product import RawProductPayload
from engine.extraction.json_ld import extract_json_ld
from engine.validation.validator import ProductValidator


@pytest.fixture
def magento_html():
    """Terminal X / Magento 2 product page HTML."""
    return """
    <!DOCTYPE html>
    <html lang="he">
    <head>
      <title>חולצת Levi's קלאסית | Terminal X</title>
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": "חולצת Levi's קלאסית",
        "description": "חולצת כותנה קלאסית מבית Levi's עם סמל האיקוני",
        "image": ["https://media.terminalx.com/catalog/levis_shirt_1.jpg",
                  "https://media.terminalx.com/catalog/levis_shirt_2.jpg"],
        "sku": "LS-TX-001",
        "brand": {"@type": "Brand", "name": "Levi's"},
        "offers": [
          {"@type": "Offer", "price": "249", "priceCurrency": "ILS", "availability": "https://schema.org/InStock"},
          {"@type": "Offer", "price": "249", "priceCurrency": "ILS", "availability": "https://schema.org/OutOfStock"}
        ]
      }
      </script>
      <script type="text/x-magento-init">
      {
        "[data-gallery-role=gallery-placeholder]": {
          "mage/gallery/gallery": {
            "data": [
              {"full": "https://media.terminalx.com/catalog/levis_shirt_1.jpg", "img": "https://media.terminalx.com/catalog/levis_shirt_1.jpg"},
              {"full": "https://media.terminalx.com/catalog/levis_shirt_2.jpg", "img": "https://media.terminalx.com/catalog/levis_shirt_2.jpg"}
            ]
          }
        }
      }
      </script>
    </head>
    <body>
      <nav class="breadcrumbs">
        <a href="/">ראשי</a> /
        <a href="/men">גברים</a> /
        <a href="/men/shirts">חולצות</a>
      </nav>
      <h1 class="page-title"><span>חולצת Levi's קלאסית</span></h1>
      <div class="price-box">
        <span class="price-container">
          <span data-price-type="finalPrice" class="price-wrapper">
            <span class="price">₪249.00</span>
          </span>
        </span>
      </div>
      <div class="product-media">
        <img class="fotorama__img" src="https://media.terminalx.com/catalog/levis_shirt_1.jpg" />
        <img class="fotorama__img" src="https://media.terminalx.com/catalog/levis_shirt_2.jpg" />
      </div>
      <div class="swatch-opt">
        <div data-attribute-code="size">
          <div class="swatch-option text" data-option-label="S">S</div>
          <div class="swatch-option text" data-option-label="M">M</div>
          <div class="swatch-option text" data-option-label="L">L</div>
          <div class="swatch-option text out-of-stock" data-option-label="XL">XL</div>
        </div>
      </div>
    </body>
    </html>
    """


@pytest.fixture
def magento_product_data():
    return {
        "productName": "חולצת Levi's קלאסית",
        "price": 249,
        "regularPrice": 0,
        "brand": "Levi's",
    }


class TestTerminalXAdapter:
    def setup_method(self):
        self.adapter = TerminalXAdapter()
        self.validator = ProductValidator()

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "terminal_x"
        assert "terminalx.com" in meta.base_url
        assert meta.platform_family == "magento"
        assert meta.js_heavy is True

    def test_category_urls(self):
        cats = self.adapter.discover_category_urls()
        assert len(cats) >= 2
        assert all("terminalx.com" in c for c in cats)

    def test_parse_from_json_ld(self, magento_html):
        json_ld = extract_json_ld(magento_html)
        raw = RawProductPayload(
            source_site="terminal_x",
            product_url="https://www.terminalx.com/levis-shirt.html",
            html_snapshot=magento_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert "Levi" in p.product_name
        assert p.source_site == "terminal_x"
        assert p.current_price == 249.0
        assert len(p.image_urls) >= 1
        assert p.brand == "Levi's"

    def test_parse_from_script_payload(self, magento_product_data):
        raw = RawProductPayload(
            source_site="terminal_x",
            product_url="https://www.terminalx.com/levis-shirt.html",
            script_payload={"magento_product": magento_product_data},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert "Levi" in result.product.product_name

    def test_parse_dom_fallback(self, magento_html):
        raw = RawProductPayload(
            source_site="terminal_x",
            product_url="https://www.terminalx.com/levis-shirt.html",
            html_snapshot=magento_html,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert p.product_name != ""
        assert p.current_price == 249.0

    def test_breadcrumbs_extracted(self, magento_html):
        json_ld = extract_json_ld(magento_html)
        raw = RawProductPayload(
            source_site="terminal_x",
            product_url="https://www.terminalx.com/levis-shirt.html",
            html_snapshot=magento_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert isinstance(result.product.breadcrumbs, list)

    def test_magento_size_extraction_dom(self, magento_html):
        raw = RawProductPayload(
            source_site="terminal_x",
            product_url="https://www.terminalx.com/levis-shirt.html",
            html_snapshot=magento_html,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        # DOM has size swatches: S, M, L, XL
        assert len(p.sizes_available) >= 1

    def test_empty_payload_fails_gracefully(self):
        raw = RawProductPayload(
            source_site="terminal_x",
            product_url="https://www.terminalx.com/test.html",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False

    def test_validation_passes(self, magento_html):
        json_ld = extract_json_ld(magento_html)
        raw = RawProductPayload(
            source_site="terminal_x",
            product_url="https://www.terminalx.com/levis-shirt.html",
            html_snapshot=magento_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        vr = self.validator.validate(result.product)
        assert vr.valid, f"Errors: {[i.issue for i in vr.errors]}"

    def test_magento_mage_init_extraction(self, magento_html):
        """Magento mage-init data is extracted from scripts."""
        from engine.extraction.script_payload import extract_script_payload
        payloads = extract_script_payload(magento_html)
        # The mage-init parser should find gallery images
        assert isinstance(payloads, dict)
