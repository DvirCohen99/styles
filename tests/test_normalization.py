"""
Tests for normalization layer.
"""
import pytest
from engine.normalization.price import normalize_price, normalize_price_pair
from engine.normalization.text import normalize_text, normalize_name, detect_gender, detect_material
from engine.normalization.variants import normalize_sizes, normalize_colors, normalize_variants
from engine.normalization.images import normalize_image_urls


class TestPriceNormalization:
    def test_string_price(self):
        assert normalize_price("149.00") == 149.0

    def test_price_with_shekel(self):
        assert normalize_price("₪149") == 149.0

    def test_price_with_comma_decimal(self):
        assert normalize_price("129,90") == 129.90

    def test_price_with_thousands(self):
        assert normalize_price("1,299") == 1299.0

    def test_shopify_cents_int(self):
        # 14900 cents = 149 ILS
        price = normalize_price(14900)
        assert price == 149.0

    def test_zero_returns_none(self):
        assert normalize_price(0) is None

    def test_none_returns_none(self):
        assert normalize_price(None) is None

    def test_invalid_string_returns_none(self):
        assert normalize_price("no price here") is None

    def test_pair_sanity_check(self):
        current, original = normalize_price_pair(149.0, 100.0)
        assert current == 149.0
        assert original is None  # original < current → discarded

    def test_pair_valid_sale(self):
        current, original = normalize_price_pair("149", "199")
        assert current == 149.0
        assert original == 199.0


class TestTextNormalization:
    def test_clean_whitespace(self):
        assert normalize_text("  hello   world  ") == "hello world"

    def test_html_entity_decode(self):
        assert normalize_text("&#169; test &amp; thing") == "© test & thing"

    def test_max_length(self):
        long = "x" * 1000
        assert len(normalize_text(long, max_len=100)) <= 100

    def test_normalize_name(self):
        name = normalize_name("  חולצת  פשתן  נשים  ")
        assert name == "חולצת פשתן נשים"

    def test_detect_gender_hebrew(self):
        assert detect_gender("חולצה לנשים") == "women"
        assert detect_gender("מכנסי גברים") == "men"
        assert detect_gender("בגד לילדים") == "kids"

    def test_detect_gender_english(self):
        assert detect_gender("Women's T-Shirt") == "women"
        assert detect_gender("Men's Hoodie") == "men"

    def test_detect_material_cotton(self):
        fabric, comp = detect_material("100% Cotton — כותנה טבעית")
        assert fabric == "cotton"

    def test_detect_material_composition(self):
        _, comp = detect_material("80% Cotton, 20% Polyester")
        assert "80%" in comp
        assert "20%" in comp


class TestVariantNormalization:
    def test_normalize_sizes_standard(self):
        sizes = normalize_sizes(["xs", "s", "m", "l", "xl"])
        assert "XS" in sizes
        assert "S" in sizes
        assert "XL" in sizes

    def test_normalize_sizes_filters_empty(self):
        sizes = normalize_sizes(["", "בחר מידה", "S", "--"])
        assert "" not in sizes
        assert "בחר מידה" not in sizes
        assert "S" in sizes

    def test_normalize_sizes_dedup(self):
        sizes = normalize_sizes(["S", "s", "S"])
        assert sizes.count("S") == 1

    def test_normalize_colors_capitalizes(self):
        colors = normalize_colors(["white", "BLACK", "Red"])
        assert "White" in colors
        assert "BLACK" in colors

    def test_normalize_colors_filters_empty(self):
        colors = normalize_colors(["", "בחר צבע", "Red"])
        assert "" not in colors
        assert "Red" in colors

    def test_normalize_variants_shopify(self, sample_shopify_product_json):
        variants_raw = sample_shopify_product_json["variants"]
        variants = normalize_variants(variants_raw)
        assert len(variants) == 2
        assert variants[0].size == "XS"
        assert variants[0].color == "לבן"
        assert variants[0].price == 149.0
        assert variants[0].original_price == 199.0
        assert variants[0].in_stock is True
        assert variants[1].in_stock is False

    def test_normalize_variants_no_crash_on_empty(self):
        variants = normalize_variants([])
        assert variants == []

    def test_normalize_variants_no_crash_on_bad_data(self):
        variants = normalize_variants([{"price": "not-a-price"}, None, 42])
        # Should not raise, returns what it can
        assert isinstance(variants, list)


class TestImageNormalization:
    def test_http_upgrade(self):
        urls = normalize_image_urls(["http://example.com/img.jpg"])
        assert urls[0].startswith("https://")

    def test_strips_query_string(self):
        urls = normalize_image_urls(["https://example.com/img.jpg?v=123"])
        assert "?" not in urls[0]

    def test_deduplication(self):
        urls = normalize_image_urls([
            "https://example.com/img.jpg",
            "https://example.com/img.jpg",
            "https://example.com/img_small.jpg",
        ])
        # First two are the same after normalization
        assert len(urls) <= 2

    def test_filters_non_images(self):
        urls = normalize_image_urls([
            "https://example.com/script.js",
            "https://example.com/img.jpg",
        ])
        assert any("img.jpg" in u for u in urls)
        assert not any("script.js" in u for u in urls)

    def test_max_count(self):
        urls = [f"https://example.com/img{i}.jpg" for i in range(20)]
        result = normalize_image_urls(urls, max_count=5)
        assert len(result) <= 5

    def test_relative_url_with_base(self):
        urls = normalize_image_urls(["/images/shirt.jpg"], base_url="https://example.com")
        assert urls[0] == "https://example.com/images/shirt.jpg"

    def test_protocol_relative(self):
        urls = normalize_image_urls(["//cdn.example.com/img.jpg"])
        assert urls[0].startswith("https://")
