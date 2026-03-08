"""
pytest configuration and shared fixtures.
"""
import pytest
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"
FIXTURES_DIR.mkdir(exist_ok=True)


@pytest.fixture
def sample_shopify_product_json():
    """Minimal valid Shopify product JSON."""
    return {
        "id": 12345678,
        "title": "חולצת פשתן נשים",
        "handle": "women-linen-shirt",
        "product_type": "חולצות",
        "vendor": "TestBrand",
        "body_html": "<p>חולצה מרשים עשויה פשתן 100%</p>",
        "tags": ["new", "women", "linen"],
        "options": [
            {"name": "מידה", "values": ["XS", "S", "M", "L", "XL"]},
            {"name": "צבע", "values": ["לבן", "שחור", "ירוק"]},
        ],
        "variants": [
            {
                "id": 111,
                "sku": "SKU-001-XS-W",
                "price": "149.00",
                "compare_at_price": "199.00",
                "available": True,
                "option1": "XS",
                "option2": "לבן",
            },
            {
                "id": 222,
                "sku": "SKU-001-S-B",
                "price": "149.00",
                "compare_at_price": "199.00",
                "available": False,
                "option1": "S",
                "option2": "שחור",
            },
        ],
        "images": [
            {"src": "https://cdn.shopify.com/s/files/1/test/shirt_main.jpg"},
            {"src": "https://cdn.shopify.com/s/files/1/test/shirt_side.jpg"},
        ],
    }


@pytest.fixture
def sample_product_html():
    """Minimal HTML with JSON-LD product schema."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
      <title>חולצת פשתן נשים | TestShop</title>
      <meta property="og:title" content="חולצת פשתן נשים" />
      <meta property="og:description" content="חולצה מרשים" />
      <meta property="og:image" content="https://example.com/shirt.jpg" />
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": "חולצת פשתן נשים",
        "description": "חולצה מרשים עשויה פשתן",
        "image": "https://example.com/shirt.jpg",
        "sku": "SKU-001",
        "brand": {"@type": "Brand", "name": "TestBrand"},
        "offers": {
          "@type": "Offer",
          "price": "149.00",
          "priceCurrency": "ILS",
          "availability": "https://schema.org/InStock"
        }
      }
      </script>
    </head>
    <body>
      <nav aria-label="breadcrumb">
        <a href="/women">נשים</a> /
        <a href="/women/tops">חולצות</a>
      </nav>
      <h1>חולצת פשתן נשים</h1>
      <span class="price">₪149.00</span>
      <img src="https://example.com/shirt.jpg" />
    </body>
    </html>
    """


@pytest.fixture
def sample_next_data_html():
    """HTML with __NEXT_DATA__ containing a product."""
    import json
    product_data = {
        "id": "prod-123",
        "name": "שמלת קיץ פרחונית",
        "price": 259,
        "originalPrice": 349,
        "description": "שמלה קיצית עם הדפס פרחים",
        "images": [
            {"url": "https://cdn.adika.co.il/dress_1.jpg"},
            {"url": "https://cdn.adika.co.il/dress_2.jpg"},
        ],
        "sizes": ["XS", "S", "M", "L"],
        "colors": ["אדום", "כחול"],
        "category": "שמלות",
        "brand": "Adika",
    }
    next_data = {
        "props": {
            "pageProps": {
                "product": product_data,
            }
        },
        "page": "/product/[id]",
    }
    return f"""
    <html><head><title>שמלת קיץ</title></head>
    <body>
    <script id="__NEXT_DATA__" type="application/json">
    {json.dumps(next_data)}
    </script>
    <h1>שמלת קיץ פרחונית</h1>
    </body></html>
    """
