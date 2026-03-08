"""
Central configuration module.
Loads settings from config.env and exposes typed constants.
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env from project root ──────────────────────────
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / "config.env")

# ── API Keys ──────────────────────────────────────────────
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
FIREBASE_PROJECT_ID: str = os.getenv("FIREBASE_PROJECT_ID", "")
FIREBASE_CREDENTIALS_PATH: str = os.getenv(
    "FIREBASE_CREDENTIALS_PATH",
    str(BASE_DIR / "config" / "firebase_credentials.json"),
)

# ── Scraping ──────────────────────────────────────────────
REQUEST_DELAY_MIN: float = float(os.getenv("REQUEST_DELAY_MIN", "3"))
REQUEST_DELAY_MAX: float = float(os.getenv("REQUEST_DELAY_MAX", "8"))
MAX_PRODUCTS_PER_SITE: int = int(os.getenv("MAX_PRODUCTS_PER_SITE", "50"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))

# ── AI ────────────────────────────────────────────────────
AI_BATCH_SIZE: int = int(os.getenv("AI_BATCH_SIZE", "5"))
AI_LANGUAGE: str = os.getenv("AI_LANGUAGE", "hebrew")
GEMINI_MODEL: str = "gemini-2.0-flash"          # Free tier: 1500 req/day
GEMINI_RPM_LIMIT: int = 15                       # Free tier rate limit

# ── Firebase ──────────────────────────────────────────────
FIRESTORE_COLLECTION: str = "fashion_products"
FIRESTORE_META_COLLECTION: str = "scrape_meta"

# ── Active sites ──────────────────────────────────────────
_sites_env = os.getenv("ACTIVE_SITES", "renoir,renuar,castro,zara,next,fox")
ACTIVE_SITES: list[str] = [s.strip() for s in _sites_env.split(",") if s.strip()]

# ── Site configurations ───────────────────────────────────
SITE_CONFIGS: dict = {
    "renoir": {
        "name": "Renoir",
        "base_url": "https://www.renoir.co.il",
        "category_urls": [
            "https://www.renoir.co.il/c/women",
            "https://www.renoir.co.il/c/men",
            "https://www.renoir.co.il/c/kids",
        ],
        "sitemap_url": "https://www.renoir.co.il/sitemap.xml",
        "js_heavy": False,
    },
    "renuar": {
        "name": "Renuar",
        "base_url": "https://www.renuar.co.il",
        "category_urls": [
            "https://www.renuar.co.il/he/women",
            "https://www.renuar.co.il/he/men",
        ],
        "sitemap_url": "https://www.renuar.co.il/sitemap.xml",
        "js_heavy": False,
    },
    "castro": {
        "name": "Castro",
        "base_url": "https://www.castro.com",
        "api_url": "https://www.castro.com/api/catalog/category/products",
        "category_ids": ["women", "men", "kids"],
        "js_heavy": True,
        "use_api": True,
    },
    "zara": {
        "name": "Zara Israel",
        "base_url": "https://www.zara.com",
        "api_base": "https://www.zara.com/il/en",
        "categories": {
            "woman": "https://www.zara.com/il/en/woman-new-in-l1180.html",
            "man":   "https://www.zara.com/il/en/man-new-in-l837.html",
            "kids":  "https://www.zara.com/il/en/girl-new-in-l1388.html",
        },
        "js_heavy": True,
        "use_api": True,
    },
    "next": {
        "name": "Next IL",
        "base_url": "https://www.next.co.il",
        "category_urls": [
            "https://www.next.co.il/he/shop/womens",
            "https://www.next.co.il/he/shop/mens",
            "https://www.next.co.il/he/shop/kids",
        ],
        "js_heavy": False,
    },
    "fox": {
        "name": "Fox Fashion",
        "base_url": "https://www.fox.co.il",
        "api_url": "https://www.fox.co.il/api",
        "category_urls": [
            "https://www.fox.co.il/women",
            "https://www.fox.co.il/men",
            "https://www.fox.co.il/kids",
        ],
        "js_heavy": True,
        "use_api": True,
    },
    "shein": {
        "name": "Shein IL",
        "base_url": "https://il.shein.com",
        "api_url": "https://il.shein.com/api/productList/v2",
        "js_heavy": True,
        "use_api": True,
    },
    "lidor": {
        "name": "Lidor",
        "base_url": "https://www.lidor.co.il",
        "category_urls": [
            "https://www.lidor.co.il/women",
            "https://www.lidor.co.il/men",
        ],
        "js_heavy": False,
    },
}

# ── Paths ─────────────────────────────────────────────────
LOGS_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data"
LOGS_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# ── Validation ────────────────────────────────────────────
def validate() -> list[str]:
    """Returns list of missing/invalid config items."""
    errors = []
    if not GEMINI_API_KEY or GEMINI_API_KEY == "YOUR_GEMINI_API_KEY_HERE":
        errors.append("GEMINI_API_KEY not set in config.env")
    if not FIREBASE_PROJECT_ID or FIREBASE_PROJECT_ID == "YOUR_PROJECT_ID_HERE":
        errors.append("FIREBASE_PROJECT_ID not set in config.env")
    creds = Path(FIREBASE_CREDENTIALS_PATH)
    if not creds.exists():
        errors.append(f"Firebase credentials not found: {FIREBASE_CREDENTIALS_PATH}")
    else:
        try:
            with open(creds) as f:
                data = json.load(f)
            if "PLACEHOLDER" in data:
                errors.append("Firebase credentials is still the placeholder — replace it!")
        except Exception:
            errors.append("Firebase credentials JSON is invalid")
    return errors
