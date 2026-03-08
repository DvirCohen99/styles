"""
Scraper registry — maps site keys to scraper classes.
"""
from scrapers.renoir import RenoirScraper
from scrapers.renuar import RenuarScraper
from scrapers.castro import CastroScraper
from scrapers.zara import ZaraScraper
from scrapers.next_il import NextILScraper
from scrapers.fox import FoxScraper
from scrapers.shein import SheinScraper
from scrapers.lidor import LidorScraper

SCRAPERS = {
    "renoir": RenoirScraper,
    "renuar": RenuarScraper,
    "castro": CastroScraper,
    "zara":   ZaraScraper,
    "next":   NextILScraper,
    "fox":    FoxScraper,
    "shein":  SheinScraper,
    "lidor":  LidorScraper,
}

def get_scraper(site_key: str):
    cls = SCRAPERS.get(site_key)
    if not cls:
        raise ValueError(f"Unknown site: {site_key}. Available: {list(SCRAPERS)}")
    return cls()
