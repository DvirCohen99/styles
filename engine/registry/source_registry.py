"""
Source adapter registry.

Maps each source key to:
- adapter class
- source metadata (platform family, priority, etc.)

Usage:
    adapter = get_adapter("renuar")
    adapter.scrape_all(limit=100)
"""
from __future__ import annotations

from typing import Type

from engine.adapters.base import BaseAdapter
from engine.schemas.source import SourceMeta


# ──────────────────────────────────────────────────────────────────────────────
# Registry entries — populated at module import
# ──────────────────────────────────────────────────────────────────────────────

class RegistryEntry:
    def __init__(
        self,
        key: str,
        adapter_class: Type[BaseAdapter],
        meta: SourceMeta,
    ):
        self.key = key
        self.adapter_class = adapter_class
        self.meta = meta

    def build(self) -> BaseAdapter:
        return self.adapter_class()


# Lazy imports to avoid circular dependencies at module load time
def _load_registry() -> dict[str, RegistryEntry]:
    from engine.adapters.renuar import RenuarAdapter
    from engine.adapters.zara import ZaraAdapter
    from engine.adapters.castro import CastroAdapter
    from engine.adapters.sde_bar import SdeBarAdapter
    from engine.adapters.lidor_bar import LidorBarAdapter
    from engine.adapters.cstyle import CStyleAdapter
    from engine.adapters.hodula import HodulaAdapter
    from engine.adapters.shoshi_tamam import ShoshiTamamAdapter
    from engine.adapters.terminal_x import TerminalXAdapter
    from engine.adapters.adika import AdikaAdapter

    entries = [
        RegistryEntry(
            key="renuar",
            adapter_class=RenuarAdapter,
            meta=SourceMeta(
                source_key="renuar",
                source_name="Renuar",
                base_url="https://www.renuar.co.il",
                platform_family="shopify",
                priority=1,
                extraction_order=1,
                has_sitemap=True,
                has_api=True,
                notes="Shopify store — use /products/<handle>.json endpoint",
            ),
        ),
        RegistryEntry(
            key="zara",
            adapter_class=ZaraAdapter,
            meta=SourceMeta(
                source_key="zara",
                source_name="Zara Israel",
                base_url="https://www.zara.com",
                platform_family="custom",
                priority=1,
                extraction_order=2,
                has_sitemap=False,
                has_api=True,
                js_heavy=True,
                notes="Zara has an internal REST API — no browser needed",
            ),
        ),
        RegistryEntry(
            key="castro",
            adapter_class=CastroAdapter,
            meta=SourceMeta(
                source_key="castro",
                source_name="Castro",
                base_url="https://www.castro.com",
                platform_family="custom",
                priority=1,
                extraction_order=3,
                has_sitemap=True,
                has_api=True,
                js_heavy=True,
                notes="Custom platform with REST API; fallback to JSON-LD + DOM",
            ),
        ),
        RegistryEntry(
            key="sde_bar",
            adapter_class=SdeBarAdapter,
            meta=SourceMeta(
                source_key="sde_bar",
                source_name="Sde Bar",
                base_url="https://www.sdebar.co.il",
                platform_family="shopify",
                priority=2,
                extraction_order=4,
                has_sitemap=True,
                has_api=True,
                notes="Shopify store",
            ),
        ),
        RegistryEntry(
            key="lidor_bar",
            adapter_class=LidorBarAdapter,
            meta=SourceMeta(
                source_key="lidor_bar",
                source_name="Lidor Bar",
                base_url="https://www.lidorbar.co.il",
                platform_family="shopify",
                priority=2,
                extraction_order=5,
                has_sitemap=True,
                has_api=True,
                notes="Shopify store",
            ),
        ),
        RegistryEntry(
            key="cstyle",
            adapter_class=CStyleAdapter,
            meta=SourceMeta(
                source_key="cstyle",
                source_name="CStyle",
                base_url="https://www.cstyle.co.il",
                platform_family="woocommerce",
                priority=2,
                extraction_order=6,
                has_sitemap=True,
                has_api=True,
                notes="WooCommerce — use REST API + JSON-LD",
            ),
        ),
        RegistryEntry(
            key="hodula",
            adapter_class=HodulaAdapter,
            meta=SourceMeta(
                source_key="hodula",
                source_name="Hodula",
                base_url="https://www.hodula.co.il",
                platform_family="shopify",
                priority=2,
                extraction_order=7,
                has_sitemap=True,
                has_api=True,
                notes="Shopify store",
            ),
        ),
        RegistryEntry(
            key="shoshi_tamam",
            adapter_class=ShoshiTamamAdapter,
            meta=SourceMeta(
                source_key="shoshi_tamam",
                source_name="Shoshi Tamam",
                base_url="https://www.shoshitamam.co.il",
                platform_family="shopify",
                priority=2,
                extraction_order=8,
                has_sitemap=True,
                has_api=True,
                notes="Shopify store",
            ),
        ),
        RegistryEntry(
            key="terminal_x",
            adapter_class=TerminalXAdapter,
            meta=SourceMeta(
                source_key="terminal_x",
                source_name="Terminal X",
                base_url="https://www.terminalx.com",
                platform_family="magento",
                priority=1,
                extraction_order=9,
                has_sitemap=True,
                has_api=False,
                js_heavy=True,
                notes="Magento 2 — use JSON-LD + inline script data; no public API",
            ),
        ),
        RegistryEntry(
            key="adika",
            adapter_class=AdikaAdapter,
            meta=SourceMeta(
                source_key="adika",
                source_name="Adika",
                base_url="https://www.adika.co.il",
                platform_family="custom",
                priority=1,
                extraction_order=10,
                has_sitemap=True,
                has_api=False,
                js_heavy=True,
                notes="Custom Next.js platform — use __NEXT_DATA__ + JSON-LD",
            ),
        ),
    ]

    return {e.key: e for e in entries}


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

# Populated on first access
_registry: dict[str, RegistryEntry] | None = None


class SourceRegistry:
    """Singleton registry with lazy loading."""

    @classmethod
    def _get(cls) -> dict[str, RegistryEntry]:
        global _registry
        if _registry is None:
            _registry = _load_registry()
        return _registry

    @classmethod
    def get_entry(cls, key: str) -> RegistryEntry:
        reg = cls._get()
        if key not in reg:
            available = list(reg.keys())
            raise KeyError(f"Unknown source: '{key}'. Available: {available}")
        return reg[key]

    @classmethod
    def all_keys(cls) -> list[str]:
        return sorted(cls._get().keys())

    @classmethod
    def all_metas(cls) -> list[SourceMeta]:
        return [e.meta for e in sorted(cls._get().values(), key=lambda e: e.meta.extraction_order)]

    @classmethod
    def active_entries(cls) -> list[RegistryEntry]:
        return [e for e in cls._get().values() if e.meta.active]


# Convenience functions
SOURCE_REGISTRY = SourceRegistry

def get_adapter(source_key: str) -> BaseAdapter:
    """Build and return an adapter instance for the given source key."""
    return SourceRegistry.get_entry(source_key).build()


def list_sources() -> list[SourceMeta]:
    """Return metadata for all registered sources, sorted by extraction order."""
    return SourceRegistry.all_metas()
