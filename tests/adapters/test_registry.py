"""
Tests for source registry.
"""
import pytest
from engine.registry.source_registry import (
    SourceRegistry,
    get_adapter,
    list_sources,
)


class TestSourceRegistry:
    ALL_EXPECTED_KEYS = [
        "renuar", "zara", "castro", "sde_bar", "lidor_bar",
        "cstyle", "hodula", "shoshi_tamam", "terminal_x", "adika",
    ]

    def test_all_10_sources_registered(self):
        keys = SourceRegistry.all_keys()
        for expected in self.ALL_EXPECTED_KEYS:
            assert expected in keys, f"Missing source: {expected}"

    def test_get_adapter_returns_correct_type(self):
        from engine.adapters.base import BaseAdapter
        for key in self.ALL_EXPECTED_KEYS:
            adapter = get_adapter(key)
            assert isinstance(adapter, BaseAdapter), f"{key} adapter is not a BaseAdapter"

    def test_get_adapter_unknown_key_raises(self):
        with pytest.raises(KeyError):
            get_adapter("nonexistent_source")

    def test_list_sources_returns_all_metas(self):
        metas = list_sources()
        assert len(metas) == 10
        keys = {m.source_key for m in metas}
        for expected in self.ALL_EXPECTED_KEYS:
            assert expected in keys

    def test_all_metas_have_required_fields(self):
        for meta in list_sources():
            assert meta.source_key, f"{meta.source_key} missing source_key"
            assert meta.source_name, f"{meta.source_key} missing source_name"
            assert meta.base_url.startswith("http"), f"{meta.source_key} invalid base_url"
            assert meta.platform_family in (
                "shopify", "magento", "woocommerce", "custom", "unknown"
            )

    def test_adapters_have_source_meta(self):
        for key in self.ALL_EXPECTED_KEYS:
            adapter = get_adapter(key)
            meta = adapter.source_meta
            assert meta.source_key == key

    def test_adapters_have_base_url(self):
        for key in self.ALL_EXPECTED_KEYS:
            adapter = get_adapter(key)
            assert adapter.BASE_URL.startswith("http")

    def test_adapters_implement_category_discovery(self):
        for key in self.ALL_EXPECTED_KEYS:
            adapter = get_adapter(key)
            cats = adapter.discover_category_urls()
            assert isinstance(cats, list), f"{key} discover_category_urls() didn't return list"
            assert len(cats) > 0, f"{key} has no category URLs defined"

    def test_shopify_adapters_platform(self):
        shopify_keys = ["renuar", "sde_bar", "lidor_bar", "hodula", "shoshi_tamam"]
        for key in shopify_keys:
            adapter = get_adapter(key)
            assert adapter.PLATFORM_FAMILY == "shopify", f"{key} should be shopify platform"

    def test_registry_is_stable_across_calls(self):
        """Multiple calls to get_adapter return fresh but equivalent instances."""
        a1 = get_adapter("renuar")
        a2 = get_adapter("renuar")
        assert type(a1) == type(a2)
        assert a1 is not a2  # Fresh instances
