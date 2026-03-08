"""
Firebase output writer.
Writes normalized products to Firestore using the existing db layer.
Supports both single-product and batch upsert modes.
"""
from __future__ import annotations

import logging
from typing import Optional

from engine.schemas.product import NormalizedProduct
from engine.schemas.source import SourceStats

log = logging.getLogger("engine.output.firebase")


class FirebaseOutputWriter:
    """
    Write products to Firestore using the existing FirestoreManager.
    Bridges the new engine schema to the existing db layer.
    """

    def __init__(self, collection: str = "fashion_products"):
        self.collection = collection
        self._db = None

    def _get_db(self):
        """Lazy Firestore connection."""
        if self._db is None:
            try:
                from db.firestore import FirestoreManager
                self._db = FirestoreManager()
            except Exception as e:
                log.error(f"Firestore init failed: {e}")
                raise
        return self._db

    def upsert_products(
        self,
        products: list[NormalizedProduct],
        source_key: str = "",
    ) -> tuple[int, int]:
        """
        Upsert normalized products to Firestore.
        Returns (created_count, updated_count).
        """
        db = self._get_db()

        # Convert to Firebase-ready dicts
        firebase_records = []
        for p in products:
            record = p.to_firebase_dict()
            firebase_records.append(record)

        # Use existing upsert mechanism if available
        created = 0
        updated = 0
        try:
            if hasattr(db, "upsert_normalized_products"):
                created, updated = db.upsert_normalized_products(firebase_records)
            else:
                # Fallback: use existing upsert_products with adapter
                adapted = self._adapt_to_enriched(products)
                created, updated = db.upsert_products(adapted)
        except Exception as e:
            log.error(f"Firestore upsert failed: {e}")
            raise

        log.info(f"Firebase: {created} created, {updated} updated for {source_key}")
        return created, updated

    def _adapt_to_enriched(self, products: list[NormalizedProduct]):
        """
        Adapt NormalizedProduct to the existing EnrichedProduct format
        for backward compatibility with the old db layer.
        """
        try:
            from ai.processor import EnrichedProduct
        except ImportError:
            return []

        adapted = []
        for p in products:
            adapted.append(EnrichedProduct(
                product_id=p.product_id,
                site=p.source_site,
                name=p.product_name,
                original_url=p.product_url,
                scrape_date=p.scraped_at,
                first_seen_date=p.first_seen_at or p.scraped_at,
                description_short=p.short_description or "",
                description_ai_expanded=p.original_description or p.short_description or p.product_name,
                tags=[p.source_site, p.category or "אופנה"],
                colors_available=p.colors_available,
                sizes_available=p.sizes_available,
                price=p.current_price,
                original_price=p.original_price,
                discount_percentage=p.discount_percent,
                is_on_sale=p.is_on_sale,
                images=p.image_urls,
                category=p.category or "אופנה",
            ))
        return adapted

    def save_source_stats(self, stats: SourceStats) -> None:
        """Save run stats to Firestore meta collection."""
        try:
            db = self._get_db()
            if hasattr(db, "save_run_metadata"):
                db.save_run_metadata(stats.to_dict())
        except Exception as e:
            log.warning(f"Could not save stats to Firestore: {e}")
