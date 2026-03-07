"""
Firestore manager.
Handles upsert (create-or-update), batch writes, and metadata tracking.
Free tier: 50k reads/day, 20k writes/day, 20k deletes/day — plenty for our use.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

import config
from ai.processor import EnrichedProduct
from utils.logger import get_logger

log = get_logger("firestore")

_app: Optional[firebase_admin.App] = None
_db = None


def init_firebase() -> None:
    """Initialize Firebase Admin SDK (idempotent)."""
    global _app, _db
    if _app is not None:
        return

    creds_path = Path(config.FIREBASE_CREDENTIALS_PATH)
    if not creds_path.exists():
        raise FileNotFoundError(
            f"Firebase credentials not found at: {creds_path}\n"
            "Please download your service account JSON from Firebase Console."
        )

    cred = credentials.Certificate(str(creds_path))
    _app = firebase_admin.initialize_app(cred, {
        "projectId": config.FIREBASE_PROJECT_ID,
    })
    _db = firestore.client()
    log.info("Firebase initialized successfully")


def get_db():
    if _db is None:
        init_firebase()
    return _db


class FirestoreManager:
    def __init__(self):
        init_firebase()
        self.db = get_db()
        self.collection = self.db.collection(config.FIRESTORE_COLLECTION)
        self.meta = self.db.collection(config.FIRESTORE_META_COLLECTION)

    def get_existing_dates(self, product_ids: list[str]) -> dict[str, str]:
        """
        Fetch first_seen_date for existing products.
        Used to preserve first_seen_date on updates.
        """
        if not product_ids:
            return {}

        existing: dict[str, str] = {}
        # Firestore 'in' query supports max 30 items per call
        chunk_size = 30
        for i in range(0, len(product_ids), chunk_size):
            chunk = product_ids[i : i + chunk_size]
            try:
                docs = self.collection.where(
                    filter=FieldFilter("product_id", "in", chunk)
                ).select(["product_id", "first_seen_date"]).stream()
                for doc in docs:
                    d = doc.to_dict()
                    if d.get("product_id") and d.get("first_seen_date"):
                        existing[d["product_id"]] = d["first_seen_date"]
            except Exception as e:
                log.warning(f"Error fetching existing dates chunk: {e}")

        log.info(f"Found {len(existing)} existing products in Firestore")
        return existing

    def upsert_products(self, products: list[EnrichedProduct]) -> tuple[int, int]:
        """
        Batch upsert products.
        Returns (created_count, updated_count).
        """
        if not products:
            return 0, 0

        existing = self.get_existing_dates([p.product_id for p in products])
        created = 0
        updated = 0

        # Firestore batch: max 500 ops per batch
        batch = self.db.batch()
        batch_count = 0
        MAX_BATCH = 490

        for product in products:
            doc_ref = self.collection.document(product.product_id)
            doc_data = product.to_dict()

            # Add server timestamp fields
            doc_data["updated_at"] = firestore.SERVER_TIMESTAMP

            if product.product_id in existing:
                updated += 1
            else:
                doc_data["created_at"] = firestore.SERVER_TIMESTAMP
                created += 1

            batch.set(doc_ref, doc_data, merge=True)
            batch_count += 1

            # Commit when approaching limit
            if batch_count >= MAX_BATCH:
                try:
                    batch.commit()
                    log.info(f"Committed batch of {batch_count} documents")
                except Exception as e:
                    log.error(f"Batch commit failed: {e}")
                batch = self.db.batch()
                batch_count = 0

        # Commit remaining
        if batch_count > 0:
            try:
                batch.commit()
                log.info(f"Committed final batch of {batch_count} documents")
            except Exception as e:
                log.error(f"Final batch commit failed: {e}")

        log.info(f"Upsert complete: {created} created, {updated} updated")
        return created, updated

    def save_run_metadata(self, run_stats: dict) -> None:
        """Save scrape run metadata for monitoring."""
        now = datetime.now(timezone.utc)
        doc_id = now.strftime("%Y%m%d_%H%M%S")
        try:
            self.meta.document(doc_id).set({
                **run_stats,
                "timestamp": firestore.SERVER_TIMESTAMP,
                "date": now.isoformat(),
            })
            log.info(f"Run metadata saved: {doc_id}")
        except Exception as e:
            log.warning(f"Failed to save run metadata: {e}")

    def get_product_count(self) -> dict[str, int]:
        """Count products per site (for dashboard)."""
        counts: dict[str, int] = {}
        try:
            # Aggregate query (Firestore supports this now)
            docs = self.collection.select(["site"]).stream()
            for doc in docs:
                site = doc.to_dict().get("site", "unknown")
                counts[site] = counts.get(site, 0) + 1
        except Exception as e:
            log.warning(f"Count query failed: {e}")
        return counts

    def get_recent_runs(self, n: int = 10) -> list[dict]:
        """Return last N scrape run summaries."""
        try:
            docs = (
                self.meta.order_by("timestamp", direction=firestore.Query.DESCENDING)
                .limit(n)
                .stream()
            )
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            log.warning(f"Failed to get recent runs: {e}")
            return []
