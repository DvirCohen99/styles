"""
Firestore Ingestion Manager — core database layer.

Collections managed:
  products            — main product documents (upserted)
  sources             — static source metadata
  source_stats        — per-source rolling statistics
  source_sync_state   — live sync state per source
  sync_logs           — per-event sync log entries
  parse_failures      — individual parse failures
  system_events       — system-wide events (start, finish, errors)
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("engine.firestore")

# ── Firestore collection names ─────────────────────────────────────────────────
COL_PRODUCTS        = "products"
COL_SOURCES         = "sources"
COL_SOURCE_STATS    = "source_stats"
COL_SOURCE_SYNC     = "source_sync_state"
COL_SYNC_LOGS       = "sync_logs"
COL_PARSE_FAILURES  = "parse_failures"
COL_SYSTEM_EVENTS   = "system_events"

_MAX_BATCH = 490  # Firestore max 500 ops/batch, leave headroom


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _event_id() -> str:
    return f"{int(_now().timestamp() * 1000)}_{uuid.uuid4().hex[:8]}"


# ──────────────────────────────────────────────────────────────────────────────
class FirestoreIngestionManager:
    """
    Main Firestore interface for the ingestion engine.
    Handles all reads and writes across all collections.
    """

    def __init__(self):
        self._db = None
        self._init_firebase()

    # ── Init ──────────────────────────────────────────────────────────────────

    def _init_firebase(self) -> None:
        try:
            import firebase_admin
            from firebase_admin import credentials, firestore as fs

            # Check if already initialized
            try:
                app = firebase_admin.get_app()
                self._db = fs.client(app)
                return
            except ValueError:
                pass

            # Find credentials
            creds_path = self._find_credentials()
            if creds_path:
                cred = credentials.Certificate(str(creds_path))
                app = firebase_admin.initialize_app(cred)
            else:
                # Try application default credentials
                app = firebase_admin.initialize_app()

            self._db = fs.client(app)
            log.info("Firestore initialized successfully")

        except ImportError:
            log.warning("firebase-admin not installed — Firestore disabled")
        except Exception as e:
            log.error(f"Firestore init failed: {e}")

    def _find_credentials(self) -> Optional[Path]:
        """Find Firebase credentials file in common locations."""
        candidates = [
            Path("config/firebase_credentials.json"),
            Path("firebase_credentials.json"),
            Path.home() / ".config/firebase_credentials.json",
        ]
        # Also check environment variable
        import os
        env_path = os.environ.get("FIREBASE_CREDENTIALS_PATH") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if env_path:
            candidates.insert(0, Path(env_path))

        for p in candidates:
            if p.exists():
                return p
        return None

    @property
    def db(self):
        if self._db is None:
            raise RuntimeError("Firestore not initialized — check credentials")
        return self._db

    @property
    def available(self) -> bool:
        return self._db is not None

    # ── Products ──────────────────────────────────────────────────────────────

    def upsert_products(
        self,
        products: list[dict],
        source_key: str,
        run_id: str,
    ) -> dict[str, int]:
        """
        Batch upsert products with full change detection.

        Returns dict with keys: created, updated, price_changed,
        stock_changed, sale_changed, unchanged.
        """
        if not products or not self.available:
            return {"created": 0, "updated": 0, "price_changed": 0,
                    "stock_changed": 0, "sale_changed": 0, "unchanged": 0}

        from firebase_admin import firestore as fs

        col = self.db.collection(COL_PRODUCTS)
        product_ids = [p["product_id"] for p in products]

        # Fetch existing docs in chunks (Firestore 'in' max 30)
        existing = self._fetch_existing_products(product_ids)

        counters = {"created": 0, "updated": 0, "price_changed": 0,
                    "stock_changed": 0, "sale_changed": 0, "unchanged": 0}

        events_to_log = []
        batch = self.db.batch()
        batch_count = 0
        now = _now()

        for product in products:
            pid = product["product_id"]
            doc_ref = col.document(pid)
            existing_doc = existing.get(pid)

            changes = self._detect_changes(product, existing_doc)
            doc_data = self._build_product_doc(product, existing_doc, changes, now)

            batch.set(doc_ref, doc_data, merge=True)
            batch_count += 1

            # Count change types
            if not existing_doc:
                counters["created"] += 1
                events_to_log.append(self._make_sync_event(
                    source_key, run_id, "product_created",
                    product_id=pid,
                    product_name=product.get("product_name", ""),
                    product_url=product.get("product_url", ""),
                ))
            else:
                if changes.get("price_changed"):
                    counters["price_changed"] += 1
                    events_to_log.append(self._make_sync_event(
                        source_key, run_id, "price_changed",
                        product_id=pid,
                        product_name=product.get("product_name", ""),
                        old_value=changes.get("old_price"),
                        new_value=changes.get("new_price"),
                    ))
                if changes.get("stock_changed"):
                    counters["stock_changed"] += 1
                    events_to_log.append(self._make_sync_event(
                        source_key, run_id, "stock_changed",
                        product_id=pid,
                        product_name=product.get("product_name", ""),
                        old_value=changes.get("old_stock"),
                        new_value=changes.get("new_stock"),
                    ))
                if changes.get("sale_changed"):
                    counters["sale_changed"] += 1
                    events_to_log.append(self._make_sync_event(
                        source_key, run_id, "sale_changed",
                        product_id=pid,
                        product_name=product.get("product_name", ""),
                        old_value=changes.get("old_sale"),
                        new_value=changes.get("new_sale"),
                    ))
                if any(changes.values()):
                    counters["updated"] += 1
                else:
                    counters["unchanged"] += 1

            if batch_count >= _MAX_BATCH:
                self._commit_batch(batch)
                batch = self.db.batch()
                batch_count = 0

        if batch_count > 0:
            self._commit_batch(batch)

        # Write events
        self._write_sync_events(events_to_log)

        log.info(f"[{source_key}] Upsert: {counters}")
        return counters

    def _fetch_existing_products(self, product_ids: list[str]) -> dict[str, dict]:
        """Fetch existing product docs for change comparison."""
        existing: dict[str, dict] = {}
        col = self.db.collection(COL_PRODUCTS)
        chunk_size = 30
        fields = [
            "product_id", "current_price", "original_price",
            "is_on_sale", "stock_status", "in_stock",
            "first_seen_at", "last_price_change_at", "last_stock_change_at",
        ]
        for i in range(0, len(product_ids), chunk_size):
            chunk = product_ids[i:i + chunk_size]
            try:
                docs = col.where("product_id", "in", chunk).select(fields).stream()
                for doc in docs:
                    d = doc.to_dict()
                    if d:
                        existing[d["product_id"]] = d
            except Exception as e:
                log.warning(f"Fetch existing chunk failed: {e}")
        return existing

    def _detect_changes(self, new: dict, old: Optional[dict]) -> dict:
        """Detect what changed between new and existing product."""
        if not old:
            return {}

        changes: dict[str, Any] = {}

        # Price change
        old_price = old.get("current_price")
        new_price = new.get("current_price")
        if old_price is not None and new_price is not None and old_price != new_price:
            changes["price_changed"] = True
            changes["old_price"] = old_price
            changes["new_price"] = new_price

        # Stock change
        old_stock = old.get("stock_status")
        new_stock = new.get("stock_status")
        if old_stock and new_stock and old_stock != new_stock:
            changes["stock_changed"] = True
            changes["old_stock"] = old_stock
            changes["new_stock"] = new_stock

        # Sale change
        old_sale = old.get("is_on_sale")
        new_sale = new.get("is_on_sale")
        if old_sale is not None and new_sale is not None and old_sale != new_sale:
            changes["sale_changed"] = True
            changes["old_sale"] = old_sale
            changes["new_sale"] = new_sale

        return changes

    def _build_product_doc(
        self,
        product: dict,
        existing: Optional[dict],
        changes: dict,
        now: datetime,
    ) -> dict:
        """Build the full Firestore product document."""
        from firebase_admin import firestore as fs

        now_iso = now.isoformat()

        doc = {
            # Identity
            "product_id": product.get("product_id", ""),
            "source_site": product.get("source_site", ""),
            "source_name": product.get("source_name", ""),
            "product_url": product.get("product_url", ""),
            "canonical_url": product.get("canonical_url"),
            "source_product_reference": product.get("source_product_reference"),
            "sku": product.get("sku_if_available"),

            # Text
            "product_name": product.get("product_name", ""),
            "short_description": product.get("short_description"),
            "original_description": product.get("original_description"),
            "ai_extended_description": product.get("ai_extended_description"),
            "searchable_text": product.get("searchable_text_blob"),
            "breadcrumbs": product.get("breadcrumbs", []),
            "bullet_points": product.get("bullet_points", []),

            # Pricing
            "current_price": product.get("current_price"),
            "original_price": product.get("original_price"),
            "currency": product.get("currency", "ILS"),
            "is_on_sale": product.get("is_on_sale", False),
            "discount_amount": product.get("discount_amount"),
            "discount_percent": product.get("discount_percent"),
            "sale_label": product.get("sale_label"),
            "promotion_text": product.get("promotion_text"),

            # Classification
            "category": product.get("category"),
            "subcategory": product.get("subcategory"),
            "product_type": product.get("product_type"),
            "collection": product.get("collection"),
            "subcollection": product.get("subcollection"),
            "collection_type": product.get("collection_type"),
            "is_new_collection": product.get("is_new_collection", False),
            "brand": product.get("brand"),
            "gender_target": product.get("gender_target"),

            # Images
            "primary_image_url": product.get("primary_image_url"),
            "image_urls": product.get("image_urls", []),
            "image_count": product.get("image_count", 0),

            # Variants (flat arrays for most sources)
            "colors_available": product.get("colors_available", []),
            "sizes_available": product.get("sizes_available", []),
            "size_labels_normalized": product.get("size_labels_normalized", []),
            "per_variant_stock": product.get("per_variant_stock_if_available", {}),

            # Stock
            "stock_status": product.get("stock_status", "unknown"),
            "in_stock": product.get("in_stock", True),
            "low_stock": product.get("low_stock", False),
            "out_of_stock": product.get("out_of_stock", False),
            "availability_text": product.get("availability_text"),

            # System
            "extraction_confidence": product.get("extraction_confidence", 1.0),
            "data_quality_score": product.get("completeness_score", 0.0),
            "parser_version": product.get("parser_version", "1.0.0"),
            "source_mode": product.get("extraction_method", "unknown"),
            "warnings": product.get("warnings", []),
            "parser_status": product.get("parser_status", "ok"),

            # Status
            "is_active": True,
            "is_missing_from_source": False,
            "is_deleted": False,

            # Timestamps (always updated)
            "last_seen_at": now_iso,
            "updated_at_in_system": fs.SERVER_TIMESTAMP,
        }

        # Preserve first_seen_at
        if existing and existing.get("first_seen_at"):
            doc["first_seen_at"] = existing["first_seen_at"]
        else:
            doc["first_seen_at"] = now_iso
            doc["created_at_in_system"] = fs.SERVER_TIMESTAMP

        # Track change timestamps
        if changes.get("price_changed"):
            doc["last_price_change_at"] = now_iso
            doc["prev_price"] = changes.get("old_price")
        elif existing and existing.get("last_price_change_at"):
            doc["last_price_change_at"] = existing["last_price_change_at"]

        if changes.get("stock_changed"):
            doc["last_stock_change_at"] = now_iso
        elif existing and existing.get("last_stock_change_at"):
            doc["last_stock_change_at"] = existing["last_stock_change_at"]

        return doc

    def _commit_batch(self, batch) -> None:
        try:
            batch.commit()
        except Exception as e:
            log.error(f"Batch commit failed: {e}")

    # ── Mark missing products ─────────────────────────────────────────────────

    def mark_missing_products(
        self,
        source_key: str,
        seen_product_ids: set[str],
        run_id: str,
    ) -> int:
        """Mark products not seen in current sync as missing."""
        if not self.available:
            return 0

        from firebase_admin import firestore as fs

        col = self.db.collection(COL_PRODUCTS)
        missing_count = 0

        try:
            # Get all active products for this source
            docs = col.where("source_site", "==", source_key)\
                      .where("is_active", "==", True)\
                      .where("is_missing_from_source", "==", False)\
                      .select(["product_id", "product_name", "product_url"])\
                      .stream()

            batch = self.db.batch()
            batch_count = 0
            events = []

            for doc in docs:
                d = doc.to_dict()
                pid = d.get("product_id", "")
                if pid and pid not in seen_product_ids:
                    batch.update(doc.reference, {
                        "is_missing_from_source": True,
                        "missing_since": _now_iso(),
                        "updated_at_in_system": fs.SERVER_TIMESTAMP,
                    })
                    batch_count += 1
                    missing_count += 1
                    events.append(self._make_sync_event(
                        source_key, run_id, "product_disappeared",
                        product_id=pid,
                        product_name=d.get("product_name", ""),
                        product_url=d.get("product_url", ""),
                    ))

                    if batch_count >= _MAX_BATCH:
                        self._commit_batch(batch)
                        batch = self.db.batch()
                        batch_count = 0

            if batch_count > 0:
                self._commit_batch(batch)

            self._write_sync_events(events)

        except Exception as e:
            log.error(f"Mark missing products failed: {e}")

        if missing_count:
            log.info(f"[{source_key}] Marked {missing_count} products as missing")
        return missing_count

    # ── Source metadata ───────────────────────────────────────────────────────

    def upsert_source_meta(self, source_key: str, meta: dict) -> None:
        """Write or update source metadata document."""
        if not self.available:
            return
        try:
            from firebase_admin import firestore as fs
            self.db.collection(COL_SOURCES).document(source_key).set({
                **meta,
                "updated_at": fs.SERVER_TIMESTAMP,
            }, merge=True)
        except Exception as e:
            log.warning(f"Failed to save source meta [{source_key}]: {e}")

    # ── Source stats ──────────────────────────────────────────────────────────

    def update_source_stats(self, source_key: str, stats: dict) -> None:
        """Merge source statistics document."""
        if not self.available:
            return
        try:
            from firebase_admin import firestore as fs
            self.db.collection(COL_SOURCE_STATS).document(source_key).set({
                "source_key": source_key,
                **stats,
                "stats_updated_at": fs.SERVER_TIMESTAMP,
            }, merge=True)
        except Exception as e:
            log.warning(f"Failed to update source stats [{source_key}]: {e}")

    def compute_and_save_source_stats(self, source_key: str) -> dict:
        """Compute source stats by aggregating the products collection."""
        if not self.available:
            return {}

        try:
            col = self.db.collection(COL_PRODUCTS)
            docs = col.where("source_site", "==", source_key).stream()

            stats = {
                "total_live_products": 0,
                "total_sale_products": 0,
                "total_new_collection_products": 0,
                "total_out_of_stock_products": 0,
                "total_low_stock_products": 0,
                "total_missing_products": 0,
                "missing_fields_count": 0,
                "warnings_count": 0,
                "avg_quality_score": 0.0,
            }
            quality_scores = []

            for doc in docs:
                d = doc.to_dict()
                if not d:
                    continue
                if d.get("is_active") and not d.get("is_missing_from_source"):
                    stats["total_live_products"] += 1
                if d.get("is_on_sale"):
                    stats["total_sale_products"] += 1
                if d.get("is_new_collection"):
                    stats["total_new_collection_products"] += 1
                if d.get("stock_status") == "out_of_stock":
                    stats["total_out_of_stock_products"] += 1
                if d.get("stock_status") == "low_stock":
                    stats["total_low_stock_products"] += 1
                if d.get("is_missing_from_source"):
                    stats["total_missing_products"] += 1
                if d.get("warnings"):
                    stats["warnings_count"] += len(d["warnings"])

                qs = d.get("data_quality_score", 0)
                if qs:
                    quality_scores.append(qs)

            if quality_scores:
                stats["avg_quality_score"] = round(sum(quality_scores) / len(quality_scores), 3)

            self.update_source_stats(source_key, stats)
            return stats

        except Exception as e:
            log.error(f"Compute source stats failed [{source_key}]: {e}")
            return {}

    def get_source_stats(self, source_key: str) -> dict:
        """Retrieve source stats document."""
        if not self.available:
            return {}
        try:
            doc = self.db.collection(COL_SOURCE_STATS).document(source_key).get()
            return doc.to_dict() or {}
        except Exception as e:
            log.warning(f"Get source stats failed [{source_key}]: {e}")
            return {}

    def get_all_source_stats(self) -> dict[str, dict]:
        """Retrieve all source stats documents."""
        if not self.available:
            return {}
        try:
            docs = self.db.collection(COL_SOURCE_STATS).stream()
            return {doc.id: (doc.to_dict() or {}) for doc in docs}
        except Exception as e:
            log.warning(f"Get all source stats failed: {e}")
            return {}

    # ── Source sync state ─────────────────────────────────────────────────────

    def set_sync_state(self, source_key: str, state: dict) -> None:
        """Write live sync state for a source."""
        if not self.available:
            return
        try:
            from firebase_admin import firestore as fs
            self.db.collection(COL_SOURCE_SYNC).document(source_key).set({
                "source_key": source_key,
                **state,
                "state_updated_at": fs.SERVER_TIMESTAMP,
            }, merge=True)
        except Exception as e:
            log.warning(f"Failed to set sync state [{source_key}]: {e}")

    def get_sync_state(self, source_key: str) -> dict:
        """Get current sync state for a source."""
        if not self.available:
            return {}
        try:
            doc = self.db.collection(COL_SOURCE_SYNC).document(source_key).get()
            return doc.to_dict() or {}
        except Exception as e:
            return {}

    def get_all_sync_states(self) -> dict[str, dict]:
        """Get all source sync states."""
        if not self.available:
            return {}
        try:
            docs = self.db.collection(COL_SOURCE_SYNC).stream()
            return {doc.id: (doc.to_dict() or {}) for doc in docs}
        except Exception as e:
            return {}

    # ── Sync logs / events ────────────────────────────────────────────────────

    def _make_sync_event(
        self,
        source_key: str,
        run_id: str,
        event_type: str,
        **kwargs,
    ) -> dict:
        return {
            "event_id": _event_id(),
            "source_key": source_key,
            "run_id": run_id,
            "event_type": event_type,
            "timestamp": _now_iso(),
            **kwargs,
        }

    def _write_sync_events(self, events: list[dict]) -> None:
        if not events or not self.available:
            return

        from firebase_admin import firestore as fs

        col = self.db.collection(COL_SYNC_LOGS)
        batch = self.db.batch()
        batch_count = 0

        for event in events:
            eid = event.get("event_id", _event_id())
            doc_ref = col.document(eid)
            batch.set(doc_ref, {**event, "written_at": fs.SERVER_TIMESTAMP})
            batch_count += 1

            if batch_count >= _MAX_BATCH:
                self._commit_batch(batch)
                batch = self.db.batch()
                batch_count = 0

        if batch_count > 0:
            self._commit_batch(batch)

    def log_sync_event(
        self,
        source_key: str,
        run_id: str,
        event_type: str,
        **kwargs,
    ) -> None:
        """Write a single sync event to sync_logs."""
        event = self._make_sync_event(source_key, run_id, event_type, **kwargs)
        self._write_sync_events([event])

    def get_recent_sync_events(self, limit: int = 100, source_key: Optional[str] = None) -> list[dict]:
        """Get recent sync events, optionally filtered by source."""
        if not self.available:
            return []
        try:
            q = self.db.collection(COL_SYNC_LOGS).order_by(
                "timestamp", direction="DESCENDING"
            ).limit(limit)
            if source_key:
                q = q.where("source_key", "==", source_key)
            docs = q.stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            log.warning(f"Get sync events failed: {e}")
            return []

    # ── System events ─────────────────────────────────────────────────────────

    def log_system_event(self, event_type: str, **kwargs) -> None:
        """Write a system-level event."""
        if not self.available:
            return
        try:
            from firebase_admin import firestore as fs
            eid = _event_id()
            self.db.collection(COL_SYSTEM_EVENTS).document(eid).set({
                "event_id": eid,
                "event_type": event_type,
                "timestamp": _now_iso(),
                "written_at": fs.SERVER_TIMESTAMP,
                **kwargs,
            })
        except Exception as e:
            log.warning(f"Failed to log system event: {e}")

    def get_recent_system_events(self, limit: int = 50) -> list[dict]:
        """Get recent system events."""
        if not self.available:
            return []
        try:
            docs = self.db.collection(COL_SYSTEM_EVENTS)\
                .order_by("timestamp", direction="DESCENDING")\
                .limit(limit)\
                .stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            return []

    # ── Parse failures ────────────────────────────────────────────────────────

    def log_parse_failure(
        self,
        source_key: str,
        url: str,
        error: str,
        run_id: str = "",
        stage: str = "parse",
    ) -> None:
        """Record a parse failure for later retry."""
        if not self.available:
            return
        try:
            from firebase_admin import firestore as fs
            fid = hashlib.md5(f"{source_key}:{url}".encode()).hexdigest()
            self.db.collection(COL_PARSE_FAILURES).document(fid).set({
                "failure_id": fid,
                "source_key": source_key,
                "url": url,
                "error": str(error)[:2000],
                "stage": stage,
                "run_id": run_id,
                "last_failed_at": _now_iso(),
                "retry_count": fs.Increment(1),
                "resolved": False,
                "written_at": fs.SERVER_TIMESTAMP,
            }, merge=True)
        except Exception as e:
            log.warning(f"Failed to log parse failure: {e}")

    def get_parse_failures(self, source_key: Optional[str] = None, limit: int = 50) -> list[dict]:
        """Get unresolved parse failures."""
        if not self.available:
            return []
        try:
            q = self.db.collection(COL_PARSE_FAILURES)\
                .where("resolved", "==", False)\
                .order_by("last_failed_at", direction="DESCENDING")\
                .limit(limit)
            if source_key:
                q = q.where("source_key", "==", source_key)
            return [doc.to_dict() for doc in q.stream()]
        except Exception as e:
            return []

    # ── Products queries ──────────────────────────────────────────────────────

    def get_products(
        self,
        source_key: Optional[str] = None,
        is_on_sale: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Query products with optional filters."""
        if not self.available:
            return []
        try:
            q = self.db.collection(COL_PRODUCTS)\
                .where("is_active", "==", True)\
                .order_by("updated_at_in_system", direction="DESCENDING")\
                .limit(limit)
            if source_key:
                q = q.where("source_site", "==", source_key)
            if is_on_sale is not None:
                q = q.where("is_on_sale", "==", is_on_sale)
            return [doc.to_dict() for doc in q.stream()]
        except Exception as e:
            log.warning(f"Get products query failed: {e}")
            return []

    def get_product(self, product_id: str) -> Optional[dict]:
        """Get a single product by ID."""
        if not self.available:
            return None
        try:
            doc = self.db.collection(COL_PRODUCTS).document(product_id).get()
            return doc.to_dict() if doc.exists else None
        except Exception as e:
            return None

    def get_product_count_by_source(self) -> dict[str, int]:
        """Get product counts grouped by source."""
        if not self.available:
            return {}
        counts: dict[str, int] = {}
        try:
            docs = self.db.collection(COL_PRODUCTS)\
                .where("is_active", "==", True)\
                .select(["source_site"]).stream()
            for doc in docs:
                d = doc.to_dict()
                site = d.get("source_site", "unknown")
                counts[site] = counts.get(site, 0) + 1
        except Exception as e:
            log.warning(f"Product count query failed: {e}")
        return counts

    def get_dashboard_stats(self) -> dict:
        """Aggregate stats for the monitoring dashboard."""
        if not self.available:
            return {}

        try:
            today = datetime.now(timezone.utc).date().isoformat()
            col = self.db.collection(COL_PRODUCTS)

            stats = {
                "total_products": 0,
                "sale_products": 0,
                "new_collection_products": 0,
                "out_of_stock_products": 0,
                "missing_products": 0,
                "products_updated_today": 0,
                "products_added_today": 0,
                "by_source": {},
            }

            docs = col.where("is_active", "==", True).stream()
            for doc in docs:
                d = doc.to_dict()
                if not d:
                    continue
                src = d.get("source_site", "unknown")
                stats["total_products"] += 1
                if d.get("is_on_sale"):
                    stats["sale_products"] += 1
                if d.get("is_new_collection"):
                    stats["new_collection_products"] += 1
                if d.get("stock_status") == "out_of_stock":
                    stats["out_of_stock_products"] += 1
                if d.get("is_missing_from_source"):
                    stats["missing_products"] += 1

                last_seen = d.get("last_seen_at", "")
                if isinstance(last_seen, str) and last_seen.startswith(today):
                    stats["products_updated_today"] += 1

                first_seen = d.get("first_seen_at", "")
                if isinstance(first_seen, str) and first_seen.startswith(today):
                    stats["products_added_today"] += 1

                if src not in stats["by_source"]:
                    stats["by_source"][src] = {"count": 0, "sale": 0, "out_of_stock": 0}
                stats["by_source"][src]["count"] += 1
                if d.get("is_on_sale"):
                    stats["by_source"][src]["sale"] += 1

            return stats

        except Exception as e:
            log.error(f"Dashboard stats failed: {e}")
            return {}
