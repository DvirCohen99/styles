"""
Sync Orchestrator — drives per-source scraping and Firestore ingestion.

Responsibilities:
- Manage sync lifecycle per source (state machine)
- Run adapters and push results to Firestore
- Update source_sync_state, source_stats, sync_logs
- Detect missing/stale products
- Support: run-all, run-source, run-incremental, retry-failures
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("engine.sync.orchestrator")

# Sync stages
STAGE_IDLE       = "idle"
STAGE_DISCOVER   = "discovering"
STAGE_SCRAPE     = "scraping"
STAGE_INGEST     = "ingesting"
STAGE_CLEANUP    = "cleanup"
STAGE_STATS      = "stats"
STAGE_DONE       = "done"
STAGE_FAILED     = "failed"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SyncResult:
    def __init__(self, source_key: str, run_id: str):
        self.source_key = source_key
        self.run_id = run_id
        self.started_at = _now_iso()
        self.finished_at: Optional[str] = None
        self.duration_sec: Optional[float] = None
        self.products_discovered = 0
        self.products_scraped = 0
        self.products_failed = 0
        self.products_created = 0
        self.products_updated = 0
        self.products_missing = 0
        self.price_changes = 0
        self.stock_changes = 0
        self.sale_changes = 0
        self.parse_failures: list[dict] = []
        self.error: Optional[str] = None
        self.success = False

    def finish(self, success: bool = True, error: str = "") -> None:
        self.finished_at = _now_iso()
        self.success = success
        if error:
            self.error = error
        if self.started_at:
            start = datetime.fromisoformat(self.started_at)
            now = datetime.now(timezone.utc)
            self.duration_sec = round((now - start).total_seconds(), 2)

    def to_dict(self) -> dict:
        return {
            "source_key": self.source_key,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_sec": self.duration_sec,
            "products_discovered": self.products_discovered,
            "products_scraped": self.products_scraped,
            "products_failed": self.products_failed,
            "products_created": self.products_created,
            "products_updated": self.products_updated,
            "products_missing": self.products_missing,
            "price_changes": self.price_changes,
            "stock_changes": self.stock_changes,
            "sale_changes": self.sale_changes,
            "error": self.error,
            "success": self.success,
        }


class SyncOrchestrator:
    """
    Drives the full sync pipeline for one or all sources.
    """

    def __init__(
        self,
        product_limit: int = 500,
        dry_run: bool = False,
        verbose: bool = True,
    ):
        self.product_limit = product_limit
        self.dry_run = dry_run
        self.verbose = verbose
        self._db: Optional[object] = None
        self._init_db()

    def _init_db(self) -> None:
        try:
            from engine.firestore.manager import FirestoreIngestionManager
            self._db = FirestoreIngestionManager()
            if not self._db.available:
                log.warning("Firestore not available — running without persistence")
                self._db = None
        except Exception as e:
            log.warning(f"Could not init Firestore: {e}")
            self._db = None

    @property
    def db(self):
        return self._db

    def _log(self, msg: str) -> None:
        if self.verbose:
            log.info(msg)
        else:
            log.debug(msg)

    # ── Public commands ───────────────────────────────────────────────────────

    def run_all_sources(self) -> list[SyncResult]:
        """Run sync for all registered sources in priority order."""
        from engine.registry.source_registry import SourceRegistry

        entries = SourceRegistry.active_entries()
        entries.sort(key=lambda e: e.meta.priority)

        results = []
        self._log(f"Starting sync for {len(entries)} sources")

        if self.db:
            self.db.log_system_event("sync_all_started", sources=[e.key for e in entries])

        for entry in entries:
            result = self.run_source(entry.key)
            results.append(result)

        successes = sum(1 for r in results if r.success)
        failures = sum(1 for r in results if not r.success)
        self._log(f"All-source sync complete: {successes} ok, {failures} failed")

        if self.db:
            self.db.log_system_event(
                "sync_all_finished",
                successes=successes,
                failures=failures,
                total_created=sum(r.products_created for r in results),
                total_updated=sum(r.products_updated for r in results),
            )

        return results

    def run_source(self, source_key: str, incremental: bool = False) -> SyncResult:
        """Run sync for a single source."""
        run_id = f"{source_key}_{uuid.uuid4().hex[:8]}"
        result = SyncResult(source_key, run_id)

        self._log(f"[{source_key}] Starting sync (run={run_id})")
        self._set_state(source_key, STAGE_DISCOVER, run_id, "Starting discovery")

        try:
            # Emit start event
            if self.db:
                self.db.log_sync_event(source_key, run_id, "sync_started")
                self.db.log_system_event("sync_started", source_key=source_key, run_id=run_id)

            # Get adapter
            adapter = self._get_adapter(source_key)
            if not adapter:
                raise RuntimeError(f"No adapter found for '{source_key}'")

            # Scrape
            self._set_state(source_key, STAGE_SCRAPE, run_id, "Scraping products")
            products, stats = adapter.scrape_all(limit=self.product_limit)

            result.products_discovered = stats.urls_discovered
            result.products_scraped = stats.products_parsed
            result.products_failed = stats.products_failed

            # Log parse failures
            if stats.error_messages and self.db:
                for err in stats.error_messages[:20]:
                    self.db.log_parse_failure(source_key, "", err, run_id)

            self._log(f"[{source_key}] Scraped {len(products)} products ({stats.products_failed} failed)")

            if products and self.db and not self.dry_run:
                # Convert to dicts for Firestore
                self._set_state(source_key, STAGE_INGEST, run_id, f"Ingesting {len(products)} products")
                product_dicts = []
                seen_ids = set()

                for p in products:
                    try:
                        d = p.to_firebase_dict()
                        d["completeness_score"] = p.completeness_score
                        product_dicts.append(d)
                        seen_ids.add(p.product_id)
                    except Exception as e:
                        log.warning(f"[{source_key}] Product to dict failed: {e}")

                if product_dicts:
                    counters = self.db.upsert_products(product_dicts, source_key, run_id)
                    result.products_created = counters.get("created", 0)
                    result.products_updated = counters.get("updated", 0)
                    result.price_changes = counters.get("price_changed", 0)
                    result.stock_changes = counters.get("stock_changed", 0)
                    result.sale_changes = counters.get("sale_changed", 0)

                # Detect missing products
                self._set_state(source_key, STAGE_CLEANUP, run_id, "Detecting missing products")
                if not incremental:
                    result.products_missing = self.db.mark_missing_products(
                        source_key, seen_ids, run_id
                    )

                # Recompute source stats
                self._set_state(source_key, STAGE_STATS, run_id, "Updating stats")
                computed_stats = self.db.compute_and_save_source_stats(source_key)

                # Persist source stats with sync result
                self.db.update_source_stats(source_key, {
                    "last_successful_sync": _now_iso(),
                    "last_sync_run_id": run_id,
                    "last_sync_products_scraped": len(products),
                    "last_sync_duration_sec": result.duration_sec,
                    "parser_health_status": stats.parser_health_status,
                    "sync_success_rate": self._compute_success_rate(source_key, True),
                })

            # Done
            result.finish(success=True)
            self._set_state(source_key, STAGE_DONE, run_id, "Sync complete", result=result)

            if self.db:
                self.db.log_sync_event(source_key, run_id, "sync_completed",
                    products_created=result.products_created,
                    products_updated=result.products_updated,
                    duration_sec=result.duration_sec)
                self.db.log_system_event("sync_completed",
                    source_key=source_key,
                    run_id=run_id,
                    products_scraped=result.products_scraped,
                    products_created=result.products_created,
                    duration_sec=result.duration_sec)

            self._log(f"[{source_key}] Sync done in {result.duration_sec}s — "
                      f"+{result.products_created} new, ~{result.products_updated} updated")

        except Exception as e:
            result.finish(success=False, error=str(e))
            log.error(f"[{source_key}] Sync FAILED: {e}", exc_info=True)

            self._set_state(source_key, STAGE_FAILED, run_id, str(e)[:500])

            if self.db:
                try:
                    self.db.update_source_stats(source_key, {
                        "last_failed_sync": _now_iso(),
                        "last_error": str(e)[:500],
                        "sync_success_rate": self._compute_success_rate(source_key, False),
                    })
                    self.db.log_sync_event(source_key, run_id, "sync_failed", error=str(e)[:500])
                    self.db.log_system_event("sync_failed",
                        source_key=source_key, run_id=run_id, error=str(e)[:500])
                except Exception:
                    pass

        return result

    def run_incremental(self, source_key: str) -> SyncResult:
        """Run sync without marking missing products (incremental update)."""
        return self.run_source(source_key, incremental=True)

    def refresh_source_stats(self, source_key: Optional[str] = None) -> dict:
        """Recompute source stats from scratch."""
        if not self.db:
            log.warning("Firestore not available for stats refresh")
            return {}

        from engine.registry.source_registry import SourceRegistry
        keys = [source_key] if source_key else SourceRegistry.all_keys()

        results = {}
        for key in keys:
            self._log(f"Refreshing stats for {key}")
            stats = self.db.compute_and_save_source_stats(key)
            results[key] = stats

        return results

    def verify_stale_products(self, source_key: Optional[str] = None, days: int = 7) -> dict:
        """Find products not seen in the last N days."""
        if not self.db or not self.db.available:
            return {}

        from datetime import timedelta
        stale_cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        stale_counts = {}
        try:
            from engine.registry.source_registry import SourceRegistry
            keys = [source_key] if source_key else SourceRegistry.all_keys()

            col = self.db.db.collection("products")
            for key in keys:
                docs = col.where("source_site", "==", key)\
                          .where("is_active", "==", True)\
                          .where("last_seen_at", "<", stale_cutoff)\
                          .select(["product_id"]).stream()
                count = sum(1 for _ in docs)
                stale_counts[key] = count
                if count:
                    log.warning(f"[{key}] {count} stale products (not seen in {days}d)")
        except Exception as e:
            log.error(f"Stale product verification failed: {e}")

        return stale_counts

    def retry_failures(self, source_key: Optional[str] = None) -> dict:
        """Retry previously failed parse URLs."""
        if not self.db:
            return {}

        failures = self.db.get_parse_failures(source_key=source_key, limit=50)
        if not failures:
            log.info("No parse failures to retry")
            return {}

        retry_counts: dict[str, int] = {}
        for failure in failures:
            src = failure.get("source_key", "")
            retry_counts[src] = retry_counts.get(src, 0) + 1

        log.info(f"Found {len(failures)} parse failures across {len(retry_counts)} sources")
        # Re-run full sync for affected sources (simple approach)
        results = {}
        for src in retry_counts:
            result = self.run_source(src)
            results[src] = {"success": result.success, "scraped": result.products_scraped}

        return results

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_adapter(self, source_key: str):
        try:
            from engine.registry.source_registry import get_adapter
            return get_adapter(source_key)
        except Exception as e:
            log.error(f"Failed to get adapter for '{source_key}': {e}")
            return None

    def _set_state(
        self,
        source_key: str,
        stage: str,
        run_id: str,
        message: str = "",
        result: Optional[SyncResult] = None,
    ) -> None:
        if not self.db:
            return

        state: dict = {
            "current_job_status": "running" if stage not in (STAGE_DONE, STAGE_FAILED, STAGE_IDLE) else stage,
            "current_stage": stage,
            "current_message": message,
            "last_started_at": result.started_at if result else _now_iso(),
        }

        if stage == STAGE_DONE and result:
            state["current_job_status"] = "idle"
            state["last_finished_at"] = result.finished_at
            state["adapter_status"] = "ok"
            state["last_run_id"] = run_id
            state["last_sync_counts"] = {
                "created": result.products_created,
                "updated": result.products_updated,
                "scraped": result.products_scraped,
                "missing": result.products_missing,
            }
        elif stage == STAGE_FAILED:
            state["current_job_status"] = "failed"
            state["adapter_status"] = "error"
            state["current_error"] = message
            state["last_failed_at"] = _now_iso()

        try:
            self.db.set_sync_state(source_key, state)
        except Exception as e:
            log.debug(f"Set state failed [{source_key}]: {e}")

    def _compute_success_rate(self, source_key: str, last_success: bool) -> float:
        """Simple exponential moving average for success rate."""
        if not self.db:
            return 1.0 if last_success else 0.0
        existing = self.db.get_source_stats(source_key)
        prev_rate = existing.get("sync_success_rate", 1.0) or 1.0
        alpha = 0.2  # weight for latest result
        new_rate = alpha * (1.0 if last_success else 0.0) + (1 - alpha) * prev_rate
        return round(new_rate, 3)
