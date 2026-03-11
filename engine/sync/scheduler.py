"""
Scheduler — runs sync jobs on a schedule or in a continuous loop.

Modes:
  loop      — run all sources then sleep, forever
  cron      — run once (intended to be called by system cron)
  watch     — run failed/stale sources more frequently
"""
from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("engine.sync.scheduler")


class Scheduler:
    def __init__(
        self,
        interval_minutes: int = 360,  # 6 hours default
        product_limit: int = 500,
        sources: Optional[list[str]] = None,
        verbose: bool = True,
    ):
        self.interval_minutes = interval_minutes
        self.product_limit = product_limit
        self.sources = sources  # None = all
        self.verbose = verbose
        self._running = False
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        def _stop(signum, frame):
            log.info("Received stop signal — shutting down scheduler")
            self._running = False

        try:
            signal.signal(signal.SIGTERM, _stop)
            signal.signal(signal.SIGINT, _stop)
        except Exception:
            pass

    def _make_orchestrator(self):
        from engine.sync.orchestrator import SyncOrchestrator
        return SyncOrchestrator(
            product_limit=self.product_limit,
            verbose=self.verbose,
        )

    def run_once(self) -> list:
        """Run all sources once and return results."""
        log.info("Scheduler: running once")
        orch = self._make_orchestrator()

        if self.sources:
            results = [orch.run_source(src) for src in self.sources]
        else:
            results = orch.run_all_sources()

        successes = sum(1 for r in results if r.success)
        log.info(f"Run complete: {successes}/{len(results)} sources succeeded")
        return results

    def run_loop(self) -> None:
        """Run sources in an infinite loop with sleep between rounds."""
        self._running = True
        log.info(f"Scheduler loop started — interval={self.interval_minutes}m")

        while self._running:
            start = time.monotonic()
            log.info(f"[{datetime.now(timezone.utc).isoformat()}] Loop iteration starting")

            try:
                self.run_once()
            except Exception as e:
                log.error(f"Scheduler loop iteration failed: {e}", exc_info=True)

            if not self._running:
                break

            elapsed_min = (time.monotonic() - start) / 60
            sleep_min = max(0, self.interval_minutes - elapsed_min)
            log.info(f"Loop done in {elapsed_min:.1f}m — sleeping {sleep_min:.1f}m")

            # Sleep in small chunks to allow clean shutdown
            sleep_sec = sleep_min * 60
            slept = 0
            while slept < sleep_sec and self._running:
                time.sleep(min(30, sleep_sec - slept))
                slept += 30

        log.info("Scheduler loop stopped")
