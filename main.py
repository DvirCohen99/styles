#!/usr/bin/env python3
"""
Fashion Scraper — Main Orchestrator
=====================================
Modes:
  --mode test    : Scrape 3 products from 1 site, test AI + Firestore
  --mode full    : Full run — all active sites
  --mode auto    : Like full, but quieter (for cron)
  --mode site X  : Scrape only site X

Usage:
  python main.py --mode test
  python main.py --mode full
  python main.py --mode site zara
  python main.py --mode auto        ← called by cron
"""
from __future__ import annotations

import argparse
import sys
import json
import time
from datetime import datetime, timezone
from pathlib import Path

# Rich for nice terminal output
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False
    class _FallbackConsole:
        def print(self, *a, **kw): print(*a)
        def rule(self, *a, **kw): print("─" * 50)
    console = _FallbackConsole()

import config
from utils.logger import get_logger
from scrapers.registry import get_scraper, SCRAPERS
from ai.processor import GeminiProcessor
from db.firestore import FirestoreManager

log = get_logger("main")


def banner():
    console.print(Panel.fit(
        "[bold cyan]🛍️  Fashion Auto-Scraper[/bold cyan]\n"
        "[dim]Android • Termux • Gemini • Firestore[/dim]",
        border_style="cyan",
    ) if HAS_RICH else "=== Fashion Auto-Scraper ===")


def validate_config() -> bool:
    errors = config.validate()
    if errors:
        console.print("\n[red]❌ Configuration errors:[/red]" if HAS_RICH else "\n❌ Configuration errors:")
        for e in errors:
            console.print(f"  • {e}")
        console.print(
            "\n[yellow]Edit config.env and run again.[/yellow]\n"
            if HAS_RICH else "\nEdit config.env and run again.\n"
        )
        return False
    return True


def run_scraper(
    site_key: str,
    limit: int,
    ai_processor: GeminiProcessor,
    db: FirestoreManager,
    test_mode: bool = False,
) -> dict:
    """Scrape one site end-to-end. Returns stats dict."""
    stats = {
        "site": site_key,
        "scraped": 0,
        "enriched": 0,
        "created": 0,
        "updated": 0,
        "errors": 0,
        "duration_sec": 0,
    }
    t_start = time.time()

    try:
        scraper = get_scraper(site_key)
    except ValueError as e:
        log.error(str(e))
        stats["errors"] = 1
        return stats

    console.print(f"\n{'[bold]' if HAS_RICH else ''}▶ Scraping {scraper.SITE_NAME}...{'[/bold]' if HAS_RICH else ''}")

    # ── 1. Scrape ─────────────────────────────────────────
    raw_products = scraper.scrape_all(limit=limit)
    stats["scraped"] = len(raw_products)

    if not raw_products:
        log.warning(f"No products scraped from {site_key}")
        return stats

    # ── 2. Fetch existing dates (for upsert) ───────────────
    try:
        existing_dates = db.get_existing_dates([p.product_id for p in raw_products])
    except Exception as e:
        log.warning(f"Could not fetch existing dates: {e}")
        existing_dates = {}

    # ── 3. AI Enrichment ───────────────────────────────────
    try:
        enriched = ai_processor.enrich_batch(raw_products, existing_dates)
        stats["enriched"] = len(enriched)
    except Exception as e:
        log.error(f"AI enrichment failed for {site_key}: {e}")
        # Store raw without AI as fallback
        from ai.processor import EnrichedProduct
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        enriched = []
        for p in raw_products:
            enriched.append(EnrichedProduct(
                product_id=p.product_id,
                site=p.site,
                name=p.name,
                original_url=p.original_url,
                scrape_date=now,
                first_seen_date=existing_dates.get(p.product_id, now),
                description_short=p.description_short,
                description_ai_expanded=p.description_short or p.name,
                tags=[p.site, "אופנה"],
                colors_available=p.colors_available,
                sizes_available=p.sizes_available,
                price=p.price,
                original_price=p.original_price,
                discount_percentage=p.discount_percentage,
                is_on_sale=p.is_on_sale,
                images=p.images,
                category=p.category,
            ))

    if test_mode:
        # Print first product as preview
        if enriched:
            ep = enriched[0]
            console.print(f"\n[green]Sample product:[/green]" if HAS_RICH else "\nSample product:")
            sample = {
                "name": ep.name,
                "price": ep.price,
                "tags": ep.tags[:5],
                "description_preview": ep.description_ai_expanded[:150] + "...",
            }
            console.print(json.dumps(sample, ensure_ascii=False, indent=2))

    # ── 4. Save to Firestore ───────────────────────────────
    try:
        created, updated = db.upsert_products(enriched)
        stats["created"] = created
        stats["updated"] = updated
    except Exception as e:
        log.error(f"Firestore save failed for {site_key}: {e}")
        stats["errors"] += 1

    stats["duration_sec"] = round(time.time() - t_start, 1)
    log.info(
        f"[{site_key}] Done: scraped={stats['scraped']} enriched={stats['enriched']} "
        f"created={stats['created']} updated={stats['updated']} ({stats['duration_sec']}s)"
    )
    return stats


def print_summary(all_stats: list[dict]) -> None:
    total_scraped = sum(s["scraped"] for s in all_stats)
    total_created = sum(s["created"] for s in all_stats)
    total_updated = sum(s["updated"] for s in all_stats)
    total_errors = sum(s["errors"] for s in all_stats)
    total_time = sum(s["duration_sec"] for s in all_stats)

    if HAS_RICH:
        table = Table(title="Run Summary", border_style="green")
        table.add_column("Site", style="cyan")
        table.add_column("Scraped", justify="right")
        table.add_column("Created", justify="right", style="green")
        table.add_column("Updated", justify="right", style="yellow")
        table.add_column("Errors", justify="right", style="red")
        table.add_column("Time(s)", justify="right")
        for s in all_stats:
            table.add_row(
                s["site"],
                str(s["scraped"]),
                str(s["created"]),
                str(s["updated"]),
                str(s["errors"]),
                str(s["duration_sec"]),
            )
        table.add_section()
        table.add_row(
            "TOTAL",
            str(total_scraped),
            str(total_created),
            str(total_updated),
            str(total_errors),
            str(round(total_time, 1)),
            style="bold",
        )
        console.print(table)
    else:
        print(f"\n=== Run Summary ===")
        for s in all_stats:
            print(f"  {s['site']}: scraped={s['scraped']} created={s['created']} updated={s['updated']}")
        print(f"  TOTAL: {total_scraped} scraped, {total_created} new, {total_updated} updated")


def main():
    parser = argparse.ArgumentParser(description="Fashion Auto-Scraper")
    parser.add_argument("--mode", choices=["test", "full", "auto", "site"], default="full")
    parser.add_argument("--site", help="Site key (for --mode site)")
    parser.add_argument("--limit", type=int, help="Max products per site (overrides config)")
    args = parser.parse_args()

    if args.mode != "auto":
        banner()

    # ── Config validation ──────────────────────────────────
    if not validate_config():
        sys.exit(1)

    # ── Determine sites + limit ────────────────────────────
    if args.mode == "test":
        sites = [config.ACTIVE_SITES[0]] if config.ACTIVE_SITES else ["renoir"]
        limit = 3
        console.print("[yellow]🧪 TEST MODE: 3 products from 1 site[/yellow]\n" if HAS_RICH else "TEST MODE\n")
    elif args.mode == "site":
        if not args.site:
            console.print("[red]--site required for --mode site[/red]" if HAS_RICH else "--site required")
            sys.exit(1)
        sites = [args.site]
        limit = args.limit or config.MAX_PRODUCTS_PER_SITE
    else:
        sites = config.ACTIVE_SITES
        limit = args.limit or config.MAX_PRODUCTS_PER_SITE

    # ── Init AI + DB ───────────────────────────────────────
    try:
        ai_processor = GeminiProcessor()
        log.info("Gemini AI processor ready")
    except Exception as e:
        log.error(f"Failed to init Gemini: {e}")
        sys.exit(1)

    try:
        db = FirestoreManager()
        log.info("Firestore connection ready")
    except Exception as e:
        log.error(f"Failed to init Firestore: {e}")
        sys.exit(1)

    # ── Run scraping pipeline ──────────────────────────────
    run_start = datetime.now(timezone.utc)
    all_stats: list[dict] = []

    for site_key in sites:
        try:
            stats = run_scraper(
                site_key=site_key,
                limit=limit,
                ai_processor=ai_processor,
                db=db,
                test_mode=(args.mode == "test"),
            )
            all_stats.append(stats)
        except KeyboardInterrupt:
            log.warning("Interrupted by user")
            break
        except Exception as e:
            log.error(f"Unhandled error for site {site_key}: {e}")
            all_stats.append({"site": site_key, "scraped": 0, "created": 0, "updated": 0, "errors": 1, "duration_sec": 0, "enriched": 0})

    # ── Summary ────────────────────────────────────────────
    print_summary(all_stats)

    # ── Save run metadata to Firestore ─────────────────────
    run_meta = {
        "mode": args.mode,
        "sites": sites,
        "total_scraped": sum(s["scraped"] for s in all_stats),
        "total_created": sum(s["created"] for s in all_stats),
        "total_updated": sum(s["updated"] for s in all_stats),
        "total_errors": sum(s["errors"] for s in all_stats),
        "duration_sec": round((datetime.now(timezone.utc) - run_start).total_seconds(), 1),
        "per_site": all_stats,
    }
    try:
        db.save_run_metadata(run_meta)
    except Exception as e:
        log.warning(f"Could not save run metadata: {e}")

    console.print(
        f"\n[bold green]✅ Done! {run_meta['total_scraped']} products scraped, "
        f"{run_meta['total_created']} new, {run_meta['total_updated']} updated.[/bold green]\n"
        if HAS_RICH else
        f"\nDone! {run_meta['total_scraped']} scraped.\n"
    )

    # Exit code — non-zero if all sites failed
    if all(s["errors"] > 0 and s["scraped"] == 0 for s in all_stats):
        sys.exit(1)


if __name__ == "__main__":
    main()
