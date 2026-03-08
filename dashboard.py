#!/usr/bin/env python3
"""
Terminal dashboard — quick status check.
Run anytime:  python dashboard.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.columns import Columns
    from rich.text import Text
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False
    console = type("C", (), {"print": staticmethod(print), "rule": staticmethod(lambda *a, **k: print("─"*50))})()

import config
from utils.logger import get_logger

log = get_logger("dashboard")


def main():
    # Validate config first
    errors = config.validate()
    if errors:
        console.print("[red]Config errors — run setup first:[/red]" if HAS_RICH else "Config errors:")
        for e in errors:
            console.print(f"  • {e}")
        sys.exit(1)

    from db.firestore import FirestoreManager
    try:
        db = FirestoreManager()
    except Exception as e:
        console.print(f"[red]Firestore connection failed: {e}[/red]" if HAS_RICH else f"Error: {e}")
        sys.exit(1)

    # Product counts
    counts = db.get_product_count()
    total = sum(counts.values())

    # Recent runs
    runs = db.get_recent_runs(10)

    if HAS_RICH:
        console.rule("[cyan]🛍️  Fashion Scraper Dashboard[/cyan]")
        console.print()

        # Products table
        pt = Table(title=f"Products in Firestore (total: {total})", border_style="cyan")
        pt.add_column("Site", style="bold")
        pt.add_column("Count", justify="right", style="green")
        for site, count in sorted(counts.items(), key=lambda x: -x[1]):
            pt.add_row(site.title(), str(count))
        console.print(pt)
        console.print()

        # Recent runs table
        if runs:
            rt = Table(title="Recent Scrape Runs", border_style="yellow")
            rt.add_column("Date", style="dim")
            rt.add_column("Mode")
            rt.add_column("Scraped", justify="right")
            rt.add_column("New", justify="right", style="green")
            rt.add_column("Updated", justify="right", style="yellow")
            rt.add_column("Errors", justify="right", style="red")
            rt.add_column("Duration", justify="right")
            for r in runs:
                date_str = r.get("date", "")[:16].replace("T", " ")
                rt.add_row(
                    date_str,
                    r.get("mode", "-"),
                    str(r.get("total_scraped", 0)),
                    str(r.get("total_created", 0)),
                    str(r.get("total_updated", 0)),
                    str(r.get("total_errors", 0)),
                    f"{r.get('duration_sec', 0):.0f}s",
                )
            console.print(rt)
        else:
            console.print("[yellow]No scrape runs recorded yet.[/yellow]")

        # Cron status
        import subprocess
        cron_output = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        has_cron = "fashion-scraper" in cron_output.stdout or "main.py" in cron_output.stdout
        cron_status = "[green]✅ Active[/green]" if has_cron else "[red]❌ Not set[/red]"
        console.print(f"\nCron schedule: {cron_status}")

        # Active sites
        console.print(f"Active sites: [cyan]{', '.join(config.ACTIVE_SITES)}[/cyan]")
        console.print()

    else:
        print(f"\n=== Dashboard (total: {total} products) ===")
        for site, count in counts.items():
            print(f"  {site}: {count}")
        if runs:
            last = runs[0]
            print(f"\nLast run: {last.get('date', '')[:16]} — scraped {last.get('total_scraped', 0)}")


if __name__ == "__main__":
    main()
