"""
DoStyle Sync CLI — operational commands for the Firestore ingestion engine.

Commands:
  run-all-sources                   — Sync all registered sources to Firestore
  run-source <source>               — Sync a single source to Firestore
  run-incremental <source>          — Incremental sync (no missing-product detection)
  refresh-source-stats [source]     — Recompute source stats from Firestore
  verify-stale-products [--days N]  — Find products not seen in N days
  verify-removed-products           — Mark products no longer on source as missing
  retry-failures [source]           — Retry failed parse URLs
  sync-loop                         — Run continuous sync loop
  source-status [source]            — Show live sync state
  dashboard-stats                   — Show monitoring dashboard stats

Usage:
  python -m engine.cli.sync_commands run-all-sources
  python -m engine.cli.sync_commands run-source renuar --limit 200
  python -m engine.cli.sync_commands sync-loop --interval 360
  python -m engine.cli.sync_commands source-status
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.text import Text
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False
    class _FallbackConsole:
        def print(self, *a, **kw): print(*[str(x) for x in a])
        def rule(self, *a, **kw): print("─" * 50)
    console = _FallbackConsole()


def _make_orchestrator(limit: int = 500, dry_run: bool = False):
    from engine.sync.orchestrator import SyncOrchestrator
    return SyncOrchestrator(product_limit=limit, dry_run=dry_run)


def _status_color(status: str) -> str:
    return {
        "ok": "green", "idle": "dim", "running": "cyan",
        "failed": "red", "error": "red", "degraded": "yellow",
        "done": "green",
    }.get(status, "white")


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_run_all_sources(args) -> None:
    orch = _make_orchestrator(limit=getattr(args, "limit", 500))
    results = orch.run_all_sources()

    if HAS_RICH:
        table = Table(title="All Sources Sync Results", border_style="cyan")
        table.add_column("Source", style="bold")
        table.add_column("Status", justify="center")
        table.add_column("Scraped", justify="right")
        table.add_column("Created", justify="right")
        table.add_column("Updated", justify="right")
        table.add_column("Missing", justify="right")
        table.add_column("Price Δ", justify="right")
        table.add_column("Duration", justify="right")
        table.add_column("Error")

        for r in results:
            status = "[green]OK[/green]" if r.success else "[red]FAIL[/red]"
            table.add_row(
                r.source_key,
                status,
                str(r.products_scraped),
                str(r.products_created),
                str(r.products_updated),
                str(r.products_missing),
                str(r.price_changes),
                f"{r.duration_sec}s" if r.duration_sec else "-",
                (r.error or "")[:60],
            )
        console.print(table)
    else:
        for r in results:
            status = "OK" if r.success else "FAIL"
            print(f"{r.source_key:20} {status:6} scraped={r.products_scraped} "
                  f"new={r.products_created} updated={r.products_updated}")

    sys.exit(0 if all(r.success for r in results) else 1)


def cmd_run_source(args) -> None:
    orch = _make_orchestrator(limit=getattr(args, "limit", 500))
    result = orch.run_source(args.source)

    if HAS_RICH:
        color = "green" if result.success else "red"
        status = "SUCCESS" if result.success else "FAILED"
        console.print(Panel.fit(
            f"[bold]Source:[/bold] {result.source_key}\n"
            f"[bold]Status:[/bold] [{color}]{status}[/{color}]\n"
            f"[bold]Run ID:[/bold] {result.run_id}\n"
            f"[bold]Discovered:[/bold] {result.products_discovered}\n"
            f"[bold]Scraped:[/bold] {result.products_scraped}\n"
            f"[bold]Failed:[/bold] {result.products_failed}\n"
            f"[bold]Created:[/bold] {result.products_created}\n"
            f"[bold]Updated:[/bold] {result.products_updated}\n"
            f"[bold]Missing:[/bold] {result.products_missing}\n"
            f"[bold]Price changes:[/bold] {result.price_changes}\n"
            f"[bold]Stock changes:[/bold] {result.stock_changes}\n"
            f"[bold]Sale changes:[/bold] {result.sale_changes}\n"
            f"[bold]Duration:[/bold] {result.duration_sec}s\n"
            + (f"[red]Error:[/red] {result.error}\n" if result.error else ""),
            title=f"Sync — {args.source}",
            border_style=color,
        ))
    else:
        print(f"Source: {result.source_key}")
        print(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
        print(f"Scraped: {result.products_scraped}")
        print(f"Created: {result.products_created}")
        print(f"Updated: {result.products_updated}")
        if result.error:
            print(f"Error: {result.error}")

    sys.exit(0 if result.success else 1)


def cmd_run_incremental(args) -> None:
    orch = _make_orchestrator(limit=getattr(args, "limit", 500))
    result = orch.run_incremental(args.source)
    console.print(f"[green]Incremental sync done[/green] — scraped={result.products_scraped} "
                  f"updated={result.products_updated}" if HAS_RICH
                  else f"Incremental sync: scraped={result.products_scraped}")
    sys.exit(0 if result.success else 1)


def cmd_refresh_source_stats(args) -> None:
    orch = _make_orchestrator()
    source = getattr(args, "source", None)
    results = orch.refresh_source_stats(source)

    if not results:
        console.print("[yellow]No stats refreshed (Firestore not available?)[/yellow]" if HAS_RICH
                      else "No stats refreshed")
        return

    if HAS_RICH:
        table = Table(title="Source Stats", border_style="blue")
        table.add_column("Source", style="bold")
        table.add_column("Live Products", justify="right")
        table.add_column("Sale", justify="right")
        table.add_column("New Collection", justify="right")
        table.add_column("Out of Stock", justify="right")
        table.add_column("Missing", justify="right")
        table.add_column("Avg Quality", justify="right")

        for key, stats in results.items():
            table.add_row(
                key,
                str(stats.get("total_live_products", "-")),
                str(stats.get("total_sale_products", "-")),
                str(stats.get("total_new_collection_products", "-")),
                str(stats.get("total_out_of_stock_products", "-")),
                str(stats.get("total_missing_products", "-")),
                f"{stats.get('avg_quality_score', 0):.2f}",
            )
        console.print(table)
    else:
        for key, stats in results.items():
            print(f"{key}: live={stats.get('total_live_products', 0)} "
                  f"sale={stats.get('total_sale_products', 0)}")


def cmd_verify_stale_products(args) -> None:
    orch = _make_orchestrator()
    days = getattr(args, "days", 7)
    stale = orch.verify_stale_products(days=days)

    if not stale:
        console.print("[green]No stale product data[/green]" if HAS_RICH else "No stale data")
        return

    total = sum(stale.values())
    if HAS_RICH:
        console.print(f"[yellow]Stale products (>{days}d):[/yellow] {total} total")
        for src, count in sorted(stale.items()):
            color = "red" if count > 50 else "yellow"
            console.print(f"  [{color}]{src}:[/{color}] {count}")
    else:
        print(f"Stale products (>{days}d): {total} total")
        for src, count in sorted(stale.items()):
            print(f"  {src}: {count}")


def cmd_verify_removed_products(args) -> None:
    """Re-run all sources to detect removed products (runs full sync)."""
    console.print("Running full sync to detect removed products..." if not HAS_RICH
                  else "[cyan]Running full sync to detect removed products...[/cyan]")
    orch = _make_orchestrator(limit=getattr(args, "limit", 500))
    results = orch.run_all_sources()
    total_missing = sum(r.products_missing for r in results)
    console.print(f"Detected {total_missing} missing/removed products" if not HAS_RICH
                  else f"[yellow]Detected {total_missing} missing/removed products[/yellow]")


def cmd_retry_failures(args) -> None:
    source = getattr(args, "source", None)
    orch = _make_orchestrator()
    results = orch.retry_failures(source_key=source)
    if not results:
        console.print("No failures to retry" if not HAS_RICH else "[green]No failures to retry[/green]")
    else:
        for src, res in results.items():
            print(f"{src}: success={res['success']} scraped={res['scraped']}")


def cmd_sync_loop(args) -> None:
    from engine.sync.scheduler import Scheduler
    interval = getattr(args, "interval", 360)
    limit = getattr(args, "limit", 500)
    sources_arg = getattr(args, "sources", None)
    sources = sources_arg.split(",") if sources_arg else None

    console.print(
        f"Starting sync loop: interval={interval}m, limit={limit}, sources={sources or 'all'}"
        if not HAS_RICH else
        f"[cyan]Starting sync loop[/cyan] — interval=[bold]{interval}m[/bold], "
        f"sources=[bold]{', '.join(sources) if sources else 'all'}[/bold]"
    )

    sched = Scheduler(interval_minutes=interval, product_limit=limit, sources=sources)
    sched.run_loop()


def cmd_source_status(args) -> None:
    """Show live sync state for all or one source."""
    try:
        from engine.firestore.manager import FirestoreIngestionManager
        db = FirestoreIngestionManager()
        if not db.available:
            console.print("[red]Firestore not available[/red]" if HAS_RICH else "Firestore not available")
            return
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]" if HAS_RICH else f"Error: {e}")
        return

    source = getattr(args, "source", None)
    if source:
        states = {source: db.get_sync_state(source)}
    else:
        states = db.get_all_sync_states()

    if not states:
        console.print("No sync state data in Firestore" if not HAS_RICH
                      else "[dim]No sync state data in Firestore[/dim]")
        return

    if HAS_RICH:
        table = Table(title="Source Sync State", border_style="cyan")
        table.add_column("Source", style="bold")
        table.add_column("Job Status", justify="center")
        table.add_column("Stage")
        table.add_column("Message")
        table.add_column("Last Started")
        table.add_column("Last Finished")
        table.add_column("Error")

        for key, state in sorted(states.items()):
            job_status = state.get("current_job_status", "unknown")
            color = _status_color(job_status)
            table.add_row(
                key,
                f"[{color}]{job_status}[/{color}]",
                state.get("current_stage", "-"),
                (state.get("current_message") or "")[:50],
                (state.get("last_started_at") or "")[:19],
                (state.get("last_finished_at") or "")[:19],
                (state.get("current_error") or "")[:40],
            )
        console.print(table)
    else:
        for key, state in sorted(states.items()):
            print(f"{key:20} {state.get('current_job_status','?'):10} {state.get('current_stage','?')}")


def cmd_dashboard_stats(args) -> None:
    """Print aggregate dashboard stats."""
    try:
        from engine.firestore.manager import FirestoreIngestionManager
        db = FirestoreIngestionManager()
        if not db.available:
            console.print("[red]Firestore not available[/red]" if HAS_RICH else "Firestore not available")
            return
        stats = db.get_dashboard_stats()
    except Exception as e:
        console.print(f"[red]{e}[/red]" if HAS_RICH else str(e))
        return

    if HAS_RICH:
        console.print(Panel(
            f"Total products: [bold]{stats.get('total_products', 0)}[/bold]\n"
            f"Sale products: [yellow]{stats.get('sale_products', 0)}[/yellow]\n"
            f"New collection: [green]{stats.get('new_collection_products', 0)}[/green]\n"
            f"Out of stock: [red]{stats.get('out_of_stock_products', 0)}[/red]\n"
            f"Missing: [dim]{stats.get('missing_products', 0)}[/dim]\n"
            f"Updated today: [cyan]{stats.get('products_updated_today', 0)}[/cyan]\n"
            f"Added today: [green]{stats.get('products_added_today', 0)}[/green]",
            title="Dashboard Stats",
        ))
    else:
        for k, v in stats.items():
            if k != "by_source":
                print(f"{k}: {v}")


# ── Parser ────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dostyle-sync",
        description="DoStyle Sync Engine — Firestore ingestion operations",
    )
    sub = parser.add_subparsers(dest="command")

    # run-all-sources
    p = sub.add_parser("run-all-sources", help="Sync all sources to Firestore")
    p.add_argument("--limit", type=int, default=500, help="Products per source limit")

    # run-source
    p = sub.add_parser("run-source", help="Sync a single source to Firestore")
    p.add_argument("source", help="Source key (e.g. renuar, zara)")
    p.add_argument("--limit", type=int, default=500)

    # run-incremental
    p = sub.add_parser("run-incremental", help="Incremental sync (no missing detection)")
    p.add_argument("source")
    p.add_argument("--limit", type=int, default=500)

    # refresh-source-stats
    p = sub.add_parser("refresh-source-stats", help="Recompute source stats")
    p.add_argument("source", nargs="?", help="Source key (omit for all)")

    # verify-stale-products
    p = sub.add_parser("verify-stale-products", help="Find products not seen recently")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("source", nargs="?")

    # verify-removed-products
    p = sub.add_parser("verify-removed-products", help="Run full sync to detect removed products")
    p.add_argument("--limit", type=int, default=500)

    # retry-failures
    p = sub.add_parser("retry-failures", help="Retry failed parse URLs")
    p.add_argument("source", nargs="?")

    # sync-loop
    p = sub.add_parser("sync-loop", help="Run continuous sync loop")
    p.add_argument("--interval", type=int, default=360, help="Minutes between rounds")
    p.add_argument("--limit", type=int, default=500)
    p.add_argument("--sources", help="Comma-separated source keys (default: all)")

    # source-status
    p = sub.add_parser("source-status", help="Show live sync state")
    p.add_argument("source", nargs="?")

    # dashboard-stats
    sub.add_parser("dashboard-stats", help="Show aggregate monitoring stats")

    return parser


def cli(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    dispatch = {
        "run-all-sources": cmd_run_all_sources,
        "run-source": cmd_run_source,
        "run-incremental": cmd_run_incremental,
        "refresh-source-stats": cmd_refresh_source_stats,
        "verify-stale-products": cmd_verify_stale_products,
        "verify-removed-products": cmd_verify_removed_products,
        "retry-failures": cmd_retry_failures,
        "sync-loop": cmd_sync_loop,
        "source-status": cmd_source_status,
        "dashboard-stats": cmd_dashboard_stats,
    }

    if not args.command:
        parser.print_help()
        sys.exit(0)

    handler = dispatch.get(args.command)
    if not handler:
        parser.print_help()
        sys.exit(1)

    handler(args)


if __name__ == "__main__":
    cli()
