"""
DoStyle Scraping Engine CLI.

Commands:
  scrape-source <source>           — Full scrape of one source
  scrape-product <source> <url>    — Scrape a single product URL
  discover-products <source>       — List product URLs for a source
  healthcheck <source>             — Run a health check
  validate-source <source>         — Validate scraped output
  export-json <source>             — Export scraped data to JSON
  export-firebase <source>         — Push scraped data to Firestore
  list-sources                     — List all registered sources

Usage:
  python -m engine.cli.commands scrape-source renuar
  python -m engine.cli.commands healthcheck zara
  python -m engine.cli.commands list-sources
"""
from __future__ import annotations

import json
import sys
import time
import logging
from pathlib import Path
from typing import Optional

# Use rich for pretty output if available
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False
    class _FallbackConsole:
        def print(self, *a, **kw): print(*[str(x) for x in a])
        def rule(self, *a, **kw): print("─" * 50)
    console = _FallbackConsole()

import argparse

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("engine.cli")


def _get_adapter(source_key: str):
    from engine.registry.source_registry import get_adapter
    try:
        return get_adapter(source_key)
    except KeyError as e:
        console.print(f"[red]Error: {e}[/red]" if HAS_RICH else f"Error: {e}")
        sys.exit(1)


def cmd_list_sources(args) -> None:
    """List all registered sources."""
    from engine.registry.source_registry import list_sources
    metas = list_sources()

    if HAS_RICH:
        table = Table(title="Registered Sources", border_style="cyan")
        table.add_column("Key", style="bold")
        table.add_column("Name")
        table.add_column("Platform")
        table.add_column("Priority", justify="center")
        table.add_column("Has API", justify="center")
        table.add_column("JS Heavy", justify="center")
        table.add_column("Active", justify="center")
        for m in metas:
            table.add_row(
                m.source_key,
                m.source_name,
                m.platform_family,
                str(m.priority),
                "✓" if m.has_api else "✗",
                "✓" if m.js_heavy else "✗",
                "[green]✓[/green]" if m.active else "[red]✗[/red]",
            )
        console.print(table)
    else:
        for m in metas:
            print(f"{m.source_key:20} {m.source_name:20} {m.platform_family:15} priority={m.priority}")


def cmd_healthcheck(args) -> None:
    """Run healthcheck for a source."""
    adapter = _get_adapter(args.source)
    console.print(f"Running healthcheck for [bold]{args.source}[/bold]..." if HAS_RICH else f"Healthcheck: {args.source}")

    t = time.monotonic()
    result = adapter.healthcheck()
    elapsed = round(time.monotonic() - t, 2)

    status_color = {"ok": "green", "degraded": "yellow", "failed": "red"}.get(result.status, "white")

    if HAS_RICH:
        color = status_color
        console.print(Panel.fit(
            f"[bold]Source:[/bold] {result.source_key}\n"
            f"[bold]Status:[/bold] [{color}]{result.status.upper()}[/{color}]\n"
            f"[bold]Reachable:[/bold] {'✓' if result.reachable else '✗'}\n"
            f"[bold]Discovery OK:[/bold] {'✓' if result.discovery_ok else '✗'}\n"
            f"[bold]Parse OK:[/bold] {'✓' if result.parse_ok else '✗'}\n"
            f"[bold]Response time:[/bold] {result.response_time_ms}ms\n"
            + (f"[bold]Sample URL:[/bold] {result.sample_product_url}\n" if result.sample_product_url else "")
            + (f"[bold]Sample name:[/bold] {result.sample_product_name}\n" if result.sample_product_name else "")
            + (f"[bold red]Error:[/bold red] {result.error}\n" if result.error else "")
            + (f"[yellow]Warnings:[/yellow] {', '.join(result.warnings)}\n" if result.warnings else "")
            + f"Elapsed: {elapsed}s",
            title=f"Health Check — {args.source}",
        ))
    else:
        print(f"Source: {result.source_key}")
        print(f"Status: {result.status.upper()}")
        print(f"Reachable: {result.reachable}")
        print(f"Discovery OK: {result.discovery_ok}")
        print(f"Parse OK: {result.parse_ok}")
        if result.error:
            print(f"Error: {result.error}")

    sys.exit(0 if result.status == "ok" else 1)


def cmd_discover_products(args) -> None:
    """Discover product URLs for a source."""
    adapter = _get_adapter(args.source)
    limit = getattr(args, "limit", 100)

    console.print(f"Discovering products for [bold]{args.source}[/bold] (limit={limit})..." if HAS_RICH else f"Discovering: {args.source}")

    urls = adapter.discover_product_urls(limit=limit)

    console.print(f"Found [bold]{len(urls)}[/bold] product URLs:" if HAS_RICH else f"Found {len(urls)} URLs:")
    for url in urls[:20]:
        console.print(f"  {url}")
    if len(urls) > 20:
        console.print(f"  ... and {len(urls) - 20} more")

    # Optionally write to file
    if getattr(args, "output", None):
        out_path = Path(args.output)
        with open(out_path, "w") as f:
            for url in urls:
                f.write(url + "\n")
        console.print(f"Written to {out_path}")


def cmd_scrape_product(args) -> None:
    """Scrape a single product URL."""
    adapter = _get_adapter(args.source)
    url = args.url

    console.print(f"Scraping [bold]{url}[/bold]..." if HAS_RICH else f"Scraping: {url}")

    raw = adapter.fetch_product_page(url)
    result = adapter.parse_product(raw)

    if result.success and result.product:
        p = result.product
        output = p.to_json_dict()
        if HAS_RICH:
            console.print(Panel.fit(
                f"[green]SUCCESS[/green] — {result.extraction_method} (confidence={result.confidence:.2f})\n"
                f"Name: {p.product_name}\n"
                f"Price: {p.current_price} {p.currency}\n"
                f"Images: {len(p.image_urls)}\n"
                f"Sizes: {', '.join(p.sizes_available[:5])}\n"
                f"Colors: {', '.join(p.colors_available[:5])}\n"
                f"Stock: {p.stock_status}",
                title="Product Scraped",
            ))
        else:
            print(f"SUCCESS: {p.product_name} | {p.current_price} {p.currency}")

        if getattr(args, "json", False):
            print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    else:
        console.print(f"[red]FAILED:[/red] {result.errors}" if HAS_RICH else f"FAILED: {result.errors}")
        sys.exit(1)


def cmd_scrape_source(args) -> None:
    """Full scrape of one source."""
    from engine.output.json_writer import JSONWriter
    from engine.validation.validator import ProductValidator

    adapter = _get_adapter(args.source)
    limit = getattr(args, "limit", 200)

    console.print(f"Scraping [bold]{args.source}[/bold] (limit={limit})..." if HAS_RICH else f"Scraping: {args.source}")

    products, stats = adapter.scrape_all(limit=limit)

    # Validate
    validator = ProductValidator()
    report = validator.validate_all(args.source, products)

    # Output
    writer = JSONWriter()
    if products:
        products_file = writer.write_products(args.source, products)
        stats_file = writer.write_stats(args.source, stats, products)
        console.print(f"[green]Output:[/green] {products_file}" if HAS_RICH else f"Output: {products_file}")

    # Summary
    if HAS_RICH:
        table = Table(title=f"Scrape Results — {args.source}", border_style="green")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        rows = [
            ("Products parsed", str(stats.products_parsed)),
            ("Products failed", str(stats.products_failed)),
            ("Products skipped", str(stats.products_skipped)),
            ("Sale products", str(stats.sale_products_count)),
            ("Out of stock", str(stats.out_of_stock_count)),
            ("New collection", str(stats.new_collection_products_count)),
            ("Validation passed", str(report.passed)),
            ("Validation failed", str(report.failed)),
            ("Warnings", str(report.warning_count)),
            ("Duration", f"{stats.duration_sec}s"),
            ("Health", stats.parser_health_status),
        ]
        for row in rows:
            table.add_row(*row)
        console.print(table)
    else:
        print(f"\nResults for {args.source}:")
        print(f"  Parsed:  {stats.products_parsed}")
        print(f"  Failed:  {stats.products_failed}")
        print(f"  Valid:   {report.passed}/{report.total}")
        print(f"  Health:  {stats.parser_health_status}")

    sys.exit(0 if stats.parser_health_status != "failed" else 1)


def cmd_validate_source(args) -> None:
    """Validate already-scraped JSON output for a source."""
    from engine.validation.validator import ProductValidator
    from engine.schemas.product import NormalizedProduct

    output_file = Path(f"data/output/{args.source}_products.json")
    if not output_file.exists():
        console.print(f"[red]No output file found:[/red] {output_file}" if HAS_RICH else f"File not found: {output_file}")
        sys.exit(1)

    with open(output_file, "r", encoding="utf-8") as f:
        records = json.load(f)

    products = []
    for r in records:
        try:
            products.append(NormalizedProduct(**r))
        except Exception as e:
            log.warning(f"Could not load product record: {e}")

    validator = ProductValidator()
    report = validator.validate_all(args.source, products)
    report.print_summary()

    for res in report.product_results:
        if not res.valid:
            print(f"  FAIL {res.product_url}")
            for issue in res.errors:
                print(f"       [{issue.severity}] {issue.field}: {issue.issue}")


def cmd_export_json(args) -> None:
    """Re-export scraped data to JSON (if already scraped)."""
    cmd_scrape_source(args)


def cmd_export_firebase(args) -> None:
    """Scrape and push to Firestore."""
    from engine.output.firebase_output import FirebaseOutputWriter

    adapter = _get_adapter(args.source)
    limit = getattr(args, "limit", 200)

    products, stats = adapter.scrape_all(limit=limit)

    if not products:
        console.print(f"[yellow]No products scraped for {args.source}[/yellow]" if HAS_RICH else "No products")
        sys.exit(1)

    # JSON export
    from engine.output.json_writer import JSONWriter
    writer = JSONWriter()
    writer.write_products(args.source, products)

    # Firebase export
    fb = FirebaseOutputWriter()
    try:
        created, updated = fb.upsert_products(products, source_key=args.source)
        console.print(f"[green]Firebase:[/green] {created} created, {updated} updated" if HAS_RICH else f"Firebase: {created} created, {updated} updated")
    except Exception as e:
        console.print(f"[red]Firebase export failed:[/red] {e}" if HAS_RICH else f"Firebase failed: {e}")
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dostyle-scraper",
        description="DoStyle Scraping Engine CLI",
    )
    subparsers = parser.add_subparsers(dest="command")

    # list-sources
    subparsers.add_parser("list-sources", help="List all registered sources")

    # healthcheck
    p = subparsers.add_parser("healthcheck", help="Run healthcheck for a source")
    p.add_argument("source", help="Source key (e.g. renuar, zara)")

    # discover-products
    p = subparsers.add_parser("discover-products", help="Discover product URLs")
    p.add_argument("source")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--output", help="Write URLs to file")

    # scrape-product
    p = subparsers.add_parser("scrape-product", help="Scrape a single product URL")
    p.add_argument("source")
    p.add_argument("url")
    p.add_argument("--json", action="store_true", help="Print JSON output")

    # scrape-source
    p = subparsers.add_parser("scrape-source", help="Full scrape of one source")
    p.add_argument("source")
    p.add_argument("--limit", type=int, default=200)

    # validate-source
    p = subparsers.add_parser("validate-source", help="Validate scraped output")
    p.add_argument("source")

    # export-json
    p = subparsers.add_parser("export-json", help="Scrape and export to JSON")
    p.add_argument("source")
    p.add_argument("--limit", type=int, default=200)

    # export-firebase
    p = subparsers.add_parser("export-firebase", help="Scrape and push to Firestore")
    p.add_argument("source")
    p.add_argument("--limit", type=int, default=200)

    return parser


def cli(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    dispatch = {
        "list-sources": cmd_list_sources,
        "healthcheck": cmd_healthcheck,
        "discover-products": cmd_discover_products,
        "scrape-product": cmd_scrape_product,
        "scrape-source": cmd_scrape_source,
        "validate-source": cmd_validate_source,
        "export-json": cmd_export_json,
        "export-firebase": cmd_export_firebase,
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
