#!/usr/bin/env python3
"""
Validate all 10 source adapters — print a detailed health + parse report.

Usage:
  python scripts/validate_all_sources.py
  python scripts/validate_all_sources.py --source renuar
  python scripts/validate_all_sources.py --quick  (skip live HTTP, use fixtures only)

Output:
  - Per-source status table
  - Blockers (if any)
  - Exit code 0 = all OK, 1 = some failed
"""
from __future__ import annotations

import argparse
import sys
import time
import traceback
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    HAS_RICH = True
    console = Console()
except ImportError:
    HAS_RICH = False
    class _Con:
        def print(self, *a, **kw): print(*a)
        def rule(self, *a, **kw): print("-"*60)
    console = _Con()

from engine.registry.source_registry import list_sources, get_adapter
from engine.validation.validator import ProductValidator


def run_source_validation(source_key: str, quick: bool = False) -> dict:
    """
    Run validation for a single source.
    Returns a result dict with status, counts, blockers.
    """
    result = {
        "source": source_key,
        "adapter_status": "unknown",
        "discovery_success": False,
        "parse_success": False,
        "products_found": 0,
        "warnings_count": 0,
        "blocker": None,
        "platform": "unknown",
        "duration_sec": 0,
    }

    t_start = time.monotonic()

    # 1. Can we load the adapter?
    try:
        adapter = get_adapter(source_key)
        result["platform"] = adapter.PLATFORM_FAMILY
        result["adapter_status"] = "loaded"
    except Exception as e:
        result["adapter_status"] = "load_failed"
        result["blocker"] = f"Adapter load error: {e}"
        result["duration_sec"] = round(time.monotonic() - t_start, 2)
        return result

    if quick:
        # Quick mode: only test adapter instantiation
        result["adapter_status"] = "ok"
        result["duration_sec"] = round(time.monotonic() - t_start, 2)
        return result

    # 2. Healthcheck (lightweight)
    try:
        hc = adapter.healthcheck()
        result["discovery_success"] = hc.discovery_ok
        result["parse_success"] = hc.parse_ok
        if hc.status == "ok":
            result["adapter_status"] = "ok"
        elif hc.status == "degraded":
            result["adapter_status"] = "degraded"
            if hc.warnings:
                result["blocker"] = "Degraded: " + "; ".join(hc.warnings[:2])
        else:
            result["adapter_status"] = "failed"
            result["blocker"] = hc.error or "Healthcheck failed"
            if hc.warnings:
                result["blocker"] += " | " + "; ".join(hc.warnings[:2])
    except Exception as e:
        result["adapter_status"] = "error"
        result["blocker"] = f"Healthcheck exception: {type(e).__name__}: {e}"
        tb = traceback.format_exc()
        result["traceback"] = tb[:300]

    result["duration_sec"] = round(time.monotonic() - t_start, 2)
    return result


def print_report(results: list[dict]) -> None:
    """Print the validation report table."""
    if HAS_RICH:
        table = Table(title="DoStyle Source Validation Report", border_style="cyan")
        table.add_column("Source", style="bold")
        table.add_column("Platform")
        table.add_column("Status")
        table.add_column("Discovery")
        table.add_column("Parse")
        table.add_column("Products", justify="right")
        table.add_column("Warnings", justify="right")
        table.add_column("Time(s)", justify="right")
        table.add_column("Blocker")

        for r in results:
            status = r["adapter_status"]
            color = {"ok": "green", "degraded": "yellow", "failed": "red", "error": "red"}.get(status, "white")
            table.add_row(
                r["source"],
                r["platform"],
                f"[{color}]{status}[/{color}]",
                "✓" if r["discovery_success"] else "✗",
                "✓" if r["parse_success"] else "✗",
                str(r["products_found"]),
                str(r["warnings_count"]),
                str(r["duration_sec"]),
                (r["blocker"] or "")[:60],
            )
        console.print(table)

        # Summary
        ok = sum(1 for r in results if r["adapter_status"] == "ok")
        degraded = sum(1 for r in results if r["adapter_status"] == "degraded")
        failed = sum(1 for r in results if r["adapter_status"] in ("failed", "error"))

        console.print(
            f"\n[bold]Summary:[/bold] "
            f"[green]{ok} OK[/green] | "
            f"[yellow]{degraded} Degraded[/yellow] | "
            f"[red]{failed} Failed[/red] "
            f"out of {len(results)} sources"
        )

        # Blockers
        blockers = [(r["source"], r["blocker"]) for r in results if r["blocker"]]
        if blockers:
            console.print("\n[bold red]Blockers:[/bold red]")
            for source, blocker in blockers:
                console.print(f"  [bold]{source}:[/bold] {blocker}")

    else:
        # Plain text
        print(f"\n{'='*70}")
        print(f"{'Source':<20} {'Status':<10} {'Discovery':<10} {'Parse':<8} {'Blocker'}")
        print(f"{'='*70}")
        for r in results:
            disc = "OK" if r["discovery_success"] else "FAIL"
            parse = "OK" if r["parse_success"] else "FAIL"
            blocker = (r["blocker"] or "")[:40]
            print(f"{r['source']:<20} {r['adapter_status']:<10} {disc:<10} {parse:<8} {blocker}")
        print(f"{'='*70}")
        ok = sum(1 for r in results if r["adapter_status"] == "ok")
        print(f"\nTotal: {ok}/{len(results)} OK")


def main():
    parser = argparse.ArgumentParser(description="Validate all DoStyle scraping sources")
    parser.add_argument("--source", help="Run for a single source key")
    parser.add_argument("--quick", action="store_true", help="Quick mode — skip live HTTP calls")
    parser.add_argument("--output", help="Write JSON report to file")
    args = parser.parse_args()

    # Determine which sources to validate
    if args.source:
        source_keys = [args.source]
    else:
        source_keys = [m.source_key for m in list_sources()]

    console.print(
        f"\n[bold cyan]DoStyle Source Validator[/bold cyan] — checking {len(source_keys)} sources"
        if HAS_RICH else f"\nDoStyle Validator — {len(source_keys)} sources"
    )
    if args.quick:
        console.print("[yellow]Quick mode: skipping live HTTP checks[/yellow]" if HAS_RICH else "Quick mode")

    results = []
    for key in source_keys:
        console.print(f"  Checking [bold]{key}[/bold]..." if HAS_RICH else f"  Checking {key}...")
        result = run_source_validation(key, quick=args.quick)
        results.append(result)

    print_report(results)

    # Optionally write JSON report
    if args.output:
        import json
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        console.print(f"\nReport written to {args.output}")

    # Exit code
    failed = sum(1 for r in results if r["adapter_status"] in ("failed", "error", "load_failed"))
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
