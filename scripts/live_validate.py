#!/usr/bin/env python3
"""
Operational validation for all 10 DoStyle sources.

Mode A — LIVE HTTP (requires internet):
  python scripts/live_validate.py

Mode B — FIXTURE (offline, full parse/validate/export proof):
  python scripts/live_validate.py --fixture
  python scripts/live_validate.py --fixture --source renuar

Each source goes through:
  1.  Adapter load        → fail fast if broken
  2.  Discovery check     → category URL list + sitemap/API capability
  3.  Product parse       → full fixture or live URL → RawPayload → NormalizedProduct
  4.  Schema validation   → all required fields present
  5.  JSON export         → data/output/<source>_products.json
  6.  Status: PASS / PARTIAL / FAIL with exact reasons

Exit code: 0 = all PASS, 1 = any FAIL.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from rich.console import Console
    from rich.table import Table
    from rich import box
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False
    class _Con:
        def print(self, *a, **kw): print(*[str(x) for x in a])
        def rule(self, *a, **kw): print("─" * 70)
    console = _Con()

from engine.registry.source_registry import list_sources, get_adapter
from engine.validation.validator import ProductValidator
from engine.output.json_writer import JSONWriter
from engine.schemas.product import RawProductPayload, NormalizedProduct
from engine.extraction.json_ld import extract_json_ld
from engine.extraction.script_payload import extract_script_payload

FIXTURES_DIR = Path(__file__).parent.parent / "tests" / "fixtures"
OUTPUT_DIR   = Path("data/output")
validator    = ProductValidator()
writer       = JSONWriter(output_dir=OUTPUT_DIR)

# ──────────────────────────────────────────────────────────────────────────────
# Fixture definitions — realistic inputs per source
# ──────────────────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))

def _load_html(name: str) -> str:
    return (FIXTURES_DIR / "html" / name).read_text(encoding="utf-8")


def _make_shopify_fixture(source_key: str, base_url: str) -> RawProductPayload:
    """Generic Shopify fixture — uses renuar_product.json + brand override."""
    data = _load_json(FIXTURES_DIR / "renuar_product.json")
    product = data["product"]
    product["vendor"] = source_key.replace("_", " ").title()
    return RawProductPayload(
        source_site=source_key,
        product_url=f"{base_url}/products/linen-shirt",
        script_payload={"shopify_product": product},
        extraction_method="api",
    )


FIXTURE_BUILDERS: dict[str, callable] = {
    "renuar": lambda a: RawProductPayload(
        source_site="renuar",
        product_url="https://www.renuar.co.il/he/products/ofek-linen-shirt",
        script_payload={"shopify_product": _load_json(FIXTURES_DIR / "renuar_product.json")["product"]},
        extraction_method="api",
    ),
    "zara": lambda a: RawProductPayload(
        source_site="zara",
        product_url="https://www.zara.com/il/he/ribbed-mini-dress-p310073139.html",
        script_payload={"zara_product": _load_json(FIXTURES_DIR / "zara_product_api.json")},
        extraction_method="api",
    ),
    "castro": lambda a: (lambda html: RawProductPayload(
        source_site="castro",
        product_url="https://www.castro.com/he/product/jeans-mom-skinny",
        html_snapshot=html,
        json_ld_data=extract_json_ld(html),
        script_payload=extract_script_payload(html),
        extraction_method="json_ld",
    ))(_load_html("castro_product.html")),
    "sde_bar":     lambda a: _make_shopify_fixture("sde_bar",     "https://www.sdebar.co.il"),
    "lidor_bar":   lambda a: _make_shopify_fixture("lidor_bar",   "https://www.lidorbar.co.il"),
    "hodula":      lambda a: _make_shopify_fixture("hodula",      "https://www.hodula.co.il"),
    "shoshi_tamam":lambda a: _make_shopify_fixture("shoshi_tamam","https://www.shoshitamam.co.il"),
    "cstyle": lambda a: (lambda html: RawProductPayload(
        source_site="cstyle",
        product_url="https://www.cstyle.co.il/product/crop-top-tie/",
        html_snapshot=html,
        json_ld_data=extract_json_ld(html),
        script_payload=extract_script_payload(html),
        extraction_method="json_ld",
    ))(_load_html("cstyle_product.html")),
    "terminal_x": lambda a: (lambda html: RawProductPayload(
        source_site="terminal_x",
        product_url="https://www.terminalx.com/new-balance-574-grey.html",
        html_snapshot=html,
        json_ld_data=extract_json_ld(html),
        script_payload=extract_script_payload(html),
        extraction_method="json_ld",
    ))(_load_html("terminal_x_product.html")),
    "adika": lambda a: (lambda html: RawProductPayload(
        source_site="adika",
        product_url="https://www.adika.co.il/product/ADIK-MIDI-ASYM-9423",
        html_snapshot=html,
        json_ld_data=extract_json_ld(html),
        script_payload=extract_script_payload(html),
        extraction_method="script",
    ))(_load_html("adika_product.html")),
}


# ──────────────────────────────────────────────────────────────────────────────
# Result container
# ──────────────────────────────────────────────────────────────────────────────

class SourceResult:
    def __init__(self, key: str):
        self.key             = key
        self.platform        = "?"
        self.status          = "FAIL"
        self.adapter_ok      = False
        self.categories      : list[str] = []
        self.parse_ok        = False
        self.extraction_method = "?"
        self.confidence      = 0.0
        self.valid           = False
        self.product         : Optional[NormalizedProduct] = None
        self.validation_errors: list[str] = []
        self.validation_warns: list[str]  = []
        self.fields          : dict[str, bool] = {}
        self.blockers        : list[str] = []
        self.warnings        : list[str] = []
        self.export_path     : Optional[str] = None
        self.duration        = 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Validation logic
# ──────────────────────────────────────────────────────────────────────────────

def validate_source(key: str, use_fixture: bool, do_export: bool) -> SourceResult:
    result = SourceResult(key)
    t0 = time.monotonic()

    _hr(f"{key.upper()}")

    # 1. Adapter load
    try:
        adapter           = get_adapter(key)
        result.adapter_ok = True
        result.platform   = adapter.PLATFORM_FAMILY
        _ok(f"Adapter: {type(adapter).__name__} | platform={adapter.PLATFORM_FAMILY}")
    except Exception as e:
        result.blockers.append(f"Adapter load failed: {e}")
        _fail(f"Adapter load FAILED: {e}")
        result.duration = time.monotonic() - t0
        return result

    # 2. Category discovery (no HTTP needed)
    try:
        cats = adapter.discover_category_urls()
        result.categories = cats
        _ok(f"Category URLs ({len(cats)}):")
        for c in cats:
            _info(f"  {c}")
    except Exception as e:
        result.warnings.append(f"discover_category_urls failed: {e}")
        _warn(f"Category discovery: {e}")

    # 3. Build raw payload
    if use_fixture:
        _info(f"Mode: FIXTURE (offline)")
        try:
            builder = FIXTURE_BUILDERS.get(key)
            if not builder:
                result.blockers.append(f"No fixture defined for {key}")
                _fail(f"No fixture for {key}")
                result.duration = time.monotonic() - t0
                return result
            raw = builder(adapter)
            _ok(f"Fixture built: extraction_method={raw.extraction_method}")
        except Exception as e:
            result.blockers.append(f"Fixture build failed: {e}")
            _fail(f"Fixture build failed: {e}")
            traceback.print_exc()
            result.duration = time.monotonic() - t0
            return result
    else:
        _info(f"Mode: LIVE HTTP")
        try:
            cats = result.categories or adapter.discover_category_urls()
            urls = adapter.discover_product_urls(limit=3)
            if not urls:
                result.blockers.append("Discovery returned 0 URLs (possible block or empty site)")
                _fail("Discovery: 0 URLs")
                result.duration = time.monotonic() - t0
                return result
            _ok(f"Discovered {len(urls)} URL(s), fetching first...")
            raw = adapter.fetch_product_page(urls[0])
            _ok(f"Fetched: {urls[0]}")
        except Exception as e:
            result.blockers.append(f"Discovery/fetch failed: {type(e).__name__}: {e}")
            _fail(f"Discovery/fetch error: {type(e).__name__}: {e}")
            result.duration = time.monotonic() - t0
            return result

    # 4. Parse
    try:
        parse_result = adapter.parse_product(raw)
        if parse_result.success and parse_result.product:
            p                      = parse_result.product
            result.parse_ok        = True
            result.product         = p
            result.extraction_method = parse_result.extraction_method
            result.confidence      = parse_result.confidence
            _ok(f"Parse OK: method={parse_result.extraction_method}, confidence={parse_result.confidence:.2f}")
            _ok(f"  name:         {p.product_name}")
            _ok(f"  price:        ₪{p.current_price} (was ₪{p.original_price}) | on_sale={p.is_on_sale}")
            _ok(f"  images:       {len(p.image_urls)} img(s)")
            _ok(f"  sizes:        {p.sizes_available[:6]}")
            _ok(f"  colors:       {p.colors_available[:5]}")
            _ok(f"  brand:        {p.brand}")
            _ok(f"  category:     {p.category}")
            _ok(f"  gender:       {p.gender_target}")
            _ok(f"  in_stock:     {p.in_stock} / {p.stock_status}")
            _ok(f"  fabric:       {p.fabric_type} / composition: {p.composition}")
            _ok(f"  breadcrumbs:  {p.breadcrumbs}")
            _ok(f"  completeness: {p.completeness_score:.0%}")
            if parse_result.warnings:
                for w in parse_result.warnings[:3]:
                    _warn(f"  warn: {w.message if hasattr(w, 'message') else w}")
        else:
            result.blockers.append(f"Parse returned success=False: {parse_result.errors}")
            _fail(f"Parse FAILED: {parse_result.errors}")
    except Exception as e:
        result.blockers.append(f"Parse exception: {type(e).__name__}: {e}")
        _fail(f"Parse exception: {type(e).__name__}: {e}")
        traceback.print_exc()

    # 5. Schema validation
    if result.product:
        p  = result.product
        vr = validator.validate(p)
        result.valid = vr.valid
        result.validation_errors = [i.issue for i in vr.errors]
        result.validation_warns  = [i.issue for i in vr.warnings]
        result.fields = {
            "product_name":    bool(p.product_name),
            "current_price":   bool(p.current_price),
            "image_urls":      bool(p.image_urls),
            "source_site":     bool(p.source_site),
            "product_url":     bool(p.product_url),
            "category":        bool(p.category),
            "brand":           bool(p.brand),
            "sizes_available": bool(p.sizes_available),
            "colors_available":bool(p.colors_available),
            "breadcrumbs":     bool(p.breadcrumbs),
        }
        if vr.valid:
            _ok(f"Schema validation: PASS (0 errors, {len(vr.warnings)} warnings)")
        else:
            for e in result.validation_errors:
                _fail(f"  schema error: {e}")
        for w in result.validation_warns[:2]:
            _warn(f"  schema warn: {w}")

    # 6. JSON export
    if result.product and do_export:
        try:
            out = writer.write_products(key, [result.product], include_raw=False)
            result.export_path = str(out)
            _ok(f"Exported → {out}")
        except Exception as e:
            result.warnings.append(f"Export failed: {e}")
            _warn(f"Export failed: {e}")

    # 7. Final status
    result.duration = round(time.monotonic() - t0, 2)
    result.status   = _compute_status(result)
    color = {"PASS": "[bold green]", "PARTIAL": "[bold yellow]", "FAIL": "[bold red]"}.get(result.status, "")
    end   = "[/bold green]" if result.status == "PASS" else "[/bold yellow]" if result.status == "PARTIAL" else "[/bold red]"
    console.print(f"\n→ {color}{result.status}{end} ({result.duration}s)\n" if HAS_RICH
                  else f"\n→ {result.status} ({result.duration}s)\n")
    return result


def _compute_status(r: SourceResult) -> str:
    if not r.adapter_ok:
        return "FAIL"
    if r.blockers:
        return "FAIL"
    if not r.parse_ok:
        return "PARTIAL"
    if not r.valid:
        return "PARTIAL"
    req_fields = ["product_name", "image_urls", "source_site", "product_url"]
    if not all(r.fields.get(f) for f in req_fields):
        return "PARTIAL"
    return "PASS"


# ──────────────────────────────────────────────────────────────────────────────
# Output helpers
# ──────────────────────────────────────────────────────────────────────────────

def _hr(title=""):
    if HAS_RICH:
        console.rule(f"[bold cyan]{title}[/bold cyan]")
    else:
        print(f"\n{'─'*70}\n  {title}\n{'─'*70}")

def _ok(msg):
    console.print(f"  [green]✓[/green] {msg}" if HAS_RICH else f"  ✓  {msg}")

def _fail(msg):
    console.print(f"  [red]✗[/red] {msg}" if HAS_RICH else f"  ✗  {msg}")

def _warn(msg):
    console.print(f"  [yellow]![/yellow] {msg}" if HAS_RICH else f"  !  {msg}")

def _info(msg):
    console.print(f"  {msg}")


# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────

def print_summary(results: list[SourceResult], mode: str) -> None:
    _hr(f"VALIDATION SUMMARY — mode={mode}")

    if HAS_RICH:
        t = Table(box=box.ROUNDED, border_style="cyan", title=f"DoStyle Source Validation ({mode})")
        t.add_column("Source",     style="bold", width=14)
        t.add_column("Platform",   width=12)
        t.add_column("Status",     width=9)
        t.add_column("Method",     width=10)
        t.add_column("Conf",       width=6)
        t.add_column("Name",       width=5, justify="center")
        t.add_column("Price",      width=6, justify="center")
        t.add_column("Images",     width=7, justify="center")
        t.add_column("Brand",      width=6, justify="center")
        t.add_column("Sizes",      width=6, justify="center")
        t.add_column("Colors",     width=7, justify="center")
        t.add_column("Breadcrumbs",width=11,justify="center")
        t.add_column("Completen.", width=10, justify="right")
        t.add_column("Blocker/Warn", width=38)

        for r in results:
            sc = {"PASS": "green", "PARTIAL": "yellow", "FAIL": "red"}.get(r.status, "white")
            fp = r.fields
            c  = r.product.completeness_score if r.product else 0.0
            note = (r.blockers[0] if r.blockers else (r.warnings[0] if r.warnings else ""))[:38]
            conf_str = f"{r.confidence:.2f}" if r.confidence else "—"
            t.add_row(
                r.key,
                r.platform,
                f"[{sc}]{r.status}[/{sc}]",
                r.extraction_method,
                conf_str,
                _tick(fp.get("product_name")),
                _tick(fp.get("current_price")),
                _tick(fp.get("image_urls")),
                _tick(fp.get("brand")),
                _tick(fp.get("sizes_available")),
                _tick(fp.get("colors_available")),
                _tick(fp.get("breadcrumbs")),
                f"{c:.0%}",
                f"[yellow]{note}[/yellow]" if note else "—",
            )
        console.print(t)
    else:
        print(f"{'Source':<16}{'Platform':<12}{'Status':<10}{'Method':<10}{'Conf':<6}{'Complete':<10}{'Blocker'}")
        print("─" * 90)
        for r in results:
            c = f"{r.product.completeness_score:.0%}" if r.product else "—"
            note = (r.blockers[0] if r.blockers else "")[:38]
            print(f"{r.key:<16}{r.platform:<12}{r.status:<10}{r.extraction_method:<10}{r.confidence:<6.2f}{c:<10}{note}")

    passed  = sum(1 for r in results if r.status == "PASS")
    partial = sum(1 for r in results if r.status == "PARTIAL")
    failed  = sum(1 for r in results if r.status == "FAIL")
    console.print(
        f"\n[bold]RESULT:[/bold] [green]{passed} PASS[/green]  "
        f"[yellow]{partial} PARTIAL[/yellow]  [red]{failed} FAIL[/red]  "
        f"(out of {len(results)} sources)" if HAS_RICH
        else f"\nRESULT: {passed} PASS | {partial} PARTIAL | {failed} FAIL / {len(results)}"
    )

    # Print parsed product names as quick sanity check
    if any(r.product for r in results):
        console.print("\n[bold]Parsed product names:[/bold]" if HAS_RICH else "\nParsed products:")
        for r in results:
            if r.product:
                p = r.product
                line = (f"  [green]{r.key:<16}[/green] {p.product_name[:45]:<46} "
                        f"₪{p.current_price or '—'}  imgs={len(p.image_urls)}"
                        if HAS_RICH else
                        f"  {r.key:<16} {p.product_name[:45]:<46} ₪{p.current_price or '—'}  imgs={len(p.image_urls)}")
                console.print(line)

    # Blockers section
    blockers = [(r.key, b) for r in results for b in r.blockers]
    if blockers:
        console.print("\n[bold red]BLOCKERS:[/bold red]" if HAS_RICH else "\nBLOCKERS:")
        for src, b in blockers:
            console.print(f"  [red]{src}:[/red] {b}" if HAS_RICH else f"  {src}: {b}")


def _tick(val) -> str:
    return "[green]✓[/green]" if val else "[red]✗[/red]"


# ──────────────────────────────────────────────────────────────────────────────
# Validation report JSON
# ──────────────────────────────────────────────────────────────────────────────

def export_validation_report(results: list[SourceResult]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report = {}
    for r in results:
        report[r.key] = {
            "status":            r.status,
            "platform":          r.platform,
            "adapter_ok":        r.adapter_ok,
            "parse_ok":          r.parse_ok,
            "valid":             r.valid,
            "extraction_method": r.extraction_method,
            "confidence":        r.confidence,
            "fields_present":    r.fields,
            "validation_errors": r.validation_errors,
            "validation_warns":  r.validation_warns,
            "blockers":          r.blockers,
            "warnings":          r.warnings,
            "duration_sec":      r.duration,
            "sample_product":    r.product.to_firebase_dict() if r.product else None,
        }
    out = OUTPUT_DIR / "_validation_report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    console.print(f"\nValidation report → [bold]{out}[/bold]" if HAS_RICH else f"\nReport → {out}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="DoStyle operational validator")
    ap.add_argument("--fixture",    action="store_true", help="Use local HTML/JSON fixtures (no HTTP)")
    ap.add_argument("--source",     help="Validate a single source key")
    ap.add_argument("--no-export",  action="store_true", help="Skip JSON file export")
    ap.add_argument("--debug",      action="store_true")
    args = ap.parse_args()

    mode = "FIXTURE" if args.fixture else "LIVE"
    keys = [args.source] if args.source else [m.source_key for m in list_sources()]

    console.print(f"\n[bold cyan]DoStyle Operational Validator[/bold cyan] — mode={mode}, sources={len(keys)}\n"
                  if HAS_RICH else f"\nDoStyle Validator — mode={mode}, sources={len(keys)}\n")

    results = []
    for key in keys:
        r = validate_source(key, use_fixture=args.fixture, do_export=not args.no_export)
        results.append(r)

    print_summary(results, mode)
    if not args.no_export:
        export_validation_report(results)

    failed = sum(1 for r in results if r.status == "FAIL")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
