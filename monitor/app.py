"""
DoStyle Internal Monitoring UI — Flask application.

Routes:
  /                   — Home dashboard
  /sources            — Live source grid
  /activity           — Live activity feed
  /products           — Products table
  /products/<id>      — Product detail
  /api/stats          — JSON: dashboard stats
  /api/sources        — JSON: source stats + sync states
  /api/activity       — JSON: recent sync events
  /api/products       — JSON: products list
  /api/product/<id>   — JSON: single product
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Optional

from flask import Flask, jsonify, render_template, request, abort

log = logging.getLogger("monitor.app")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET", "dostyle-monitor-secret")

# ── Firestore singleton ───────────────────────────────────────────────────────

_db = None


def get_db():
    global _db
    if _db is None:
        try:
            from engine.firestore.manager import FirestoreIngestionManager
            _db = FirestoreIngestionManager()
        except Exception as e:
            log.warning(f"Firestore init failed: {e}")
            _db = None
    return _db


# ── Source registry helper ────────────────────────────────────────────────────

def get_all_source_keys() -> list[str]:
    try:
        from engine.registry.source_registry import SourceRegistry
        return SourceRegistry.all_keys()
    except Exception:
        return []


def get_source_meta_map() -> dict[str, dict]:
    try:
        from engine.registry.source_registry import SourceRegistry
        return {m.source_key: m.model_dump() for m in SourceRegistry.all_metas()}
    except Exception:
        return {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts_ago(ts_str: Optional[str]) -> str:
    """Convert ISO timestamp to human-readable '2h ago' format."""
    if not ts_str:
        return "-"
    try:
        if hasattr(ts_str, "isoformat"):
            ts = ts_str
        else:
            ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        diff = now - ts
        sec = int(diff.total_seconds())
        if sec < 60:
            return f"{sec}s ago"
        elif sec < 3600:
            return f"{sec // 60}m ago"
        elif sec < 86400:
            return f"{sec // 3600}h ago"
        else:
            return f"{sec // 86400}d ago"
    except Exception:
        return str(ts_str)[:19] if ts_str else "-"


def _quality_color(score: float) -> str:
    if score >= 0.8:
        return "quality-high"
    elif score >= 0.5:
        return "quality-mid"
    return "quality-low"


def _status_badge(status: str) -> str:
    badges = {
        "ok": "badge-ok", "idle": "badge-idle", "running": "badge-running",
        "failed": "badge-failed", "error": "badge-failed", "degraded": "badge-warn",
        "done": "badge-ok", "unknown": "badge-idle",
    }
    return badges.get(status, "badge-idle")


app.jinja_env.globals.update(
    ts_ago=_ts_ago,
    quality_color=_quality_color,
    status_badge=_status_badge,
)


# ── Page routes ───────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    db = get_db()
    stats = {}
    source_states = {}
    source_stats_map = {}
    all_keys = get_all_source_keys()

    if db and db.available:
        try:
            stats = db.get_dashboard_stats()
        except Exception:
            pass
        try:
            source_states = db.get_all_sync_states()
        except Exception:
            pass
        try:
            source_stats_map = db.get_all_source_stats()
        except Exception:
            pass

    healthy_sources = sum(
        1 for k in all_keys
        if source_states.get(k, {}).get("current_job_status") not in ("failed", "error")
    )
    failing_sources = len(all_keys) - healthy_sources
    active_jobs = sum(
        1 for k in all_keys
        if source_states.get(k, {}).get("current_job_status") == "running"
    )
    warnings_count = sum(
        s.get("warnings_count", 0) for s in source_stats_map.values()
    )

    return render_template(
        "dashboard.html",
        stats=stats,
        source_states=source_states,
        source_stats=source_stats_map,
        all_source_keys=all_keys,
        healthy_sources=healthy_sources,
        failing_sources=failing_sources,
        active_jobs=active_jobs,
        warnings_count=warnings_count,
        db_available=bool(db and db.available),
    )


@app.route("/sources")
def sources_page():
    db = get_db()
    meta_map = get_source_meta_map()
    source_states = {}
    source_stats_map = {}

    if db and db.available:
        try:
            source_states = db.get_all_sync_states()
        except Exception:
            pass
        try:
            source_stats_map = db.get_all_source_stats()
        except Exception:
            pass

    sources = []
    for key, meta in meta_map.items():
        state = source_states.get(key, {})
        stats = source_stats_map.get(key, {})
        sources.append({
            "key": key,
            "name": meta.get("source_name", key),
            "platform": meta.get("platform_family", "?"),
            "base_url": meta.get("base_url", ""),
            "active": meta.get("active", True),
            "state": state,
            "stats": stats,
        })

    sources.sort(key=lambda s: s.get("name", ""))

    return render_template(
        "sources.html",
        sources=sources,
        db_available=bool(db and db.available),
    )


@app.route("/activity")
def activity_page():
    db = get_db()
    events = []
    if db and db.available:
        try:
            events = db.get_recent_sync_events(limit=200)
        except Exception:
            pass

    return render_template(
        "activity.html",
        events=events,
        db_available=bool(db and db.available),
    )


@app.route("/products")
def products_page():
    source_filter = request.args.get("source")
    sale_filter = request.args.get("sale")
    limit = min(int(request.args.get("limit", 100)), 500)

    db = get_db()
    products = []
    source_keys = get_all_source_keys()

    if db and db.available:
        try:
            is_on_sale = True if sale_filter == "1" else (False if sale_filter == "0" else None)
            products = db.get_products(
                source_key=source_filter or None,
                is_on_sale=is_on_sale,
                limit=limit,
            )
        except Exception as e:
            log.warning(f"Products query failed: {e}")

    return render_template(
        "products.html",
        products=products,
        source_keys=source_keys,
        source_filter=source_filter or "",
        sale_filter=sale_filter or "",
        limit=limit,
        db_available=bool(db and db.available),
    )


@app.route("/products/<product_id>")
def product_detail(product_id: str):
    db = get_db()
    product = None
    if db and db.available:
        try:
            product = db.get_product(product_id)
        except Exception:
            pass

    if not product:
        abort(404)

    return render_template(
        "product_detail.html",
        product=product,
        db_available=bool(db and db.available),
    )


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    db = get_db()
    if not db or not db.available:
        return jsonify({"error": "Firestore not available"}), 503
    try:
        stats = db.get_dashboard_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sources")
def api_sources():
    db = get_db()
    meta_map = get_source_meta_map()
    states = {}
    stats_map = {}

    if db and db.available:
        try:
            states = db.get_all_sync_states()
            stats_map = db.get_all_source_stats()
        except Exception:
            pass

    result = {}
    for key, meta in meta_map.items():
        result[key] = {
            "meta": meta,
            "state": states.get(key, {}),
            "stats": stats_map.get(key, {}),
        }
    return jsonify(result)


@app.route("/api/activity")
def api_activity():
    db = get_db()
    source = request.args.get("source")
    limit = min(int(request.args.get("limit", 100)), 500)

    if not db or not db.available:
        return jsonify([])

    try:
        events = db.get_recent_sync_events(limit=limit, source_key=source or None)
        return jsonify(events)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/products")
def api_products():
    db = get_db()
    source = request.args.get("source")
    sale = request.args.get("sale")
    limit = min(int(request.args.get("limit", 50)), 200)

    if not db or not db.available:
        return jsonify([])

    try:
        is_on_sale = True if sale == "1" else (False if sale == "0" else None)
        products = db.get_products(source_key=source, is_on_sale=is_on_sale, limit=limit)
        return jsonify(products)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/product/<product_id>")
def api_product(product_id: str):
    db = get_db()
    if not db or not db.available:
        return jsonify({"error": "Firestore not available"}), 503
    try:
        product = db.get_product(product_id)
        if not product:
            return jsonify({"error": "Not found"}), 404
        return jsonify(product)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/failures")
def api_failures():
    db = get_db()
    source = request.args.get("source")
    if not db or not db.available:
        return jsonify([])
    try:
        failures = db.get_parse_failures(source_key=source, limit=50)
        return jsonify(failures)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Error pages ───────────────────────────────────────────────────────────────

@app.errorhandler(404)
def not_found(e):
    return render_template("404.html"), 404


@app.errorhandler(500)
def server_error(e):
    return render_template("500.html", error=str(e)), 500


# ── Entry point ───────────────────────────────────────────────────────────────

def run_monitor(host: str = "0.0.0.0", port: int = 5000, debug: bool = False) -> None:
    """Start the monitoring UI server."""
    print(f"\n  DoStyle Monitor UI — http://{host}:{port}\n")
    app.run(host=host, port=port, debug=debug, use_reloader=False)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="DoStyle Monitor UI")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    run_monitor(args.host, args.port, args.debug)
