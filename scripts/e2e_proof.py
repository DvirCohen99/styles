#!/usr/bin/env python3
"""
End-to-end operational proof for the DoStyle Firestore ingestion engine.

Loads existing scraped product data from data/output/*.json, ingests them
through the real FirestoreIngestionManager into the Firestore emulator,
runs source stats computation, writes sync logs and system events, then
verifies all Firestore collections and confirms upsert idempotency.
"""
import json
import os
import sys
import uuid
import hashlib
from datetime import datetime, timezone
from pathlib import Path

# ── Point at emulator ────────────────────────────────────────────────────────
os.environ["FIRESTORE_EMULATOR_HOST"] = "127.0.0.1:8080"

# ── Seed Python path ─────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

import firebase_admin
from firebase_admin import credentials, firestore as fs

# ─────────────────────────────────────────────────────────────────────────────
DIVIDER = "=" * 70
SEP     = "-" * 50

def heading(s): print(f"\n{DIVIDER}\n  {s}\n{DIVIDER}")
def ok(s):      print(f"  ✓  {s}")
def info(s):    print(f"  ·  {s}")
def fail(s):    print(f"  ✗  {s}")

# ─────────────────────────────────────────────────────────────────────────────
# 1. INIT FIREBASE EMULATOR
# ─────────────────────────────────────────────────────────────────────────────
heading("1. INITIALIZING FIRESTORE EMULATOR")

try:
    firebase_admin.get_app()
except ValueError:
    firebase_admin.initialize_app(options={"projectId": "dostyle-demo"})

from engine.firestore.manager import FirestoreIngestionManager
db = FirestoreIngestionManager()
assert db.available, "Firestore not available!"
ok("FirestoreIngestionManager initialized against emulator")

# ─────────────────────────────────────────────────────────────────────────────
# 2. LOAD SCRAPED DATA FROM JSON FILES
# ─────────────────────────────────────────────────────────────────────────────
heading("2. LOADING SCRAPED PRODUCT DATA (from data/output/*.json)")

OUTPUT_DIR = Path("data/output")
source_products = {}

for json_file in sorted(OUTPUT_DIR.glob("*_products.json")):
    source_key = json_file.stem.replace("_products", "")
    try:
        with open(json_file) as f:
            records = json.load(f)
        if not isinstance(records, list):
            records = [records]
        source_products[source_key] = records
        info(f"{source_key:15} → {len(records):3d} products loaded from {json_file.name}")
    except Exception as e:
        fail(f"{source_key}: {e}")

total_loaded = sum(len(v) for v in source_products.values())
ok(f"Total products loaded: {total_loaded} across {len(source_products)} sources")

# ─────────────────────────────────────────────────────────────────────────────
# 3. NORMALIZE RECORDS TO REQUIRED SCHEMA
# ─────────────────────────────────────────────────────────────────────────────
heading("3. NORMALIZING RECORDS TO FIRESTORE SCHEMA")

def normalize_record(r: dict, source_key: str) -> dict:
    """
    Map whatever shape is in the JSON file to the FirestoreIngestionManager
    product dict shape.  Handles both old (site/name/price) and new
    (source_site/product_name/current_price) field names.
    """
    url  = r.get("product_url") or r.get("original_url") or r.get("url", "")
    name = r.get("product_name") or r.get("name") or r.get("title", "")
    site = r.get("source_site") or r.get("site") or source_key

    # Stable deterministic ID  (same as NormalizedProduct.make_id)
    key = f"{site}:{url.split('?')[0]}"
    pid = hashlib.md5(key.encode()).hexdigest()

    price    = r.get("current_price") or r.get("price")
    orig_p   = r.get("original_price")
    is_sale  = r.get("is_on_sale", False)
    if not is_sale and price and orig_p and orig_p > price:
        is_sale = True

    stock = r.get("stock_status", "unknown")
    if stock == "unknown" and r.get("in_stock", True):
        stock = "in_stock"

    images = r.get("image_urls") or r.get("images") or []
    if isinstance(images, str):
        images = [images]

    cat  = r.get("category")
    subc = r.get("subcategory")

    # breadcrumbs → category fallback
    breadcrumbs = r.get("breadcrumbs", [])
    if not cat and breadcrumbs:
        cat = breadcrumbs[-1] if breadcrumbs else None

    coll = r.get("collection") or r.get("collection_name")
    is_new = r.get("is_new_collection", False)

    colors = r.get("colors_available") or r.get("colors") or []
    sizes  = r.get("sizes_available")  or r.get("sizes")  or []

    q = r.get("completeness_score") or r.get("data_quality_score", 0.5)

    return {
        "product_id":            pid,
        "source_site":           site,
        "source_name":           r.get("source_name") or site.replace("_", " ").title(),
        "product_url":           url,
        "product_name":          name,
        "short_description":     r.get("short_description"),
        "original_description":  r.get("original_description"),
        "searchable_text_blob":  r.get("searchable_text_blob") or name,
        "breadcrumbs":           breadcrumbs,
        "bullet_points":         r.get("bullet_points", []),
        "current_price":         price,
        "original_price":        orig_p,
        "currency":              r.get("currency", "ILS"),
        "is_on_sale":            is_sale,
        "discount_amount":       r.get("discount_amount"),
        "discount_percent":      r.get("discount_percent"),
        "category":              cat,
        "subcategory":           subc,
        "collection":            coll,
        "collection_type":       r.get("collection_type"),
        "is_new_collection":     is_new,
        "brand":                 r.get("brand"),
        "gender_target":         r.get("gender_target"),
        "primary_image_url":     (images[0] if images else r.get("primary_image_url")),
        "image_urls":            images,
        "image_count":           len(images),
        "colors_available":      colors,
        "sizes_available":       sizes,
        "size_labels_normalized":r.get("size_labels_normalized", []),
        "per_variant_stock_if_available": r.get("per_variant_stock_if_available", {}),
        "stock_status":          stock,
        "in_stock":              stock not in ("out_of_stock",),
        "low_stock":             stock == "low_stock",
        "out_of_stock":          stock == "out_of_stock",
        "extraction_confidence": r.get("extraction_confidence", 1.0),
        "completeness_score":    q,
        "parser_version":        r.get("parser_version", "1.0.0"),
        "parser_status":         r.get("parser_status", "ok"),
        "source_mode":           r.get("extraction_method") or r.get("source_mode", "json"),
        "warnings":              r.get("warnings", []),
        "sku_if_available":      r.get("sku_if_available") or r.get("sku"),
        "source_product_reference": r.get("source_product_reference"),
    }

all_normalized: dict[str, list[dict]] = {}
for src, records in source_products.items():
    normalized = []
    for r in records:
        try:
            normalized.append(normalize_record(r, src))
        except Exception as e:
            fail(f"{src} record failed normalize: {e}")
    all_normalized[src] = normalized
    info(f"{src:15} → {len(normalized):3d} records normalized")

total_normalized = sum(len(v) for v in all_normalized.values())
ok(f"Total normalized: {total_normalized}")

# ─────────────────────────────────────────────────────────────────────────────
# 4. FIRST SYNC — INGEST ALL PRODUCTS INTO FIRESTORE
# ─────────────────────────────────────────────────────────────────────────────
heading("4. FIRST SYNC — INGESTING ALL PRODUCTS INTO FIRESTORE")

db.log_system_event("sync_all_started",
    sources=list(all_normalized.keys()),
    total_products=total_normalized,
    run="proof_run_1")

all_counters = {}
all_run_ids  = {}

for src, records in all_normalized.items():
    if not records:
        continue
    run_id = f"{src}_{uuid.uuid4().hex[:8]}"
    all_run_ids[src] = run_id

    # Set sync state → running
    db.set_sync_state(src, {
        "current_job_status": "running",
        "current_stage":      "ingesting",
        "current_message":    f"Ingesting {len(records)} products",
        "last_started_at":    datetime.now(timezone.utc).isoformat(),
        "adapter_status":     "ok",
    })

    db.log_sync_event(src, run_id, "sync_started", products_to_ingest=len(records))

    counters = db.upsert_products(records, src, run_id)
    all_counters[src] = counters

    # Mark sync state → done
    db.set_sync_state(src, {
        "current_job_status": "idle",
        "current_stage":      "done",
        "current_message":    "Sync complete",
        "last_finished_at":   datetime.now(timezone.utc).isoformat(),
        "adapter_status":     "ok",
        "last_run_id":        run_id,
        "last_sync_counts":   counters,
    })

    db.log_sync_event(src, run_id, "sync_completed",
        products_created=counters["created"],
        products_updated=counters["updated"],
        duration_sec=0.1)

    db.log_system_event("sync_completed",
        source_key=src, run_id=run_id,
        products_created=counters["created"],
        products_updated=counters["updated"])

    info(f"{src:15} → created={counters['created']:3d}  updated={counters['updated']:3d}  "
         f"price_changed={counters['price_changed']}  "
         f"stock_changed={counters['stock_changed']}  "
         f"unchanged={counters['unchanged']}")

total_created = sum(c["created"] for c in all_counters.values())
total_updated = sum(c["updated"] for c in all_counters.values())
ok(f"First sync complete: {total_created} created, {total_updated} updated")

# ─────────────────────────────────────────────────────────────────────────────
# 5. COMPUTE AND SAVE SOURCE STATS
# ─────────────────────────────────────────────────────────────────────────────
heading("5. COMPUTING SOURCE STATS FOR ALL SOURCES")

for src in all_normalized:
    stats = db.compute_and_save_source_stats(src)
    last_succ = datetime.now(timezone.utc).isoformat()
    db.update_source_stats(src, {
        "last_successful_sync":      last_succ,
        "last_sync_run_id":          all_run_ids.get(src, ""),
        "last_sync_products_scraped":len(all_normalized[src]),
        "parser_health_status":      "ok",
        "sync_success_rate":         1.0,
        "freshness_score":           1.0,
    })
    info(f"{src:15} → live={stats.get('total_live_products',0):3d}  "
         f"sale={stats.get('total_sale_products',0):3d}  "
         f"oos={stats.get('total_out_of_stock_products',0):3d}  "
         f"quality={stats.get('avg_quality_score',0):.2f}")

ok("Source stats saved")

# ─────────────────────────────────────────────────────────────────────────────
# 6. VERIFY ALL FIRESTORE COLLECTIONS
# ─────────────────────────────────────────────────────────────────────────────
heading("6. VERIFYING FIRESTORE COLLECTIONS")

raw_db = db.db  # underlying Firestore client

def count_collection(name):
    return sum(1 for _ in raw_db.collection(name).stream())

col_counts = {}
for col in ["products", "source_stats", "source_sync_state",
            "sync_logs", "system_events", "parse_failures"]:
    n = count_collection(col)
    col_counts[col] = n
    ok(f"  {col:22} → {n} documents")

assert col_counts["products"] == total_normalized, \
    f"Expected {total_normalized} products, got {col_counts['products']}"
ok(f"products collection size matches input ({total_normalized})")

# ─────────────────────────────────────────────────────────────────────────────
# 7. PRODUCTS PER SOURCE
# ─────────────────────────────────────────────────────────────────────────────
heading("7. PRODUCTS PER SOURCE IN FIRESTORE")

source_counts = {}
for src in all_normalized:
    docs = raw_db.collection("products").where("source_site", "==", src).stream()
    count = sum(1 for _ in docs)
    source_counts[src] = count
    ok(f"  {src:15} → {count} products in Firestore")

# ─────────────────────────────────────────────────────────────────────────────
# 8. SAMPLE DOCUMENT IDs
# ─────────────────────────────────────────────────────────────────────────────
heading("8. SAMPLE DOCUMENT IDs (5 random products)")

sample_docs = list(raw_db.collection("products").limit(5).stream())
sample_ids = []
for doc in sample_docs:
    d = doc.to_dict()
    sample_ids.append(doc.id)
    info(f"  ID={doc.id[:16]}...  src={d.get('source_site'):12}  "
         f"name={str(d.get('product_name',''))[:35]}")

ok(f"Sample IDs retrieved: {len(sample_ids)}")

# ─────────────────────────────────────────────────────────────────────────────
# 9. COLLECTION COUNTS (is_new_collection, is_on_sale)
# ─────────────────────────────────────────────────────────────────────────────
heading("9. COLLECTION & SALE COUNTS ACROSS ALL SOURCES")

sale_count     = 0
new_coll_count = 0
oos_count      = 0

for doc in raw_db.collection("products").stream():
    d = doc.to_dict()
    if d.get("is_on_sale"):         sale_count     += 1
    if d.get("is_new_collection"):  new_coll_count += 1
    if d.get("stock_status") == "out_of_stock": oos_count += 1

ok(f"  On sale:           {sale_count}")
ok(f"  New collection:    {new_coll_count}")
ok(f"  Out of stock:      {oos_count}")

# ─────────────────────────────────────────────────────────────────────────────
# 10. SYNC LOGS CHECK
# ─────────────────────────────────────────────────────────────────────────────
heading("10. SYNC LOG EVENTS")

sync_events = db.get_recent_sync_events(limit=50)
event_types = {}
for e in sync_events:
    t = e.get("event_type", "?")
    event_types[t] = event_types.get(t, 0) + 1

ok(f"  Total sync_log docs: {col_counts['sync_logs']}")
for etype, cnt in sorted(event_types.items()):
    info(f"    {etype:25} → {cnt}")

# Show 3 sample events
info("  Sample sync events:")
for e in sync_events[:3]:
    info(f"    [{e.get('source_key','?'):12}] {e.get('event_type','?'):20} ts={e.get('timestamp','?')[:19]}")

# ─────────────────────────────────────────────────────────────────────────────
# 11. SYSTEM EVENTS CHECK
# ─────────────────────────────────────────────────────────────────────────────
heading("11. SYSTEM EVENTS")

sys_events = db.get_recent_system_events(limit=30)
sys_types = {}
for e in sys_events:
    t = e.get("event_type", "?")
    sys_types[t] = sys_types.get(t, 0) + 1

ok(f"  Total system_event docs: {col_counts['system_events']}")
for etype, cnt in sorted(sys_types.items()):
    info(f"    {etype:25} → {cnt}")

# ─────────────────────────────────────────────────────────────────────────────
# 12. SOURCE SYNC STATE VERIFICATION
# ─────────────────────────────────────────────────────────────────────────────
heading("12. SOURCE SYNC STATE (all sources)")

all_states = db.get_all_sync_states()
for src, state in sorted(all_states.items()):
    status = state.get("current_job_status", "?")
    stage  = state.get("current_stage", "?")
    ok(f"  {src:15} → status={status:6}  stage={stage}")

# ─────────────────────────────────────────────────────────────────────────────
# 13. SECOND SYNC — UPSERT STABILITY / NO DUPLICATES
# ─────────────────────────────────────────────────────────────────────────────
heading("13. SECOND SYNC — TESTING UPSERT STABILITY (no duplicates)")

all_counters_2 = {}
for src, records in all_normalized.items():
    if not records:
        continue
    run_id = f"{src}_{uuid.uuid4().hex[:8]}_r2"
    counters = db.upsert_products(records, src, run_id)
    all_counters_2[src] = counters
    info(f"{src:15} → created={counters['created']:3d}  updated={counters['updated']:3d}  "
         f"unchanged={counters['unchanged']:3d}  price_changed={counters['price_changed']}")

total_created_2 = sum(c["created"] for c in all_counters_2.values())
total_updated_2 = sum(c["updated"] for c in all_counters_2.values())
total_unchanged_2 = sum(c["unchanged"] for c in all_counters_2.values())

info(f"Second sync totals: created={total_created_2}  updated={total_updated_2}  unchanged={total_unchanged_2}")

# Count products again — must be same
count_after_2 = count_collection("products")
ok(f"Product count after 2nd sync: {count_after_2} (was {total_normalized})")
assert count_after_2 == total_normalized, \
    f"DUPLICATE DETECTION FAILED: {count_after_2} != {total_normalized}"
ok("No duplicates created — upsert is stable and idempotent")
assert total_created_2 == 0, \
    f"Expected 0 new creates on 2nd run, got {total_created_2}"
ok(f"All {total_unchanged_2 + total_updated_2} records handled as updates/unchanged (0 phantom creates)")

# ─────────────────────────────────────────────────────────────────────────────
# 14. SINGLE PRODUCT DETAIL VERIFICATION
# ─────────────────────────────────────────────────────────────────────────────
heading("14. SINGLE PRODUCT DOCUMENT DETAIL")

sample_id = sample_ids[0]
p = db.get_product(sample_id)
assert p, f"Could not retrieve product {sample_id}"

ok(f"  product_id:          {p.get('product_id', '')[:16]}...")
ok(f"  source_site:         {p.get('source_site')}")
ok(f"  product_name:        {str(p.get('product_name',''))[:50]}")
ok(f"  current_price:       {p.get('current_price')}")
ok(f"  is_on_sale:          {p.get('is_on_sale')}")
ok(f"  stock_status:        {p.get('stock_status')}")
ok(f"  image_count:         {p.get('image_count')}")
ok(f"  colors_available:    {p.get('colors_available', [])[:5]}")
ok(f"  sizes_available:     {p.get('sizes_available', [])[:5]}")
ok(f"  is_active:           {p.get('is_active')}")
ok(f"  is_missing_from_src: {p.get('is_missing_from_source')}")
ok(f"  first_seen_at:       {str(p.get('first_seen_at',''))[:19]}")
ok(f"  last_seen_at:        {str(p.get('last_seen_at',''))[:19]}")
ok(f"  data_quality_score:  {p.get('data_quality_score')}")
ok(f"  extraction_confidence: {p.get('extraction_confidence')}")
ok(f"  parser_version:      {p.get('parser_version')}")

# ─────────────────────────────────────────────────────────────────────────────
# 15. DASHBOARD STATS
# ─────────────────────────────────────────────────────────────────────────────
heading("15. DASHBOARD AGGREGATE STATS")

dash = db.get_dashboard_stats()
ok(f"  total_products:        {dash.get('total_products')}")
ok(f"  sale_products:         {dash.get('sale_products')}")
ok(f"  new_collection_prods:  {dash.get('new_collection_products')}")
ok(f"  out_of_stock_prods:    {dash.get('out_of_stock_products')}")
ok(f"  missing_products:      {dash.get('missing_products')}")
ok(f"  products_updated_today:{dash.get('products_updated_today')}")
ok(f"  products_added_today:  {dash.get('products_added_today')}")
by_src = dash.get("by_source", {})
for src in sorted(by_src):
    info(f"    by_source[{src}]: count={by_src[src].get('count')}  sale={by_src[src].get('sale')}")

# ─────────────────────────────────────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
heading("FINAL PROOF SUMMARY")

print()
print(f"  {'METRIC':<35} {'VALUE':>10}")
print(f"  {'-'*45}")
print(f"  {'Total products in Firestore':<35} {count_after_2:>10}")
for src, cnt in sorted(source_counts.items()):
    print(f"    {src:<33} {cnt:>10}")
print(f"  {'On-sale products':<35} {sale_count:>10}")
print(f"  {'New collection products':<35} {new_coll_count:>10}")
print(f"  {'Out-of-stock products':<35} {oos_count:>10}")
print(f"  {'sync_logs documents':<35} {col_counts['sync_logs']:>10}")
print(f"  {'system_events documents':<35} {col_counts['system_events']:>10}")
print(f"  {'source_stats documents':<35} {col_counts['source_stats']:>10}")
print(f"  {'source_sync_state documents':<35} {col_counts['source_sync_state']:>10}")
print(f"  {'--- UPSERT STABILITY ---'}")
print(f"  {'Creates on 2nd sync (must be 0)':<35} {total_created_2:>10}")
print(f"  {'Unchanged on 2nd sync':<35} {total_unchanged_2:>10}")
print()
print(f"  ALL ASSERTIONS PASSED ✓")
print()
