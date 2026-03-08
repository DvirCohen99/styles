"""
JSON output writer.
Writes normalized products to:
  - Per-source JSON files
  - Newline-delimited JSON (NDJSON)
  - Source-level aggregate stats
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from engine.schemas.product import NormalizedProduct
from engine.schemas.source import SourceStats

log = logging.getLogger("engine.output.json")

DEFAULT_OUTPUT_DIR = Path("data/output")


class JSONWriter:
    """Write normalized products to JSON files."""

    def __init__(self, output_dir: Path = DEFAULT_OUTPUT_DIR):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_products(
        self,
        source_key: str,
        products: list[NormalizedProduct],
        include_raw: bool = False,
    ) -> Path:
        """
        Write products to a JSON file: data/output/<source_key>_products.json
        Returns the output file path.
        """
        output_file = self.output_dir / f"{source_key}_products.json"

        records = []
        for p in products:
            if include_raw:
                records.append(p.to_json_dict())
            else:
                records.append(p.to_firebase_dict())

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2, default=str)

        log.info(f"Wrote {len(records)} products to {output_file}")
        return output_file

    def write_ndjson(
        self,
        source_key: str,
        products: list[NormalizedProduct],
    ) -> Path:
        """
        Write products to a newline-delimited JSON file.
        Suitable for BigQuery / Firestore batch imports.
        """
        output_file = self.output_dir / f"{source_key}_products.ndjson"

        with open(output_file, "w", encoding="utf-8") as f:
            for p in products:
                record = p.to_firebase_dict()
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

        log.info(f"Wrote {len(products)} products (NDJSON) to {output_file}")
        return output_file

    def write_stats(
        self,
        source_key: str,
        stats: SourceStats,
        products: list[NormalizedProduct],
    ) -> Path:
        """
        Write source-level aggregate stats JSON.
        """
        output_file = self.output_dir / f"{source_key}_stats.json"

        stats_dict = stats.to_dict()
        stats_dict["computed"] = {
            "total_products_in_output": len(products),
            "products_with_price": sum(1 for p in products if p.current_price),
            "products_with_images": sum(1 for p in products if p.image_urls),
            "products_on_sale": sum(1 for p in products if p.is_on_sale),
            "products_out_of_stock": sum(1 for p in products if p.out_of_stock),
            "products_new_collection": sum(1 for p in products if p.is_new_collection),
            "avg_image_count": (
                sum(p.image_count for p in products) / len(products)
                if products else 0
            ),
            "avg_confidence": (
                sum(p.extraction_confidence for p in products) / len(products)
                if products else 0
            ),
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(stats_dict, f, ensure_ascii=False, indent=2, default=str)

        log.info(f"Wrote stats to {output_file}")
        return output_file
