"""
Product validation layer.

Validates normalized products against required field rules and
generates per-source validation reports.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from engine.schemas.product import NormalizedProduct

log = logging.getLogger("engine.validation")


@dataclass
class FieldIssue:
    field: str
    issue: str
    severity: str = "warning"  # error | warning | info


@dataclass
class ProductValidationResult:
    product_id: str
    product_url: str
    valid: bool
    issues: list[FieldIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[FieldIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[FieldIssue]:
        return [i for i in self.issues if i.severity == "warning"]


@dataclass
class ValidationReport:
    source_key: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    warning_count: int = 0
    missing_name: int = 0
    missing_price: int = 0
    missing_images: int = 0
    missing_source_site: int = 0
    missing_product_url: int = 0
    product_results: list[ProductValidationResult] = field(default_factory=list)

    def summary_dict(self) -> dict:
        return {
            "source_key": self.source_key,
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "warning_count": self.warning_count,
            "missing_name_count": self.missing_name,
            "missing_price_count": self.missing_price,
            "missing_images_count": self.missing_images,
            "pass_rate": f"{(self.passed / max(1, self.total)) * 100:.1f}%",
        }

    def print_summary(self) -> None:
        print(f"\n{'='*50}")
        print(f"Validation Report: {self.source_key}")
        print(f"{'='*50}")
        print(f"  Total products:   {self.total}")
        print(f"  Passed:           {self.passed}  ({(self.passed / max(1, self.total)) * 100:.1f}%)")
        print(f"  Failed:           {self.failed}")
        print(f"  Warnings:         {self.warning_count}")
        print(f"  Missing name:     {self.missing_name}")
        print(f"  Missing price:    {self.missing_price}")
        print(f"  Missing images:   {self.missing_images}")
        print(f"{'='*50}")


class ProductValidator:
    """
    Validates NormalizedProduct instances against required and optional field rules.

    Required fields (failure if missing):
        - product_id
        - source_site
        - product_url
        - product_name

    Warned fields (warning if missing):
        - current_price
        - image_urls
        - category
        - brand

    Additional checks:
        - price sanity (> 0 and < 100000)
        - image URL format
        - product_url is a valid URL
    """

    REQUIRED_FIELDS = ["product_id", "source_site", "product_url", "product_name"]
    WARNED_FIELDS = ["current_price", "image_urls", "category"]

    def validate(self, product: NormalizedProduct) -> ProductValidationResult:
        issues: list[FieldIssue] = []

        # Required field checks
        for f in self.REQUIRED_FIELDS:
            val = getattr(product, f, None)
            if not val:
                issues.append(FieldIssue(field=f, issue=f"Required field '{f}' is missing or empty", severity="error"))

        # Warned field checks
        for f in self.WARNED_FIELDS:
            val = getattr(product, f, None)
            if not val:
                issues.append(FieldIssue(field=f, issue=f"Recommended field '{f}' is missing", severity="warning"))

        # Price sanity
        if product.current_price is not None:
            if product.current_price <= 0:
                issues.append(FieldIssue(field="current_price", issue="Price is <= 0", severity="error"))
            elif product.current_price > 50000:
                issues.append(FieldIssue(field="current_price", issue="Price suspiciously high (> 50000 ILS)", severity="warning"))

        # Image URL format
        for url in product.image_urls:
            if not url.startswith("http"):
                issues.append(FieldIssue(field="image_urls", issue=f"Non-HTTP image URL: {url[:60]}", severity="warning"))

        # Product URL format
        if product.product_url and not product.product_url.startswith("http"):
            issues.append(FieldIssue(field="product_url", issue="product_url is not an absolute URL", severity="error"))

        # Name length sanity
        if product.product_name and len(product.product_name) < 2:
            issues.append(FieldIssue(field="product_name", issue="Product name is too short (< 2 chars)", severity="error"))

        errors = [i for i in issues if i.severity == "error"]
        valid = len(errors) == 0

        return ProductValidationResult(
            product_id=product.product_id,
            product_url=product.product_url,
            valid=valid,
            issues=issues,
        )

    def validate_all(
        self,
        source_key: str,
        products: list[NormalizedProduct],
    ) -> ValidationReport:
        """Validate a batch of products and return a ValidationReport."""
        report = ValidationReport(source_key=source_key, total=len(products))

        for product in products:
            result = self.validate(product)
            report.product_results.append(result)

            if result.valid:
                report.passed += 1
            else:
                report.failed += 1

            report.warning_count += len(result.warnings)

            # Aggregate specific missing field counts
            missing_fields = {i.field for i in result.issues if i.severity == "error"}
            if "product_name" in missing_fields:
                report.missing_name += 1
            if "current_price" in {i.field for i in result.issues}:
                report.missing_price += 1
            if "image_urls" in {i.field for i in result.issues}:
                report.missing_images += 1
            if "source_site" in missing_fields:
                report.missing_source_site += 1
            if "product_url" in missing_fields:
                report.missing_product_url += 1

        return report
