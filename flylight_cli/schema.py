from __future__ import annotations

from typing import Any


SCHEMA: dict[str, dict[str, Any]] = {
    "line": {
        "description": "One synced line record with normalized arrays and embedded image records.",
        "key_fields": ["release", "line"],
        "produced_by": [
            "show-line",
            "show-release --include-lines",
            "export-ndjson --entity line",
        ],
        "fields": [
            "release",
            "line",
            "image_count",
            "sample_count",
            "annotations_text",
            "rois_text",
            "robot_ids_text",
            "expressed_in_text",
            "genotype_text",
            "ad_text",
            "dbd_text",
            "em_cell_types_text",
            "annotations",
            "rois",
            "robot_ids",
            "expressed_in",
            "genotype_parts",
            "ad_parts",
            "dbd_parts",
            "em_cell_types",
            "source_kind",
            "source_locator",
            "source_token",
            "images",
        ],
    },
    "image": {
        "description": "One synced image record with normalized arrays and derived asset URLs.",
        "key_fields": ["image_id"],
        "produced_by": [
            "show-image",
            "show-line",
            "export-ndjson --entity image",
        ],
        "fields": [
            "image_id",
            "release",
            "line",
            "robot_id",
            "slide_code",
            "objective",
            "area",
            "tile",
            "gender",
            "roi",
            "annotations_text",
            "em_cell_types_text",
            "annotations",
            "roi_terms",
            "genotype_parts",
            "ad_parts",
            "dbd_parts",
            "em_cell_types",
            "metadata_key",
            "metadata_url",
            "asset_urls",
            "source_kind",
        ],
    },
    "release": {
        "description": "One synced release summary record.",
        "key_fields": ["release"],
        "produced_by": [
            "show-release",
            "stats",
            "export-ndjson --entity release",
        ],
        "fields": [
            "release",
            "manifest_key",
            "manifest_url",
            "publication",
            "line_count",
            "image_count",
            "synced_at",
            "source_kind",
            "source_locator",
            "source_token",
        ],
    },
    "compare-line": {
        "description": "One row per release for a specific line diff, with shared metadata attached.",
        "key_fields": ["line", "release"],
        "produced_by": [
            "compare-line",
            "export-ndjson --entity compare-line",
        ],
        "fields": [
            "entity",
            "line",
            "release_count",
            "shared",
            "release",
            "source_kind",
            "image_count",
            "sample_count",
            "record",
        ],
    },
    "compare-release": {
        "description": "Release diff export: one summary row plus per-line status rows.",
        "key_fields": ["left_release", "right_release"],
        "produced_by": [
            "compare-release",
            "export-ndjson --entity compare-release",
        ],
        "fields": [
            "entity",
            "status",
            "left_release",
            "right_release",
            "line",
            "left_line_count",
            "right_line_count",
            "added_count",
            "removed_count",
            "changed_count",
            "unchanged_count",
            "record",
            "left",
            "right",
        ],
    },
}


def schema_for_entity(entity: str | None = None) -> dict[str, Any]:
    if entity is None:
        return SCHEMA
    if entity not in SCHEMA:
        raise SystemExit(f"unknown schema entity: {entity}")
    return {entity: SCHEMA[entity]}
