from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .cache import cache_entry_for_url
from .core import ReleasePlan, release_summary_url, s3_url_for_key


def get_cached_inputs(plan: ReleasePlan, cache_dir: Path) -> list[dict[str, Any]]:
    urls: list[str] = []
    if plan.manifest_object is not None:
        urls.append(s3_url_for_key(plan.manifest_object["key"]))
    if plan.metadata_objects:
        urls.extend(s3_url_for_key(item["key"]) for item in plan.metadata_objects)
    if plan.html_summary is not None:
        urls.append(release_summary_url(plan.release))

    inputs = []
    for url in urls:
        entry = cache_entry_for_url(url, cache_dir=cache_dir)
        inputs.append(
            {
                "url": url,
                "cached": entry is not None,
                "bytes": entry["bytes"] if entry else None,
                "cached_at": entry["cached_at"] if entry and "cached_at" in entry else None,
                "suffix": entry["suffix"] if entry else Path(url).suffix or ".bin",
            }
        )
    return inputs


def summarize_release_sync(
    conn: sqlite3.Connection,
    plan: ReleasePlan,
    cache_dir: Path,
    incremental: bool,
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT name, synced_at, source_kind, source_locator, source_token, line_count, image_count
        FROM releases
        WHERE name = ?
        """,
        (plan.release,),
    ).fetchone()

    cached_inputs = get_cached_inputs(plan, cache_dir)
    cached_count = sum(1 for item in cached_inputs if item["cached"])
    total_inputs = len(cached_inputs)
    token_matches = row is not None and row["source_token"] == plan.source_token

    if plan.source_kind == "empty":
        action = "skip"
        reason = "no_source"
    elif incremental and token_matches:
        action = "skip"
        reason = "up_to_date"
    else:
        action = "sync"
        reason = "not_synced" if row is None else "stale"

    return {
        "release": plan.release,
        "source_kind": plan.source_kind,
        "source_locator": plan.source_locator,
        "source_token": plan.source_token,
        "action": action,
        "reason": reason,
        "source_cache_ready": total_inputs == cached_count,
        "cache": {
            "cached_inputs": cached_count,
            "total_inputs": total_inputs,
            "missing_inputs": total_inputs - cached_count,
            "inputs": cached_inputs,
        },
        "source_counts": {
            "line_prefixes": len(plan.line_prefixes or []),
            "metadata_objects": len(plan.metadata_objects or []),
            "html_lines": len(plan.html_summary or {}),
        },
        "db": {
            "present": row is not None,
            "source_token_match": bool(token_matches),
            "synced_at": row["synced_at"] if row else None,
            "source_kind": row["source_kind"] if row else None,
            "source_locator": row["source_locator"] if row else None,
            "line_count": row["line_count"] if row else None,
            "image_count": row["image_count"] if row else None,
        },
    }
