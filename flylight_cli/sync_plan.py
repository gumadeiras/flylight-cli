from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .cache import cache_entry_for_url
from .core import ReleasePlan, release_summary_url, s3_url_for_key


def plan_input_urls(plan: ReleasePlan) -> list[str]:
    urls: list[str] = []
    if plan.manifest_object is not None:
        urls.append(s3_url_for_key(plan.manifest_object["key"]))
    if plan.metadata_objects:
        urls.extend(s3_url_for_key(item["key"]) for item in plan.metadata_objects)
    if plan.html_summary is not None:
        urls.append(release_summary_url(plan.release))
    return urls


def get_cached_inputs(plan: ReleasePlan, cache_dir: Path) -> list[dict[str, Any]]:
    inputs = []
    for url in plan_input_urls(plan):
        entry = cache_entry_for_url(url, cache_dir=cache_dir)
        cached_at = entry["cached_at"] if entry and "cached_at" in entry else None
        bytes_count = entry["bytes"] if entry is not None else None
        suffix = entry["suffix"] if entry is not None else Path(url).suffix or ".bin"
        inputs.append(
            {
                "url": url,
                "cached": entry is not None,
                "bytes": bytes_count,
                "cached_at": cached_at,
                "suffix": suffix,
            }
        )
    return inputs


def sync_action(plan: ReleasePlan, incremental: bool, token_matches: bool, has_row: bool) -> tuple[str, str]:
    if plan.source_kind == "empty":
        return "skip", "no_source"
    if incremental and token_matches:
        return "skip", "up_to_date"
    return "sync", "stale" if has_row else "not_synced"


def db_state(row: sqlite3.Row | None) -> dict[str, Any]:
    record = dict(row) if row is not None else None
    return {
        "present": record is not None,
        "synced_at": record["synced_at"] if record else None,
        "source_kind": record["source_kind"] if record else None,
        "source_locator": record["source_locator"] if record else None,
        "line_count": record["line_count"] if record else None,
        "image_count": record["image_count"] if record else None,
        "source_token": record["source_token"] if record else None,
    }


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

    db = db_state(row)
    cached_inputs = get_cached_inputs(plan, cache_dir)
    cached_count = sum(1 for item in cached_inputs if item["cached"])
    total_inputs = len(cached_inputs)
    has_row = bool(db["present"])
    token_matches = db["source_token"] == plan.source_token
    action, reason = sync_action(plan, incremental=incremental, token_matches=token_matches, has_row=has_row)

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
            **db,
            "source_token_match": bool(token_matches),
        },
    }
