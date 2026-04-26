from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from typing import Any

from .db import refresh_release_fts
from .normalize import extract_em_cell_type_terms


def join_terms(terms: set[str]) -> str:
    return " | ".join(sorted(terms))


def reindex_em_cell_types(conn: sqlite3.Connection, release: str | None = None) -> dict[str, Any]:
    release_clause = ""
    params: list[Any] = []
    if release:
        release_clause = "WHERE release = ?"
        params.append(release)

    image_rows = list(
        conn.execute(
            f"""
            SELECT image_id, release, line, raw_json
            FROM images
            {release_clause}
            ORDER BY release, line, image_id
            """,
            params,
        )
    )
    line_rows = list(
        conn.execute(
            f"""
            SELECT release, line
            FROM line_releases
            {release_clause}
            ORDER BY release, line
            """,
            params,
        )
    )

    image_updates = []
    line_terms: dict[tuple[str, str], set[str]] = defaultdict(set)
    releases = {row["release"] for row in line_rows}
    for row in image_rows:
        payload = json.loads(row["raw_json"])
        terms = extract_em_cell_type_terms(payload)
        image_updates.append((join_terms(set(terms)), row["image_id"]))
        if terms:
            line_terms[(row["release"], row["line"])].update(terms)

    line_updates = []
    for row in line_rows:
        key = (row["release"], row["line"])
        line_updates.append((join_terms(line_terms.get(key, set())), row["release"], row["line"]))

    with conn:
        conn.executemany("UPDATE images SET em_cell_types_text = ? WHERE image_id = ?", image_updates)
        conn.executemany(
            "UPDATE line_releases SET em_cell_types_text = ? WHERE release = ? AND line = ?",
            line_updates,
        )
        for release_name in sorted(releases):
            refresh_release_fts(conn, release_name)

    return {
        "release": release,
        "release_count": len(releases),
        "line_count": len(line_updates),
        "image_count": len(image_updates),
    }
