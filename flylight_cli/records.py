from __future__ import annotations

import json
import sqlite3
from collections import Counter
from typing import Any

from .normalize import normalize_image_record, normalize_line_record, normalize_release_record


def asset_urls_from_image(release: str, line: str, payload: dict[str, Any]) -> list[str]:
    from .core import s3_url_for_key

    urls = []
    for value in payload.values():
        if not isinstance(value, str):
            continue
        lower = value.lower()
        if lower.endswith((".png", ".mp4", ".h5j", ".lsm", ".lsm.bz2", ".json")):
            if value.startswith(("http://", "https://")):
                urls.append(value)
            else:
                urls.append(s3_url_for_key(f"{release}/{line}/{value}"))
    return sorted(set(urls))


def get_image_records(
    conn: sqlite3.Connection,
    release: str,
    line: str,
    include_raw: bool = False,
) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT i.image_id, i.release, i.line, i.robot_id, i.slide_code, i.objective, i.area, i.tile, i.gender, i.roi,
                   i.annotations_text, i.metadata_key, i.metadata_url, i.raw_json, r.source_kind
            FROM images i
            JOIN releases r ON r.name = i.release
            WHERE i.release = ? AND i.line = ?
            ORDER BY i.slide_code, i.image_id
            """,
            (release, line),
        )
    ]
    result = []
    for row in rows:
        payload = json.loads(row.pop("raw_json"))
        row["asset_urls"] = asset_urls_from_image(release, line, payload)
        if include_raw:
            row["raw"] = payload
        result.append(normalize_image_record(row, payload))
    return result


def get_image_record(conn: sqlite3.Connection, image_id: int, include_raw: bool = False) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT i.image_id, i.release, i.line, i.robot_id, i.slide_code, i.objective, i.area, i.tile, i.gender, i.roi,
               i.annotations_text, i.metadata_key, i.metadata_url, i.raw_json, r.source_kind
        FROM images i
        JOIN releases r ON r.name = i.release
        WHERE i.image_id = ?
        """,
        (image_id,),
    ).fetchone()
    if row is None:
        raise SystemExit(f"no image found: {image_id}")
    record = dict(row)
    payload = json.loads(record.pop("raw_json"))
    record["asset_urls"] = asset_urls_from_image(record["release"], record["line"], payload)
    if include_raw:
        record["raw"] = payload
    return normalize_image_record(record, payload)


def get_line_matches(conn: sqlite3.Connection, line: str, releases: list[str] | None = None) -> list[dict[str, str]]:
    clauses = ["line = ?"]
    params: list[Any] = [line]
    if releases:
        placeholders = ",".join("?" for _ in releases)
        clauses.append(f"release IN ({placeholders})")
        params.extend(releases)
    return [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT release, line
            FROM line_releases
            WHERE {' AND '.join(clauses)}
            ORDER BY release
            """,
            params,
        )
    ]


def get_line_record(
    conn: sqlite3.Connection,
    release: str,
    line: str,
    include_raw: bool = False,
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT lr.release, lr.line, lr.image_count, lr.sample_count, lr.annotations_text, lr.rois_text,
               lr.robot_ids_text, lr.expressed_in_text, lr.genotype_text, lr.ad_text, lr.dbd_text,
               r.source_kind, r.source_locator, r.source_token
        FROM line_releases lr
        JOIN releases r ON r.name = lr.release
        WHERE lr.release = ? AND lr.line = ?
        """,
        (release, line),
    ).fetchone()
    if row is None:
        raise SystemExit(f"no line found: {line} in {release}")
    record = normalize_line_record(dict(row))
    record["images"] = get_image_records(conn, release, line, include_raw=include_raw)
    return record


def get_release_records(
    conn: sqlite3.Connection,
    release: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    clauses = ["1=1"]
    params: list[Any] = []
    if release:
        clauses.append("name = ?")
        params.append(release)
    sql = f"""
        SELECT name, manifest_key, manifest_url, publication_json, line_count, image_count, synced_at,
               source_kind, source_locator, source_token
        FROM releases
        WHERE {' AND '.join(clauses)}
        ORDER BY name
    """
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    rows = [dict(row) for row in conn.execute(sql, params)]
    result = []
    for row in rows:
        publication = json.loads(row.pop("publication_json"))
        row["release"] = row.pop("name")
        result.append(normalize_release_record(row, publication))
    return result


def get_release_record(conn: sqlite3.Connection, release: str) -> dict[str, Any]:
    rows = get_release_records(conn, release=release, limit=1)
    if not rows:
        raise SystemExit(f"no release found: {release}")
    return rows[0]


def compare_line_records(conn: sqlite3.Connection, line: str, releases: list[str] | None = None) -> dict[str, Any]:
    matches = get_line_matches(conn, line, releases=releases)
    if not matches:
        raise SystemExit(f"no line found: {line}")
    records = [get_line_record(conn, item["release"], item["line"], include_raw=False) for item in matches]
    shared_fields = {}
    for field in ["annotations", "rois", "robot_ids", "expressed_in", "genotype_parts", "ad_parts", "dbd_parts"]:
        sets = [set(record[field]) for record in records]
        shared_fields[field] = sorted(set.intersection(*sets)) if sets else []
    return {
        "line": line,
        "release_count": len(records),
        "shared": shared_fields,
        "releases": records,
    }


def get_db_stats(conn: sqlite3.Connection, release: str | None = None) -> dict[str, Any]:
    releases = get_release_records(conn, release=release)
    source_kinds = Counter(item["source_kind"] for item in releases)
    return {
        "release_count": len(releases),
        "line_count": sum(int(item["line_count"]) for item in releases),
        "image_count": sum(int(item["image_count"]) for item in releases),
        "source_kinds": dict(sorted(source_kinds.items())),
        "releases": releases,
    }
