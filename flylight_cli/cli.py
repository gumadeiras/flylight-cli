from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, TextIO

from .core import (
    DEFAULT_DB,
    DEFAULT_RAW_DIR,
    DEFAULT_WORKERS,
    ReleasePlan,
    asset_urls_from_image,
    connect_db,
    ensure_parent,
    get_line_record,
    json_dumps,
    list_releases,
    plan_release,
    s3_url_for_key,
    should_skip_incremental,
    sync_release_from_plan,
)


def cmd_releases(args: argparse.Namespace) -> int:
    rows = []
    for release in list_releases():
        plan = plan_release(release, include_html_fallback=False)
        rows.append(
            {
                "release": release,
                "source_kind": plan.source_kind,
                "manifest_key": plan.manifest_object["key"] if plan.manifest_object else None,
                "manifest_url": s3_url_for_key(plan.manifest_object["key"]) if plan.manifest_object else None,
            }
        )
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        for row in rows:
            print("\t".join([row["release"], row["source_kind"], row["manifest_url"] or "NO_MANIFEST"]))
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    releases = args.release or ([] if not args.all else list_releases())
    if not releases:
        raise SystemExit("choose --all or at least one --release")
    incremental = args.incremental or (args.all and not args.force)
    workers = getattr(args, "workers", DEFAULT_WORKERS)
    conn = connect_db(args.db)
    raw_dir = None if args.no_raw else args.raw_dir
    synced = []
    skipped = []
    for release in releases:
        plan = plan_release(release, include_html_fallback=True, workers=workers)
        if plan.source_kind == "empty":
            skipped.append({"release": release, "reason": "no_source"})
            continue
        if incremental and should_skip_incremental(conn, release, plan.source_token):
            skipped.append({"release": release, "reason": "up_to_date"})
            continue
        result = sync_release_from_plan(conn, plan, raw_dir, workers=workers)
        synced.append(result)
        if args.verbose:
            print(
                f"synced {release}: kind={result['source_kind']} lines={result['lines']} images={result['images']}",
                file=sys.stderr,
            )
    payload = {"synced": synced, "skipped": skipped}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        for item in synced:
            print(f"{item['release']}\tkind={item['source_kind']}\tlines={item['lines']}\timages={item['images']}")
        for item in skipped:
            print(f"{item['release']}\tskipped={item['reason']}", file=sys.stderr)
    return 0


def build_search_sql(args: argparse.Namespace) -> tuple[str, list[Any]]:
    clauses = ["1=1"]
    params: list[Any] = []
    if args.release:
        clauses.append("release = ?")
        params.append(args.release)
    if args.line:
        clauses.append("line LIKE ?")
        params.append(f"%{args.line}%")
    if args.annotation:
        clauses.append("annotations_text LIKE ?")
        params.append(f"%{args.annotation}%")
    if args.roi:
        clauses.append("rois_text LIKE ?")
        params.append(f"%{args.roi}%")
    if args.term:
        clauses.append(
            "(line LIKE ? OR annotations_text LIKE ? OR rois_text LIKE ? OR expressed_in_text LIKE ? OR genotype_text LIKE ? OR ad_text LIKE ? OR dbd_text LIKE ?)"
        )
        like = f"%{args.term}%"
        params.extend([like, like, like, like, like, like, like])
    sql = f"""
      SELECT release, line, image_count, sample_count, annotations_text, rois_text, robot_ids_text,
             expressed_in_text, genotype_text, ad_text, dbd_text
      FROM line_releases
      WHERE {' AND '.join(clauses)}
      ORDER BY line, release
      LIMIT ?
    """
    params.append(args.limit)
    return sql, params


def cmd_search(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    sql, params = build_search_sql(args)
    rows = [dict(row) for row in conn.execute(sql, params)]
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    for row in rows:
        fields = [row["line"], row["release"], f"images={row['image_count']}", f"samples={row['sample_count']}"]
        if row["expressed_in_text"]:
            fields.append(row["expressed_in_text"])
        print("\t".join(fields))
    return 0


def cmd_show_line(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    clauses = ["line = ?"]
    params: list[Any] = [args.line]
    if args.release:
        clauses.append("release = ?")
        params.append(args.release)
    matches = [
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
    if not matches:
        raise SystemExit(f"no line found: {args.line}")
    result = {
        "line": args.line,
        "releases": [get_line_record(conn, item["release"], item["line"], include_raw=args.raw) for item in matches],
    }
    print(json.dumps(result, indent=2))
    return 0


def write_ndjson(rows: list[dict[str, Any]], out: TextIO) -> None:
    for row in rows:
        out.write(json_dumps(row) + "\n")


def cmd_export_ndjson(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    out_handle: TextIO
    if args.out:
        ensure_parent(args.out)
        out_handle = args.out.open("w", encoding="utf-8")
    else:
        out_handle = sys.stdout

    try:
        if args.entity == "line":
            sql, params = build_search_sql(args)
            rows = [dict(row) for row in conn.execute(sql, params)]
            payload = [get_line_record(conn, row["release"], row["line"], include_raw=args.raw) for row in rows]
            write_ndjson(payload, out_handle)
        else:
            clauses = ["1=1"]
            params: list[Any] = []
            if args.release:
                clauses.append("release = ?")
                params.append(args.release)
            if args.line:
                clauses.append("line LIKE ?")
                params.append(f"%{args.line}%")
            if args.term:
                clauses.append("(line LIKE ? OR roi LIKE ? OR annotations_text LIKE ?)")
                like = f"%{args.term}%"
                params.extend([like, like, like])
            params.append(args.limit)
            rows = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT image_id, release, line, robot_id, slide_code, objective, area, tile, gender, roi,
                           annotations_text, metadata_key, metadata_url, raw_json
                    FROM images
                    WHERE {' AND '.join(clauses)}
                    ORDER BY release, line, slide_code, image_id
                    LIMIT ?
                    """,
                    params,
                )
            ]
            payload = []
            for row in rows:
                raw = json.loads(row.pop("raw_json"))
                row["asset_urls"] = asset_urls_from_image(row["release"], row["line"], raw)
                if args.raw:
                    row["raw"] = raw
                payload.append(row)
            write_ndjson(payload, out_handle)
    finally:
        if args.out:
            out_handle.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Index/query Janelia FlyLight Split-GAL4 data from S3 + CGI fallback surfaces."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("releases", help="list releases and source types")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_releases)

    p = sub.add_parser("sync", help="sync one or more releases into sqlite")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--no-raw", action="store_true", help="skip writing raw manifest files")
    p.add_argument("--release", action="append", help="repeatable release name")
    p.add_argument("--all", action="store_true", help="sync every release found in the bucket")
    p.add_argument("--incremental", action="store_true", help="skip unchanged releases")
    p.add_argument("--force", action="store_true", help="disable incremental skip")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="parallel workers for fallback sync")
    p.add_argument("--json", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.set_defaults(func=cmd_sync)

    p = sub.add_parser("search", help="search synced line records")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--release")
    p.add_argument("--line")
    p.add_argument("--annotation")
    p.add_argument("--roi")
    p.add_argument("--term")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_search)

    p = sub.add_parser("show-line", help="show one line with images + asset urls")
    p.add_argument("line")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--release")
    p.add_argument("--raw", action="store_true", help="include raw image payloads")
    p.set_defaults(func=cmd_show_line)

    p = sub.add_parser("export-ndjson", help="export line or image records for agent ingest")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--entity", choices=["line", "image"], default="line")
    p.add_argument("--release")
    p.add_argument("--line")
    p.add_argument("--annotation")
    p.add_argument("--roi")
    p.add_argument("--term")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--raw", action="store_true", help="include raw image payloads")
    p.add_argument("--out", type=Path)
    p.set_defaults(func=cmd_export_ndjson)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
