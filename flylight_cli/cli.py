from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, TextIO

from .cache import DEFAULT_CACHE_DIR, OfflineCacheMiss, cache_stats, set_cache_options
from .core import (
    DEFAULT_DB,
    DEFAULT_RAW_DIR,
    DEFAULT_WORKERS,
    json_dumps,
    list_releases,
    plan_release,
    s3_url_for_key,
    should_skip_incremental,
    sync_release_from_plan,
)
from .db import connect_db, ensure_parent
from .normalize import normalize_image_record
from .normalize import normalize_line_record
from .query import build_image_search_sql, build_line_search_sql, build_line_text_search_sql
from .records import (
    asset_urls_from_image,
    compare_line_records,
    compare_release_records,
    get_db_stats,
    get_image_record,
    get_line_matches,
    get_line_record,
    get_release_record,
    get_release_records,
)
from .snapshot import export_snapshot, import_snapshot


def apply_cache_args(args: argparse.Namespace) -> None:
    try:
        set_cache_options(
            cache_dir=getattr(args, "cache_dir", DEFAULT_CACHE_DIR),
            offline=getattr(args, "offline", False),
            refresh=getattr(args, "refresh_cache", False),
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def add_cache_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--offline", action="store_true", help="use cached HTTP responses only")
    parser.add_argument("--refresh-cache", action="store_true", help="bypass cached HTTP responses and refresh them")


def cmd_releases(args: argparse.Namespace) -> int:
    apply_cache_args(args)
    rows = []
    try:
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
    except OfflineCacheMiss as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        for row in rows:
            print("\t".join([row["release"], row["source_kind"], row["manifest_url"] or "NO_MANIFEST"]))
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    apply_cache_args(args)
    incremental = args.incremental or (args.all and not args.force)
    workers = getattr(args, "workers", DEFAULT_WORKERS)
    conn = connect_db(args.db)
    raw_dir = None if args.no_raw else args.raw_dir
    synced = []
    skipped = []
    try:
        releases = args.release or ([] if not args.all else list_releases())
        if not releases:
            raise SystemExit("choose --all or at least one --release")
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
    except OfflineCacheMiss as exc:
        raise SystemExit(str(exc)) from exc
    payload = {"synced": synced, "skipped": skipped}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        for item in synced:
            print(f"{item['release']}\tkind={item['source_kind']}\tlines={item['lines']}\timages={item['images']}")
        for item in skipped:
            print(f"{item['release']}\tskipped={item['reason']}", file=sys.stderr)
    return 0


def cmd_cache_info(args: argparse.Namespace) -> int:
    payload = cache_stats(args.cache_dir)
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    print("\t".join([payload["cache_dir"], f"entries={payload['entries']}", f"bytes={payload['bytes']}"]))
    return 0


def cmd_snapshot_export(args: argparse.Namespace) -> int:
    payload = export_snapshot(args.out, db_path=args.db, raw_dir=args.raw_dir, cache_dir=args.cache_dir)
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    print(
        "\t".join(
            [
                payload["archive_path"],
                f"db={payload['db_present']}",
                f"raw_files={payload['raw_file_count']}",
                f"cache_entries={payload['cache_entries']}",
            ]
        )
    )
    return 0


def cmd_snapshot_import(args: argparse.Namespace) -> int:
    payload = import_snapshot(
        args.archive,
        db_path=args.db,
        raw_dir=args.raw_dir,
        cache_dir=args.cache_dir,
        force=args.force,
    )
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    imported = payload["imported"]
    print(
        "\t".join(
            [
                payload["archive_path"],
                f"db={imported['db']}",
                f"raw_files={imported['raw_files']}",
                f"cache_files={imported['cache_files']}",
            ]
        )
    )
    return 0


def cmd_search(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    sql, params = build_line_search_sql(args)
    rows = [normalize_line_record(dict(row)) for row in conn.execute(sql, params)]
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    for row in rows:
        fields = [row["line"], row["release"], f"images={row['image_count']}", f"samples={row['sample_count']}"]
        if row["expressed_in_text"]:
            fields.append(row["expressed_in_text"])
        print("\t".join(fields))
    return 0


def cmd_search_images(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    sql, params = build_image_search_sql(args)
    rows = [dict(row) for row in conn.execute(sql, params)]
    payload = []
    for row in rows:
        raw = json.loads(row.pop("raw_json"))
        row["asset_urls"] = asset_urls_from_image(row["release"], row["line"], raw)
        if args.raw:
            row["raw"] = raw
        payload.append(normalize_image_record(row, raw))
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    for row in payload:
        fields = [
            str(row["image_id"]),
            row["line"],
            row["release"],
            row.get("area") or "",
            row.get("objective") or "",
            row.get("roi") or "",
        ]
        print("\t".join(fields))
    return 0


def cmd_search_text(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    sql, params = build_line_text_search_sql(args)
    rows = []
    for row in conn.execute(sql, params):
        item = normalize_line_record(dict(row))
        item["rank"] = row["rank"]
        rows.append(item)
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    for row in rows:
        fields = [
            row["line"],
            row["release"],
            f"rank={row['rank']:.3f}",
            f"images={row['image_count']}",
            f"samples={row['sample_count']}",
        ]
        if row["expressed_in_text"]:
            fields.append(row["expressed_in_text"])
        print("\t".join(fields))
    return 0


def cmd_show_line(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    matches = get_line_matches(conn, args.line, releases=[args.release] if args.release else None)
    if not matches:
        raise SystemExit(f"no line found: {args.line}")
    result = {
        "line": args.line,
        "releases": [get_line_record(conn, item["release"], item["line"], include_raw=args.raw) for item in matches],
    }
    print(json.dumps(result, indent=2))
    return 0


def cmd_show_release(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    result = get_release_record(conn, args.release)
    if args.include_lines:
        sql, params = build_line_search_sql(args)
        rows = [dict(row) for row in conn.execute(sql, params)]
        result["lines"] = [get_line_record(conn, row["release"], row["line"], include_raw=args.raw) for row in rows]
    print(json.dumps(result, indent=2))
    return 0


def cmd_show_image(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    print(json.dumps(get_image_record(conn, args.image_id, include_raw=args.raw), indent=2))
    return 0


def cmd_compare_line(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    result = compare_line_records(conn, args.line, releases=args.release)
    if args.json:
        print(json.dumps(result, indent=2))
        return 0
    print(f"line={result['line']}\treleases={result['release_count']}")
    for field, values in result["shared"].items():
        if values:
            print(f"shared_{field}\t{' | '.join(values)}")
    for row in result["releases"]:
        print(
            "\t".join(
                [
                    row["release"],
                    row["source_kind"],
                    f"images={row['image_count']}",
                    f"samples={row['sample_count']}",
                ]
            )
        )
    return 0


def cmd_compare_release(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    result = compare_release_records(
        conn,
        left_release=args.left_release,
        right_release=args.right_release,
        include_lines=args.include_lines,
    )
    if args.json:
        print(json.dumps(result, indent=2))
        return 0
    summary = result["summary"]
    print(
        "\t".join(
            [
                result["left_release"]["release"],
                result["right_release"]["release"],
                f"added={summary['added_count']}",
                f"removed={summary['removed_count']}",
                f"changed={summary['changed_count']}",
                f"unchanged={summary['unchanged_count']}",
            ]
        )
    )
    for label in ["added_lines", "removed_lines", "changed_lines"]:
        if result[label]:
            print(f"{label}\t{' | '.join(result[label])}")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    payload = get_db_stats(conn, release=args.release)
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    print(
        "\t".join(
            [
                f"releases={payload['release_count']}",
                f"lines={payload['line_count']}",
                f"images={payload['image_count']}",
            ]
        )
    )
    for kind, count in payload["source_kinds"].items():
        print(f"source_kind\t{kind}\t{count}")
    for row in payload["releases"]:
        print(
            "\t".join(
                [
                    row["release"],
                    row["source_kind"],
                    f"lines={row['line_count']}",
                    f"images={row['image_count']}",
                ]
            )
        )
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
            sql, params = build_line_search_sql(args)
            rows = [dict(row) for row in conn.execute(sql, params)]
            payload = [get_line_record(conn, row["release"], row["line"], include_raw=args.raw) for row in rows]
            write_ndjson(payload, out_handle)
        elif args.entity == "image":
            sql, params = build_image_search_sql(args)
            rows = [dict(row) for row in conn.execute(sql, params)]
            payload = []
            for row in rows:
                raw = json.loads(row.pop("raw_json"))
                row["asset_urls"] = asset_urls_from_image(row["release"], row["line"], raw)
                if args.raw:
                    row["raw"] = raw
                payload.append(normalize_image_record(row, raw))
            write_ndjson(payload, out_handle)
        else:
            payload = get_release_records(conn, release=args.release, limit=args.limit)
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
    add_cache_args(p)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_releases)

    p = sub.add_parser("sync", help="sync one or more releases into sqlite")
    add_cache_args(p)
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

    p = sub.add_parser("cache-info", help="show HTTP cache location and size")
    p.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_cache_info)

    p = sub.add_parser("snapshot-export", help="bundle db, raw manifests, and HTTP cache for offline reuse")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_snapshot_export)

    p = sub.add_parser("snapshot-import", help="restore db, raw manifests, and HTTP cache from a snapshot")
    p.add_argument("archive", type=Path)
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    p.add_argument("--force", action="store_true", help="overwrite an existing target db")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_snapshot_import)

    p = sub.add_parser("search", help="search synced line records")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--release")
    p.add_argument("--line")
    p.add_argument("--annotation")
    p.add_argument("--roi")
    p.add_argument("--robot-id")
    p.add_argument("--expressed-in")
    p.add_argument("--genotype")
    p.add_argument("--ad")
    p.add_argument("--dbd")
    p.add_argument("--source-kind", choices=["manifest", "line-metadata", "cgi-html", "empty"])
    p.add_argument("--min-images", type=int)
    p.add_argument("--min-samples", type=int)
    p.add_argument("--term")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_search)

    p = sub.add_parser("search-text", help="full-text search synced line records")
    p.add_argument("query", help="SQLite FTS query, e.g. 'DNp04 AND 31B08'")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--release")
    p.add_argument("--source-kind", choices=["manifest", "line-metadata", "cgi-html", "empty"])
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_search_text)

    p = sub.add_parser("search-images", help="search synced image records")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--release")
    p.add_argument("--line")
    p.add_argument("--annotation")
    p.add_argument("--roi")
    p.add_argument("--robot-id")
    p.add_argument("--area")
    p.add_argument("--objective")
    p.add_argument("--gender")
    p.add_argument("--source-kind", choices=["manifest", "line-metadata", "cgi-html", "empty"])
    p.add_argument("--term")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--raw", action="store_true", help="include raw image payloads")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_search_images)

    p = sub.add_parser("show-line", help="show one line with images + asset urls")
    p.add_argument("line")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--release")
    p.add_argument("--raw", action="store_true", help="include raw image payloads")
    p.set_defaults(func=cmd_show_line)

    p = sub.add_parser("show-image", help="show one image record with asset urls")
    p.add_argument("image_id", type=int)
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--raw", action="store_true", help="include raw image payload")
    p.set_defaults(func=cmd_show_image)

    p = sub.add_parser("compare-line", help="compare one line across releases")
    p.add_argument("line")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--release", action="append", help="limit comparison to specific releases")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_compare_line)

    p = sub.add_parser("compare-release", help="compare two synced releases")
    p.add_argument("left_release")
    p.add_argument("right_release")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--include-lines", action="store_true")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_compare_release)

    p = sub.add_parser("show-release", help="show one release with optional embedded lines")
    p.add_argument("release")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--include-lines", action="store_true")
    p.add_argument("--line", help="substring filter when embedding lines")
    p.add_argument("--annotation")
    p.add_argument("--roi")
    p.add_argument("--robot-id")
    p.add_argument("--expressed-in")
    p.add_argument("--genotype")
    p.add_argument("--ad")
    p.add_argument("--dbd")
    p.add_argument("--source-kind", choices=["manifest", "line-metadata", "cgi-html", "empty"])
    p.add_argument("--min-images", type=int)
    p.add_argument("--min-samples", type=int)
    p.add_argument("--term")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--raw", action="store_true", help="include raw image payloads in embedded lines")
    p.set_defaults(func=cmd_show_release)

    p = sub.add_parser("stats", help="show counts for synced releases")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--release")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_stats)

    p = sub.add_parser("export-ndjson", help="export line or image records for agent ingest")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--entity", choices=["line", "image", "release"], default="line")
    p.add_argument("--release")
    p.add_argument("--line")
    p.add_argument("--annotation")
    p.add_argument("--roi")
    p.add_argument("--robot-id")
    p.add_argument("--expressed-in")
    p.add_argument("--genotype")
    p.add_argument("--ad")
    p.add_argument("--dbd")
    p.add_argument("--area")
    p.add_argument("--objective")
    p.add_argument("--gender")
    p.add_argument("--source-kind", choices=["manifest", "line-metadata", "cgi-html", "empty"])
    p.add_argument("--min-images", type=int)
    p.add_argument("--min-samples", type=int)
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
