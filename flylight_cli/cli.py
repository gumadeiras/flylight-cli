from __future__ import annotations

import argparse
import json
import sys
from typing import Any, TextIO

from .cache import DEFAULT_CACHE_DIR, OfflineCacheMiss, cache_stats, set_cache_options
from .core import (
    DEFAULT_WORKERS,
    json_dumps,
    list_releases,
    plan_release,
    s3_url_for_key,
    should_skip_incremental,
    sync_release_from_plan,
)
from .examples import examples_for_topic
from .db import connect_db, ensure_parent
from .normalize import normalize_image_record
from .normalize import normalize_line_record
from .parser import build_parser as build_cli_parser
from .query import build_image_search_sql, build_line_search_sql, build_line_text_search_sql
from .records import (
    asset_urls_from_image,
    compare_line_records,
    compare_release_records,
    export_compare_line_rows,
    export_compare_release_rows,
    get_db_stats,
    get_image_record,
    get_line_matches,
    get_line_record,
    get_release_record,
    get_release_records,
)
from .reindex import reindex_em_cell_types
from .schema import schema_for_entity
from .snapshot import export_snapshot, import_snapshot
from .sync_plan import summarize_release_sync


def apply_cache_args(args: argparse.Namespace) -> None:
    try:
        set_cache_options(
            cache_dir=getattr(args, "cache_dir", DEFAULT_CACHE_DIR),
            offline=getattr(args, "offline", False),
            refresh=getattr(args, "refresh_cache", False),
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def sync_incremental_enabled(args: argparse.Namespace) -> bool:
    return args.incremental or (args.all and not args.force)


def selected_releases(args: argparse.Namespace) -> list[str]:
    releases = args.release or ([] if not args.all else list_releases())
    if not releases:
        raise SystemExit("choose --all or at least one --release")
    return releases


def progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def cmd_releases(args: argparse.Namespace) -> int:
    apply_cache_args(args)
    rows = []
    try:
        for release in list_releases():
            plan = plan_release(release, include_html_fallback=False)
            source_url = s3_url_for_key(plan.manifest_object["key"]) if plan.manifest_object else None
            rows.append(
                {
                    "release": release,
                    "source_kind": plan.source_kind,
                    "manifest_key": plan.manifest_object["key"] if plan.manifest_object else None,
                    "manifest_url": source_url,
                    "source_locator": plan.source_locator,
                }
            )
    except OfflineCacheMiss as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        for row in rows:
            print("\t".join([row["release"], row["source_kind"], row["manifest_url"] or row["source_locator"]]))
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    apply_cache_args(args)
    incremental = sync_incremental_enabled(args)
    refresh_cache = getattr(args, "refresh_cache", False)
    workers = getattr(args, "workers", DEFAULT_WORKERS)
    conn = connect_db(args.db)
    try:
        raw_dir = None if args.no_raw else args.raw_dir
        synced = []
        skipped = []
        progress("selecting releases")
        releases = selected_releases(args)
        progress(f"selected {len(releases)} release(s)")
        for index, release in enumerate(releases, start=1):
            progress(f"[{index}/{len(releases)}] planning {release}")
            plan = plan_release(release, include_html_fallback=True, workers=workers)
            if plan.source_kind == "empty":
                progress(f"[{index}/{len(releases)}] skipped {release}: no_source")
                skipped.append({"release": release, "reason": "no_source"})
                continue
            if incremental and not refresh_cache and should_skip_incremental(conn, release, plan.source_token):
                progress(f"[{index}/{len(releases)}] skipped {release}: up_to_date")
                skipped.append({"release": release, "reason": "up_to_date"})
                continue
            progress(f"[{index}/{len(releases)}] syncing {release}: kind={plan.source_kind}")
            result = sync_release_from_plan(conn, plan, raw_dir, workers=workers, progress=progress)
            synced.append(result)
            progress(
                f"[{index}/{len(releases)}] synced {release}: "
                f"kind={result['source_kind']} lines={result['lines']} images={result['images']}"
            )
    except OfflineCacheMiss as exc:
        raise SystemExit(str(exc)) from exc
    else:
        payload = {"synced": synced, "skipped": skipped}
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            for item in synced:
                print(f"{item['release']}\tkind={item['source_kind']}\tlines={item['lines']}\timages={item['images']}")
            for item in skipped:
                print(f"{item['release']}\tskipped={item['reason']}", file=sys.stderr)
        return 0
    finally:
        conn.close()


def cmd_update(args: argparse.Namespace) -> int:
    args.release = None
    args.all = True
    args.incremental = True
    args.force = False
    return cmd_sync(args)


def cmd_find(args: argparse.Namespace) -> int:
    for name in [
        "line",
        "annotation",
        "roi",
        "robot_id",
        "expressed_in",
        "genotype",
        "ad",
        "dbd",
        "em_cell_type",
        "min_images",
        "min_samples",
    ]:
        setattr(args, name, None)
    args.term = args.query
    return cmd_search(args)


def cmd_images(args: argparse.Namespace) -> int:
    for name in ["line", "annotation", "roi", "robot_id", "area", "objective", "gender", "em_cell_type"]:
        setattr(args, name, None)
    args.term = args.query
    return cmd_search_images(args)


def cmd_cache_info(args: argparse.Namespace) -> int:
    payload = cache_stats(args.cache_dir)
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    fields = [payload["cache_dir"], f"entries={payload['entries']}", f"bytes={payload['bytes']}"]
    if payload.get("oldest_cached_at"):
        fields.append(f"oldest={payload['oldest_cached_at']}")
    if payload.get("newest_cached_at"):
        fields.append(f"newest={payload['newest_cached_at']}")
    print("\t".join(fields))
    for suffix, count in sorted(payload["suffix_counts"].items()):
        print(f"suffix\t{suffix}\t{count}")
    return 0


def cmd_sync_plan(args: argparse.Namespace) -> int:
    apply_cache_args(args)
    incremental = sync_incremental_enabled(args)
    workers = getattr(args, "workers", DEFAULT_WORKERS)
    conn = connect_db(args.db)
    try:
        rows = []
        for release in selected_releases(args):
            plan = plan_release(release, include_html_fallback=True, workers=workers)
            rows.append(
                summarize_release_sync(
                    conn,
                    plan,
                    cache_dir=args.cache_dir,
                    incremental=incremental,
                )
            )
    except OfflineCacheMiss as exc:
        raise SystemExit(str(exc)) from exc
    finally:
        conn.close()

    payload = {"incremental": incremental, "releases": rows}
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    for row in rows:
        print(
            "\t".join(
                [
                    row["release"],
                    row["source_kind"],
                    f"action={row['action']}",
                    f"reason={row['reason']}",
                    f"cache={row['cache']['cached_inputs']}/{row['cache']['total_inputs']}",
                ]
            )
        )
    return 0


def cmd_schema(args: argparse.Namespace) -> int:
    payload = schema_for_entity(args.entity)
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    for entity, info in payload.items():
        print(entity)
        print(f"description\t{info['description']}")
        print(f"key_fields\t{' | '.join(info['key_fields'])}")
        print(f"produced_by\t{' | '.join(info['produced_by'])}")
        print(f"fields\t{' | '.join(info['fields'])}")
    return 0


def cmd_examples(args: argparse.Namespace) -> int:
    payload = examples_for_topic(args.topic)
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    for topic, info in payload.items():
        print(topic)
        print(f"description\t{info['description']}")
        for command in info["commands"]:
            print(f"command\t{command}")
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


def cmd_reindex(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
        payload = reindex_em_cell_types(conn, release=args.release)
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
        return 0
    finally:
        conn.close()


def cmd_search(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
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
    finally:
        conn.close()


def cmd_search_images(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
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
    finally:
        conn.close()


def cmd_search_text(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
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
    finally:
        conn.close()


def cmd_show_line(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
        matches = get_line_matches(conn, args.line, releases=[args.release] if args.release else None)
        if not matches:
            raise SystemExit(f"no line found: {args.line}")
        result = {
            "line": args.line,
            "releases": [get_line_record(conn, item["release"], item["line"], include_raw=args.raw) for item in matches],
        }
        print(json.dumps(result, indent=2))
        return 0
    finally:
        conn.close()


def cmd_show_release(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
        result = get_release_record(conn, args.release)
        if args.include_lines:
            sql, params = build_line_search_sql(args)
            rows = [dict(row) for row in conn.execute(sql, params)]
            result["lines"] = [get_line_record(conn, row["release"], row["line"], include_raw=args.raw) for row in rows]
        print(json.dumps(result, indent=2))
        return 0
    finally:
        conn.close()


def cmd_show_image(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
        print(json.dumps(get_image_record(conn, args.image_id, include_raw=args.raw), indent=2))
        return 0
    finally:
        conn.close()


def cmd_compare_line(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
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
    finally:
        conn.close()


def cmd_compare_release(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
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
    finally:
        conn.close()


def cmd_stats(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    try:
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
    finally:
        conn.close()


def write_ndjson(rows: list[dict[str, Any]], out: TextIO) -> None:
    for row in rows:
        out.write(json_dumps(row) + "\n")


def require_arg(value: Any, flag: str) -> Any:
    if value in (None, "", []):
        raise SystemExit(f"choose {flag}")
    return value


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
        elif args.entity == "release":
            payload = get_release_records(conn, release=args.release, limit=args.limit)
            write_ndjson(payload, out_handle)
        elif args.entity == "compare-line":
            line = require_arg(args.line, "--line")
            payload = export_compare_line_rows(conn, line=line, releases=args.release, include_records=args.raw)
            write_ndjson(payload, out_handle)
        else:
            left_release = require_arg(args.left_release, "--left-release")
            right_release = require_arg(args.right_release, "--right-release")
            payload = export_compare_release_rows(
                conn,
                left_release=left_release,
                right_release=right_release,
                include_records=args.raw,
            )
            write_ndjson(payload, out_handle)
    finally:
        if args.out:
            out_handle.close()
        conn.close()
    return 0


def command_handlers() -> dict[str, Any]:
    return {
        "update": cmd_update,
        "releases": cmd_releases,
        "find": cmd_find,
        "images": cmd_images,
        "sync": cmd_sync,
        "sync-plan": cmd_sync_plan,
        "cache-info": cmd_cache_info,
        "schema": cmd_schema,
        "examples": cmd_examples,
        "snapshot-export": cmd_snapshot_export,
        "snapshot-import": cmd_snapshot_import,
        "reindex": cmd_reindex,
        "search": cmd_search,
        "search-text": cmd_search_text,
        "search-images": cmd_search_images,
        "show-line": cmd_show_line,
        "show-image": cmd_show_image,
        "show-release": cmd_show_release,
        "compare-line": cmd_compare_line,
        "compare-release": cmd_compare_release,
        "stats": cmd_stats,
        "export-ndjson": cmd_export_ndjson,
    }


def build_parser() -> argparse.ArgumentParser:
    return build_cli_parser(command_handlers())


def command_parser(parser: argparse.ArgumentParser, command: str) -> argparse.ArgumentParser | None:
    return parser.get_default("subcommands").get(command)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    parser = build_parser()
    if not argv:
        parser.print_help()
        return 0
    if len(argv) == 1:
        subparser = command_parser(parser, argv[0])
        if subparser is not None:
            subparser.print_help()
            return 0
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 0
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
