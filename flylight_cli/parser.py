from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from . import __version__
from .cache import DEFAULT_CACHE_DIR
from .core import DEFAULT_DB, DEFAULT_RAW_DIR, DEFAULT_WORKERS
from .examples import EXAMPLES
from .schema import SCHEMA


SOURCE_KIND_CHOICES = ["manifest", "line-metadata", "cgi-html", "flew-html", "empty"]
ENTITY_CHOICES = ["line", "image", "release", "compare-line", "compare-release"]


def add_cache_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--offline", action="store_true", help="use cached HTTP responses only")
    parser.add_argument("--refresh-cache", action="store_true", help="bypass cached HTTP responses and refresh them")


def add_db_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)


def add_sync_args(parser: argparse.ArgumentParser, *, include_selection: bool) -> None:
    add_cache_args(parser)
    add_db_arg(parser)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--no-raw", action="store_true", help="skip writing raw manifest files")
    if include_selection:
        parser.add_argument("--release", action="append", help="repeatable release name")
        parser.add_argument("--all", action="store_true", help="sync every release")
        parser.add_argument("--incremental", action="store_true", help="skip unchanged releases")
        parser.add_argument("--force", action="store_true", help="disable incremental skip")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--verbose", action="store_true")


def add_sync_plan_args(parser: argparse.ArgumentParser) -> None:
    add_cache_args(parser)
    add_db_arg(parser)
    parser.add_argument("--release", action="append", help="repeatable release name")
    parser.add_argument("--all", action="store_true", help="plan every release")
    parser.add_argument("--incremental", action="store_true", help="mark unchanged releases as skips")
    parser.add_argument("--force", action="store_true", help="disable incremental skip")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--json", action="store_true")


def add_line_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--release")
    parser.add_argument("--line")
    parser.add_argument("--annotation")
    parser.add_argument("--roi")
    parser.add_argument("--robot-id")
    parser.add_argument("--expressed-in")
    parser.add_argument("--genotype")
    parser.add_argument("--ad")
    parser.add_argument("--dbd")
    parser.add_argument("--em-cell-type")
    parser.add_argument("--source-kind", choices=SOURCE_KIND_CHOICES)
    parser.add_argument("--min-images", type=int)
    parser.add_argument("--min-samples", type=int)
    parser.add_argument("--term")
    parser.add_argument("--limit", type=int, default=25)


def add_image_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--release")
    parser.add_argument("--line")
    parser.add_argument("--annotation")
    parser.add_argument("--roi")
    parser.add_argument("--robot-id")
    parser.add_argument("--area")
    parser.add_argument("--objective")
    parser.add_argument("--gender")
    parser.add_argument("--em-cell-type")
    parser.add_argument("--source-kind", choices=SOURCE_KIND_CHOICES)
    parser.add_argument("--term")
    parser.add_argument("--limit", type=int, default=25)


def add_line_show_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--line", help="substring filter when embedding lines")
    parser.add_argument("--annotation")
    parser.add_argument("--roi")
    parser.add_argument("--robot-id")
    parser.add_argument("--expressed-in")
    parser.add_argument("--genotype")
    parser.add_argument("--ad")
    parser.add_argument("--dbd")
    parser.add_argument("--em-cell-type")
    parser.add_argument("--source-kind", choices=SOURCE_KIND_CHOICES)
    parser.add_argument("--min-images", type=int)
    parser.add_argument("--min-samples", type=int)
    parser.add_argument("--term")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--raw", action="store_true", help="include raw image payloads in embedded lines")


def add_export_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--release")
    parser.add_argument("--line")
    parser.add_argument("--left-release")
    parser.add_argument("--right-release")
    parser.add_argument("--annotation")
    parser.add_argument("--roi")
    parser.add_argument("--robot-id")
    parser.add_argument("--expressed-in")
    parser.add_argument("--genotype")
    parser.add_argument("--ad")
    parser.add_argument("--dbd")
    parser.add_argument("--em-cell-type")
    parser.add_argument("--area")
    parser.add_argument("--objective")
    parser.add_argument("--gender")
    parser.add_argument("--source-kind", choices=SOURCE_KIND_CHOICES)
    parser.add_argument("--min-images", type=int)
    parser.add_argument("--min-samples", type=int)
    parser.add_argument("--term")
    parser.add_argument("--limit", type=int, default=100)


def set_handler(parser: argparse.ArgumentParser, handlers: Mapping[str, Callable[..., int]], name: str) -> None:
    parser.set_defaults(func=handlers[name])


def build_parser(handlers: Mapping[str, Callable[..., int]]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Index/query Janelia FlyLight Split-GAL4 and GAL4/LexA data from S3 + CGI surfaces."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")
    parser.set_defaults(subcommands=sub.choices)

    p = sub.add_parser("update", help="simple sync: update all sources incrementally")
    add_sync_args(p, include_selection=False)
    p.add_argument("--all", action="store_true", required=True, help="confirm updating every source")
    set_handler(p, handlers, "update")

    p = sub.add_parser("sources", help="list available sources")
    add_cache_args(p)
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "releases")

    p = sub.add_parser("find", help="simple line search")
    p.add_argument("query")
    add_db_arg(p)
    p.add_argument("--source-kind", choices=SOURCE_KIND_CHOICES)
    p.add_argument("--release")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "find")

    p = sub.add_parser("images", help="simple image search")
    p.add_argument("query")
    add_db_arg(p)
    p.add_argument("--source-kind", choices=SOURCE_KIND_CHOICES)
    p.add_argument("--release")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--raw", action="store_true")
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "images")

    p = sub.add_parser("line", help="show one line")
    p.add_argument("line")
    add_db_arg(p)
    p.add_argument("--release")
    p.add_argument("--raw", action="store_true")
    set_handler(p, handlers, "show-line")

    p = sub.add_parser("image", help="show one image")
    p.add_argument("image_id", type=int)
    add_db_arg(p)
    p.add_argument("--raw", action="store_true")
    set_handler(p, handlers, "show-image")

    p = sub.add_parser("release", help="show one release")
    p.add_argument("release")
    add_db_arg(p)
    p.add_argument("--include-lines", action="store_true")
    add_line_show_filters(p)
    set_handler(p, handlers, "show-release")

    p = sub.add_parser("releases", help="list releases and source types")
    add_cache_args(p)
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "releases")

    p = sub.add_parser("sync", help="sync one or more releases into sqlite")
    add_sync_args(p, include_selection=True)
    set_handler(p, handlers, "sync")

    p = sub.add_parser("sync-plan", help="dry-run sync planning with cache and db coverage")
    add_sync_plan_args(p)
    set_handler(p, handlers, "sync-plan")

    p = sub.add_parser("cache-info", help="show HTTP cache location and size")
    p.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "cache-info")

    p = sub.add_parser("schema", help="show record schemas")
    p.add_argument("--entity", choices=sorted(SCHEMA.keys()))
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "schema")

    p = sub.add_parser("examples", help="show command recipes")
    p.add_argument("--topic", choices=sorted(EXAMPLES.keys()))
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "examples")

    p = sub.add_parser("snapshot-export", help="bundle db, raw manifests, and HTTP cache")
    add_db_arg(p)
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "snapshot-export")

    p = sub.add_parser("snapshot-import", help="restore db, raw manifests, and HTTP cache")
    p.add_argument("archive", type=Path)
    add_db_arg(p)
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    p.add_argument("--force", action="store_true", help="overwrite an existing target db")
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "snapshot-import")

    p = sub.add_parser("reindex", help="rebuild derived searchable fields")
    add_db_arg(p)
    p.add_argument("--release")
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "reindex")

    p = sub.add_parser("search", help="search synced line records")
    add_db_arg(p)
    add_line_filters(p)
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "search")

    p = sub.add_parser("search-text", help="full-text search synced line records")
    p.add_argument("query", help="SQLite FTS query, e.g. 'DNp04 AND 31B08'")
    add_db_arg(p)
    p.add_argument("--release")
    p.add_argument("--source-kind", choices=SOURCE_KIND_CHOICES)
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "search-text")

    p = sub.add_parser("search-images", help="search synced image records")
    add_db_arg(p)
    add_image_filters(p)
    p.add_argument("--raw", action="store_true")
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "search-images")

    p = sub.add_parser("show-line", help="show one line with images + asset urls")
    p.add_argument("line")
    add_db_arg(p)
    p.add_argument("--release")
    p.add_argument("--raw", action="store_true")
    set_handler(p, handlers, "show-line")

    p = sub.add_parser("show-image", help="show one image record with asset urls")
    p.add_argument("image_id", type=int)
    add_db_arg(p)
    p.add_argument("--raw", action="store_true")
    set_handler(p, handlers, "show-image")

    p = sub.add_parser("show-release", help="show one release with optional embedded lines")
    p.add_argument("release")
    add_db_arg(p)
    p.add_argument("--include-lines", action="store_true")
    add_line_show_filters(p)
    set_handler(p, handlers, "show-release")

    p = sub.add_parser("compare-line", help="compare one line across releases")
    p.add_argument("line")
    add_db_arg(p)
    p.add_argument("--release", action="append", help="limit comparison to specific releases")
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "compare-line")

    p = sub.add_parser("compare-release", help="compare two synced releases")
    p.add_argument("left_release")
    p.add_argument("right_release")
    add_db_arg(p)
    p.add_argument("--include-lines", action="store_true")
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "compare-release")

    p = sub.add_parser("stats", help="show counts for synced releases")
    add_db_arg(p)
    p.add_argument("--release")
    p.add_argument("--json", action="store_true")
    set_handler(p, handlers, "stats")

    p = sub.add_parser("export-ndjson", help="export records for agent ingest")
    add_db_arg(p)
    p.add_argument("--entity", choices=ENTITY_CHOICES, default="line")
    add_export_filters(p)
    p.add_argument("--raw", action="store_true", help="include raw image payloads")
    p.add_argument("--out", type=Path)
    set_handler(p, handlers, "export-ndjson")

    return parser
