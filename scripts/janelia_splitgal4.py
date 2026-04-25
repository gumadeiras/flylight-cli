#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET


BUCKET = "janelia-flylight-imagery"
S3_HTTP_ROOT = f"https://s3.amazonaws.com/{BUCKET}"
S3_LIST_ROOT = f"{S3_HTTP_ROOT}/"
SPLITGAL4_SUMMARY_URL = "https://splitgal4.janelia.org/cgi-bin/splitgal4_summary.cgi"
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
USER_AGENT = "flylight-cli/0.2"
DEFAULT_DB = Path("data/janelia_splitgal4.sqlite")
DEFAULT_RAW_DIR = Path("data/raw_manifests")
DEFAULT_WORKERS = 12


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req) as resp:
        return resp.read()


def fetch_text(url: str) -> str:
    return fetch_bytes(url).decode("utf-8", errors="replace")


def fetch_json(url: str) -> Any:
    return json.loads(fetch_text(url))


def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def safe_slug(text: str) -> str:
    out = []
    for ch in text.lower():
        out.append(ch if ch.isalnum() else "_")
    slug = "".join(out).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "release"


def s3_url_for_key(key: str) -> str:
    return f"{S3_HTTP_ROOT}/{quote(key, safe='/')}"


def s3_list_xml(prefix: str = "", delimiter: str | None = None, marker: str | None = None) -> ET.Element:
    params: dict[str, str] = {"prefix": prefix}
    if delimiter:
        params["delimiter"] = delimiter
    if marker:
        params["marker"] = marker
    url = f"{S3_LIST_ROOT}?{urlencode(params)}"
    return ET.fromstring(fetch_bytes(url))


def s3_list_all(prefix: str = "", delimiter: str | None = None) -> tuple[list[dict[str, str]], list[str]]:
    marker = None
    contents: list[dict[str, str]] = []
    prefixes: list[str] = []
    while True:
        root = s3_list_xml(prefix=prefix, delimiter=delimiter, marker=marker)
        new_contents = []
        for node in root.findall("s3:Contents", NS):
            key = node.findtext("s3:Key", default="", namespaces=NS)
            last_modified = node.findtext("s3:LastModified", default="", namespaces=NS)
            new_contents.append({"key": key, "last_modified": last_modified})
        new_prefixes = [node.text or "" for node in root.findall("s3:CommonPrefixes/s3:Prefix", NS)]
        contents.extend(new_contents)
        prefixes.extend(new_prefixes)
        is_truncated = root.findtext("s3:IsTruncated", default="false", namespaces=NS) == "true"
        if not is_truncated:
            break
        next_marker = root.findtext("s3:NextMarker", default="", namespaces=NS)
        if next_marker:
            marker = next_marker
        elif new_contents:
            marker = new_contents[-1]["key"]
        elif new_prefixes:
            marker = new_prefixes[-1]
        else:
            break
    return contents, prefixes


def list_releases() -> list[str]:
    _, prefixes = s3_list_all(delimiter="/")
    return sorted(prefix.rstrip("/") for prefix in prefixes if prefix.rstrip("/") != "content")


def find_release_manifest_object(release: str) -> dict[str, str] | None:
    contents, _ = s3_list_all(prefix=f"{release}/", delimiter="/")
    for item in contents:
        if item["key"].endswith(".metadata.json"):
            return item
    return None


def normalize_annotations(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if value in (None, ""):
        return []
    return [str(value).strip()]


def extract_image_id(payload: dict[str, Any], metadata_key: str) -> int:
    direct = payload.get("id")
    if direct not in (None, ""):
        return int(direct)
    match = re.search(r"-(\d+)-metadata\.json$", metadata_key)
    if match:
        return int(match.group(1))
    digest = hashlib.sha1(metadata_key.encode("utf-8")).digest()[:8]
    return int.from_bytes(digest, "big") & 0x7FFFFFFFFFFFFFFF


def strip_html(raw: str) -> str:
    text = re.sub(r"<[^>]+>", "", raw)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_release_summary_html(release: str) -> str | None:
    url = f"{SPLITGAL4_SUMMARY_URL}?{urlencode({'_gsearch': 'Search', 'alps_release': release})}"
    try:
        html = fetch_text(url)
    except Exception:
        return None
    if 'id="linelist"' not in html:
        return None
    return html


def parse_release_summary_html(html: str) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    pattern = re.compile(
        r'<tr[^>]*data-line="([^"]+)"[^>]*data-robotid="([^"]*)"[^>]*>(.*?)</tr>',
        re.S,
    )
    for line, robot_id, row_html in pattern.findall(html):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.S)
        if len(cells) < 5:
            continue
        rows[line] = {
            "line": line,
            "robot_id": robot_id.strip(),
            "expressed_in_text": strip_html(cells[1]),
            "genotype_text": strip_html(cells[2]),
            "ad_text": strip_html(cells[3]),
            "dbd_text": strip_html(cells[4]),
        }
    return rows


@dataclass
class ReleasePlan:
    release: str
    source_kind: str
    source_locator: str
    source_token: str
    manifest_object: dict[str, str] | None = None
    metadata_objects: list[dict[str, str]] | None = None
    line_prefixes: list[str] | None = None
    html_summary: dict[str, dict[str, str]] | None = None
    publication_json: Any = None


def metadata_objects_for_prefix(prefix: str) -> list[dict[str, str]]:
    contents, _ = s3_list_all(prefix=prefix)
    return [item for item in contents if item["key"].endswith("-metadata.json")]


def collect_line_metadata_objects(prefixes: list[str], workers: int) -> list[dict[str, str]]:
    if not prefixes:
        return []
    workers = max(1, workers)
    if workers == 1:
        metadata_objects: list[dict[str, str]] = []
        for prefix in prefixes:
            metadata_objects.extend(metadata_objects_for_prefix(prefix))
        return metadata_objects
    metadata_objects = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for objects in executor.map(metadata_objects_for_prefix, prefixes):
            metadata_objects.extend(objects)
    return metadata_objects


def plan_release(release: str, include_html_fallback: bool = True, workers: int = DEFAULT_WORKERS) -> ReleasePlan:
    manifest_object = find_release_manifest_object(release)
    if manifest_object is not None:
        return ReleasePlan(
            release=release,
            source_kind="manifest",
            source_locator=manifest_object["key"],
            source_token=f"manifest:{manifest_object['key']}:{manifest_object['last_modified']}",
            manifest_object=manifest_object,
        )

    _, line_prefixes = s3_list_all(prefix=f"{release}/", delimiter="/")
    line_prefixes = sorted(line_prefixes)
    metadata_objects = collect_line_metadata_objects(line_prefixes, workers)

    html_summary = None
    html_token = ""
    if include_html_fallback:
        html = fetch_release_summary_html(release)
        if html:
            html_summary = parse_release_summary_html(html)
            html_token = md5_text(html)

    if metadata_objects:
        metadata_objects.sort(key=lambda item: item["key"])
        latest = max(item["last_modified"] for item in metadata_objects)
        token = f"line-metadata:{len(line_prefixes)}:{len(metadata_objects)}:{latest}:{html_token}"
        return ReleasePlan(
            release=release,
            source_kind="line-metadata",
            source_locator=f"{release}/",
            source_token=token,
            metadata_objects=metadata_objects,
            line_prefixes=line_prefixes,
            html_summary=html_summary,
        )

    if html_summary:
        token = f"cgi-html:{len(html_summary)}:{html_token}"
        return ReleasePlan(
            release=release,
            source_kind="cgi-html",
            source_locator=release,
            source_token=token,
            line_prefixes=line_prefixes,
            html_summary=html_summary,
        )

    return ReleasePlan(
        release=release,
        source_kind="empty",
        source_locator=release,
        source_token="empty",
        line_prefixes=line_prefixes,
    )


def connect_db(path: Path) -> sqlite3.Connection:
    ensure_parent(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, spec: str) -> None:
    existing = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table})")
    }
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {spec}")


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS releases (
          name TEXT PRIMARY KEY,
          manifest_key TEXT NOT NULL,
          manifest_url TEXT NOT NULL,
          publication_json TEXT,
          line_count INTEGER NOT NULL,
          image_count INTEGER NOT NULL,
          synced_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS line_releases (
          release TEXT NOT NULL,
          line TEXT NOT NULL,
          image_count INTEGER NOT NULL,
          sample_count INTEGER NOT NULL,
          annotations_text TEXT NOT NULL,
          rois_text TEXT NOT NULL,
          robot_ids_text TEXT NOT NULL,
          PRIMARY KEY (release, line),
          FOREIGN KEY (release) REFERENCES releases(name) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS images (
          image_id INTEGER PRIMARY KEY,
          release TEXT NOT NULL,
          line TEXT NOT NULL,
          robot_id TEXT,
          slide_code TEXT,
          objective TEXT,
          area TEXT,
          tile TEXT,
          gender TEXT,
          roi TEXT,
          annotations_text TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          FOREIGN KEY (release) REFERENCES releases(name) ON DELETE CASCADE
        );
        """
    )
    ensure_column(conn, "releases", "source_kind", "source_kind TEXT NOT NULL DEFAULT 'manifest'")
    ensure_column(conn, "releases", "source_locator", "source_locator TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "releases", "source_token", "source_token TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "line_releases", "expressed_in_text", "expressed_in_text TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "line_releases", "genotype_text", "genotype_text TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "line_releases", "ad_text", "ad_text TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "line_releases", "dbd_text", "dbd_text TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "images", "metadata_key", "metadata_key TEXT")
    ensure_column(conn, "images", "metadata_url", "metadata_url TEXT")
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_line_releases_line ON line_releases(line);
        CREATE INDEX IF NOT EXISTS idx_images_line ON images(line);
        CREATE INDEX IF NOT EXISTS idx_images_release_line ON images(release, line);
        CREATE INDEX IF NOT EXISTS idx_images_roi ON images(roi);
        """
    )


@dataclass
class LineAggregate:
    image_count: int = 0
    slide_codes: set[str] | None = None
    annotations: set[str] | None = None
    rois: set[str] | None = None
    robot_ids: set[str] | None = None
    expressed_in_text: str = ""
    genotype_text: str = ""
    ad_text: str = ""
    dbd_text: str = ""

    def __post_init__(self) -> None:
        self.slide_codes = set()
        self.annotations = set()
        self.rois = set()
        self.robot_ids = set()

    def merge_payload(self, payload: dict[str, Any]) -> None:
        self.image_count += 1
        slide_code = str(payload.get("slide_code", "") or "").strip()
        if slide_code:
            self.slide_codes.add(slide_code)
        for ann in normalize_annotations(payload.get("annotations")):
            self.annotations.add(ann)
        roi = str(payload.get("roi", "") or "").strip()
        if roi:
            self.rois.add(roi)
            if not self.expressed_in_text:
                self.expressed_in_text = roi
        robot_id = str(payload.get("robot_id", "") or "").strip()
        if robot_id:
            self.robot_ids.add(robot_id)
        if not self.genotype_text:
            self.genotype_text = str(payload.get("genotype", "") or "").strip()
        if not self.ad_text:
            self.ad_text = str(payload.get("ad", "") or "").strip()
        if not self.dbd_text:
            self.dbd_text = str(payload.get("dbd", "") or "").strip()

    def merge_html(self, summary: dict[str, str]) -> None:
        robot_id = summary.get("robot_id", "").strip()
        if robot_id:
            self.robot_ids.add(robot_id)
        if summary.get("expressed_in_text") and not self.expressed_in_text:
            self.expressed_in_text = summary["expressed_in_text"]
        if summary.get("genotype_text") and not self.genotype_text:
            self.genotype_text = summary["genotype_text"]
        if summary.get("ad_text") and not self.ad_text:
            self.ad_text = summary["ad_text"]
        if summary.get("dbd_text") and not self.dbd_text:
            self.dbd_text = summary["dbd_text"]


def build_image_row(
    release: str,
    line: str,
    payload: dict[str, Any],
    metadata_key: str | None = None,
) -> tuple[Any, ...]:
    annotations = normalize_annotations(payload.get("annotations"))
    return (
        extract_image_id(payload, metadata_key or f"{release}/{line}/manifest"),
        release,
        line,
        str(payload.get("robot_id", "") or "").strip() or None,
        str(payload.get("slide_code", "") or "").strip() or None,
        str(payload.get("objective", "") or "").strip() or None,
        str(payload.get("area", "") or "").strip() or None,
        str(payload.get("tile", "") or "").strip() or None,
        str(payload.get("gender", "") or "").strip() or None,
        str(payload.get("roi", "") or "").strip() or None,
        " | ".join(sorted(set(annotations))),
        metadata_key,
        s3_url_for_key(metadata_key) if metadata_key else None,
        json_dumps(payload),
    )


def store_release(
    conn: sqlite3.Connection,
    plan: ReleasePlan,
    release_data: dict[str, Any],
    raw_dir: Path | None,
) -> dict[str, Any]:
    release = plan.release
    images: list[dict[str, Any]] = release_data.get("images", [])
    line_names: list[str] = release_data.get("lines", [])
    publication_json = release_data.get("publication")
    manifest_payload = release_data.get("manifest_payload")

    if raw_dir is not None and manifest_payload is not None:
        raw_path = raw_dir / f"{safe_slug(release)}.json"
        ensure_parent(raw_path)
        raw_path.write_text(json_dumps(manifest_payload), encoding="utf-8")

    aggregates: dict[str, LineAggregate] = defaultdict(LineAggregate)
    image_rows: list[tuple[Any, ...]] = []

    for item in images:
        line = str(item.get("line", "")).strip()
        if not line:
            continue
        payload = dict(item)
        metadata_key = payload.pop("_metadata_key", None)
        aggregate = aggregates[line]
        aggregate.merge_payload(payload)
        image_rows.append(build_image_row(release, line, payload, metadata_key))

    for line in line_names:
        line_name = str(line).strip()
        if line_name and line_name not in aggregates:
            aggregates[line_name] = LineAggregate()

    if plan.html_summary:
        for line_name, summary in plan.html_summary.items():
            aggregates[line_name].merge_html(summary)

    line_rows = []
    for line_name, aggregate in sorted(aggregates.items()):
        line_rows.append(
            (
                release,
                line_name,
                aggregate.image_count,
                len(aggregate.slide_codes),
                " | ".join(sorted(aggregate.annotations)),
                " | ".join(sorted(aggregate.rois)),
                " | ".join(sorted(aggregate.robot_ids)),
                aggregate.expressed_in_text,
                aggregate.genotype_text,
                aggregate.ad_text,
                aggregate.dbd_text,
            )
        )

    manifest_key = plan.manifest_object["key"] if plan.manifest_object else plan.source_locator
    manifest_url = s3_url_for_key(manifest_key) if plan.manifest_object else ""
    publication_blob = json_dumps(publication_json)

    with conn:
        conn.execute(
            """
            INSERT INTO releases(
              name, manifest_key, manifest_url, publication_json, line_count, image_count, synced_at,
              source_kind, source_locator, source_token
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
              manifest_key=excluded.manifest_key,
              manifest_url=excluded.manifest_url,
              publication_json=excluded.publication_json,
              line_count=excluded.line_count,
              image_count=excluded.image_count,
              synced_at=excluded.synced_at,
              source_kind=excluded.source_kind,
              source_locator=excluded.source_locator,
              source_token=excluded.source_token
            """,
            (
                release,
                manifest_key,
                manifest_url,
                publication_blob,
                len(line_rows),
                len(image_rows),
                now_iso(),
                plan.source_kind,
                plan.source_locator,
                plan.source_token,
            ),
        )
        conn.execute("DELETE FROM line_releases WHERE release = ?", (release,))
        conn.execute("DELETE FROM images WHERE release = ?", (release,))
        conn.executemany(
            """
            INSERT INTO line_releases(
              release, line, image_count, sample_count, annotations_text, rois_text, robot_ids_text,
              expressed_in_text, genotype_text, ad_text, dbd_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            line_rows,
        )
        conn.executemany(
            """
            INSERT INTO images(
              image_id, release, line, robot_id, slide_code, objective, area, tile, gender, roi,
              annotations_text, metadata_key, metadata_url, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            image_rows,
        )

    return {
        "release": release,
        "source_kind": plan.source_kind,
        "lines": len(line_rows),
        "images": len(image_rows),
    }


def sync_release_from_plan(
    conn: sqlite3.Connection,
    plan: ReleasePlan,
    raw_dir: Path | None,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, Any]:
    if plan.source_kind == "manifest":
        manifest = fetch_json(s3_url_for_key(plan.manifest_object["key"]))
        if not isinstance(manifest, dict):
            raise SystemExit(f"unexpected manifest shape for {plan.release}")
        release_data = {
            "lines": manifest.get("lines", []),
            "images": manifest.get("images", []),
            "publication": manifest.get("publication"),
            "manifest_payload": manifest,
        }
        return store_release(conn, plan, release_data, raw_dir)

    if plan.source_kind == "line-metadata":
        def fetch_metadata_payload(item: dict[str, str]) -> dict[str, Any] | None:
            payload = fetch_json(s3_url_for_key(item["key"]))
            if not isinstance(payload, dict):
                return None
            line = str(payload.get("publishing_name", "") or "").strip()
            if not line:
                parts = item["key"].split("/")
                line = parts[1] if len(parts) > 1 else ""
            payload["line"] = line
            payload["_metadata_key"] = item["key"]
            return payload

        workers = max(1, workers)
        metadata_items = plan.metadata_objects or []
        if workers == 1:
            images = [payload for item in metadata_items if (payload := fetch_metadata_payload(item)) is not None]
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                images = [payload for payload in executor.map(fetch_metadata_payload, metadata_items) if payload is not None]
        lines = [prefix.rstrip("/").split("/")[-1] for prefix in (plan.line_prefixes or [])]
        release_data = {
            "lines": sorted(set(lines)),
            "images": images,
            "publication": None,
            "manifest_payload": {"lines": sorted(set(lines)), "images": images},
        }
        return store_release(conn, plan, release_data, raw_dir)

    if plan.source_kind == "cgi-html":
        lines = sorted((plan.html_summary or {}).keys())
        release_data = {"lines": lines, "images": [], "publication": None, "manifest_payload": None}
        return store_release(conn, plan, release_data, raw_dir)

    return {"release": plan.release, "source_kind": "empty", "lines": 0, "images": 0}


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
            print(
                "\t".join(
                    [
                        row["release"],
                        row["source_kind"],
                        row["manifest_url"] or "NO_MANIFEST",
                    ]
                )
            )
    return 0


def should_skip_incremental(conn: sqlite3.Connection, release: str, token: str) -> bool:
    row = conn.execute("SELECT source_token FROM releases WHERE name = ?", (release,)).fetchone()
    return row is not None and row["source_token"] == token


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
            print(
                f"{item['release']}\tkind={item['source_kind']}\tlines={item['lines']}\timages={item['images']}"
            )
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
        fields = [
            row["line"],
            row["release"],
            f"images={row['image_count']}",
            f"samples={row['sample_count']}",
        ]
        if row["expressed_in_text"]:
            fields.append(row["expressed_in_text"])
        print("\t".join(fields))
    return 0


def asset_urls_from_image(release: str, line: str, payload: dict[str, Any]) -> list[str]:
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
            SELECT image_id, release, line, robot_id, slide_code, objective, area, tile, gender, roi,
                   annotations_text, metadata_key, metadata_url, raw_json
            FROM images
            WHERE release = ? AND line = ?
            ORDER BY slide_code, image_id
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
        result.append(row)
    return result


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
    record = dict(row)
    record["images"] = get_image_records(conn, release, line, include_raw=include_raw)
    return record


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
        "releases": [
            get_line_record(conn, item["release"], item["line"], include_raw=args.raw)
            for item in matches
        ],
    }
    print(json.dumps(result, indent=2))
    return 0


def write_ndjson(rows: list[dict[str, Any]], out: TextIO) -> None:
    for row in rows:
        out.write(json.dumps(row, ensure_ascii=True) + "\n")


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
            payload = [
                get_line_record(conn, row["release"], row["line"], include_raw=args.raw)
                for row in rows
            ]
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


if __name__ == "__main__":
    raise SystemExit(main())
