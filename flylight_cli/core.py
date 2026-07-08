from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, unquote_plus, urlencode
import xml.etree.ElementTree as ET

from . import __version__
from .cache import OfflineCacheMiss, fetch_bytes as cached_fetch_bytes
from .db import connect_db, ensure_parent, refresh_release_fts
from .flew import (
    FLEW_INDEX_URL,
    FLEW_RELEASE,
    flew_imagery_url,
    flew_source_token,
    parse_flew_catalog_html,
    parse_flew_line_html,
)
from .normalize import extract_em_cell_type_terms, normalize_image_record, normalize_line_record, normalize_release_record
from .paths import default_data_dir
from .records import asset_urls_from_image, get_db_stats, get_image_record, get_image_records, get_line_record, get_release_record, get_release_records
from .text_utils import md5_text, safe_slug, strip_html


BUCKET = "janelia-flylight-imagery"
S3_HTTP_ROOT = f"https://s3.amazonaws.com/{BUCKET}"
S3_LIST_ROOT = f"{S3_HTTP_ROOT}/"
SPLITGAL4_INDEX_URL = "https://splitgal4.janelia.org/cgi-bin/splitgal4.cgi"
SPLITGAL4_SUMMARY_URL = "https://splitgal4.janelia.org/cgi-bin/splitgal4_summary.cgi"
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
USER_AGENT = f"flylight/{__version__}"
DEFAULT_DATA_DIR = default_data_dir()
DEFAULT_DB = DEFAULT_DATA_DIR / "janelia_splitgal4.sqlite"
DEFAULT_RAW_DIR = DEFAULT_DATA_DIR / "raw_manifests"
DEFAULT_WORKERS = 12
ProgressCallback = Callable[[str], None]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_bytes(url: str) -> bytes:
    return cached_fetch_bytes(url, USER_AGENT)


def fetch_text(url: str) -> str:
    return fetch_bytes(url).decode("utf-8", errors="replace")


def fetch_json(url: str) -> Any:
    return json.loads(fetch_text(url))


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


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
    return [*parse_release_catalog_html(fetch_text(SPLITGAL4_INDEX_URL)), FLEW_RELEASE]


def parse_release_catalog_html(html: str) -> list[str]:
    releases = []
    seen = set()
    pattern = re.compile(r"splitgal4_summary\.cgi\?_gsearch=Search&amp;alps_release=([^\"']+)|splitgal4_summary\.cgi\?_gsearch=Search&alps_release=([^\"']+)")
    for encoded_left, encoded_right in pattern.findall(html):
        encoded = encoded_left or encoded_right
        release = unquote_plus(unescape(encoded))
        release = release.strip()
        if not release or release in seen:
            continue
        seen.add(release)
        releases.append(release)
    return releases


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


def fetch_release_summary_html(release: str) -> str | None:
    url = release_summary_url(release)
    try:
        html = fetch_text(url)
    except Exception:
        return None
    if 'id="linelist"' not in html:
        return None
    return html


def release_summary_url(release: str) -> str:
    return f"{SPLITGAL4_SUMMARY_URL}?{urlencode({'_gsearch': 'Search', 'alps_release': release})}"


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
    flew_lines: list[str] | None = None
    flew_catalog_hash: str | None = None
    html_summary: dict[str, dict[str, str]] | None = None
    publication_json: Any = None


def plan_flew_catalog() -> ReleasePlan:
    html = fetch_text(FLEW_INDEX_URL)
    lines = parse_flew_catalog_html(html)
    catalog_hash = md5_text(html)
    return ReleasePlan(
        release=FLEW_RELEASE,
        source_kind="flew-html",
        source_locator=FLEW_INDEX_URL,
        source_token=flew_source_token(catalog_hash, lines),
        flew_lines=lines,
        flew_catalog_hash=catalog_hash,
    )


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
    if release == FLEW_RELEASE:
        return plan_flew_catalog()

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


@dataclass
class LineAggregate:
    image_count: int = 0
    slide_codes: set[str] | None = None
    annotations: set[str] | None = None
    rois: set[str] | None = None
    robot_ids: set[str] | None = None
    em_cell_types: set[str] | None = None
    expressed_in_text: str = ""
    genotype_text: str = ""
    ad_text: str = ""
    dbd_text: str = ""

    def __post_init__(self) -> None:
        self.slide_codes = set()
        self.annotations = set()
        self.rois = set()
        self.robot_ids = set()
        self.em_cell_types = set()

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
        for term in extract_em_cell_type_terms(payload):
            self.em_cell_types.add(term)
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

    def merge_flew_summary(self, summary: dict[str, str]) -> None:
        robot_id = summary.get("Robot ID", "").strip()
        if robot_id:
            self.robot_ids.add(robot_id)
        for key, value in summary.items():
            clean_value = value.strip()
            if clean_value:
                self.annotations.add(f"{key}: {clean_value}")


def build_image_row(
    release: str,
    line: str,
    payload: dict[str, Any],
    metadata_key: str | None = None,
) -> tuple[Any, ...]:
    annotations = normalize_annotations(payload.get("annotations"))
    em_cell_types = extract_em_cell_type_terms(payload)
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
        " | ".join(em_cell_types),
        metadata_key,
        metadata_key if metadata_key and metadata_key.startswith(("http://", "https://")) else s3_url_for_key(metadata_key) if metadata_key else None,
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
    line_summaries: dict[str, dict[str, str]] = release_data.get("line_summaries", {})

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
        aggregates[line].merge_payload(payload)
        image_rows.append(build_image_row(release, line, payload, metadata_key))

    for line in line_names:
        line_name = str(line).strip()
        if line_name and line_name not in aggregates:
            aggregates[line_name] = LineAggregate()

    for line_name, summary in line_summaries.items():
        aggregates[line_name].merge_flew_summary(summary)

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
                " | ".join(sorted(aggregate.em_cell_types)),
            )
        )

    manifest_key = plan.manifest_object["key"] if plan.manifest_object else plan.source_locator
    manifest_url = s3_url_for_key(manifest_key) if plan.manifest_object else ""

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
                json_dumps(publication_json),
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
              expressed_in_text, genotype_text, ad_text, dbd_text, em_cell_types_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            line_rows,
        )
        conn.executemany(
            """
            INSERT INTO images(
              image_id, release, line, robot_id, slide_code, objective, area, tile, gender, roi,
              annotations_text, em_cell_types_text, metadata_key, metadata_url, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            image_rows,
        )
        refresh_release_fts(conn, release)

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
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    def report(message: str) -> None:
        if progress is not None:
            progress(message)

    if plan.source_kind == "manifest":
        report(f"{plan.release}: fetching manifest")
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
        report(f"{plan.release}: fetching {len(metadata_items)} metadata files with {workers} workers")
        if workers == 1:
            images = []
            for index, item in enumerate(metadata_items, start=1):
                payload = fetch_metadata_payload(item)
                if payload is not None:
                    images.append(payload)
                if index % 500 == 0 or index == len(metadata_items):
                    report(f"{plan.release}: fetched {index}/{len(metadata_items)} metadata files")
        else:
            images = []
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for index, payload in enumerate(executor.map(fetch_metadata_payload, metadata_items), start=1):
                    if payload is not None:
                        images.append(payload)
                    if index % 500 == 0 or index == len(metadata_items):
                        report(f"{plan.release}: fetched {index}/{len(metadata_items)} metadata files")
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
        report(f"{plan.release}: storing {len(lines)} CGI summary lines")
        release_data = {"lines": lines, "images": [], "publication": None, "manifest_payload": None}
        return store_release(conn, plan, release_data, raw_dir)

    if plan.source_kind == "flew-html":
        def fetch_flew_payload(line: str) -> dict[str, Any]:
            html = fetch_text(flew_imagery_url(line))
            payload = parse_flew_line_html(line, html)
            payload["_page_hash"] = md5_text(html)
            return payload

        def fetch_flew_result(line: str) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
            try:
                return fetch_flew_payload(line), None
            except Exception as exc:
                return None, {"line": line, "error": str(exc)}

        lines = sorted(set(plan.flew_lines or []))
        workers = max(1, workers)
        line_payloads = []
        line_errors = []
        report(f"{plan.release}: fetching {len(lines)} FLEW line pages with {workers} workers")
        if workers == 1:
            for index, line in enumerate(lines, start=1):
                payload, error = fetch_flew_result(line)
                if payload is not None:
                    line_payloads.append(payload)
                if error is not None:
                    line_errors.append(error)
                if index % 500 == 0 or index == len(lines):
                    report(f"{plan.release}: fetched {index}/{len(lines)} FLEW pages, errors={len(line_errors)}")
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for index, (payload, error) in enumerate(executor.map(fetch_flew_result, lines), start=1):
                    if payload is not None:
                        line_payloads.append(payload)
                    if error is not None:
                        line_errors.append(error)
                    if index % 500 == 0 or index == len(lines):
                        report(f"{plan.release}: fetched {index}/{len(lines)} FLEW pages, errors={len(line_errors)}")
        if line_errors and not line_payloads:
            raise SystemExit(f"failed to fetch FLEW line pages; first error: {line_errors[0]['error']}")
        images = [image for payload in line_payloads for image in payload["images"]]
        page_hashes = {payload["line"]: payload["_page_hash"] for payload in line_payloads}
        catalog_hash = plan.flew_catalog_hash or plan.source_token
        source_token = flew_source_token(catalog_hash, lines, page_hashes)
        if line_errors:
            error_digest = md5_text(json_dumps(line_errors))
            source_token = f"{source_token}:errors={error_digest}"
        synced_plan = replace(plan, source_token=source_token)
        release_data = {
            "lines": lines,
            "images": images,
            "line_summaries": {payload["line"]: payload["line_summary"] for payload in line_payloads},
            "publication": None,
            "manifest_payload": {
                "lines": lines,
                "line_pages": line_payloads,
                "images": images,
                "line_errors": line_errors,
            },
        }
        result = store_release(conn, synced_plan, release_data, raw_dir)
        result["failed_lines"] = len(line_errors)
        return result

    return {"release": plan.release, "source_kind": "empty", "lines": 0, "images": 0}


def should_skip_incremental(conn: sqlite3.Connection, release: str, token: str) -> bool:
    row = conn.execute("SELECT source_token FROM releases WHERE name = ?", (release,)).fetchone()
    return row is not None and row["source_token"] == token
