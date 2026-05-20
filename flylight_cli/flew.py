from __future__ import annotations

import re
from html import unescape
from typing import Any
from urllib.parse import urlencode, urljoin

from .cache import get_cache_options, load_cached_bytes
from .text_utils import md5_text, safe_slug, strip_html


FLEW_INDEX_URL = "https://flweb.janelia.org/cgi-bin/flew.cgi"
FLEW_IMAGERY_URL = "https://flweb.janelia.org/cgi-bin/view_flew_imagery.cgi"
FLEW_RELEASE = "FlyLight GAL4/LexA Collection"


def parse_flew_catalog_html(html: str) -> list[str]:
    match = re.search(r'<select[^>]*name="line"[^>]*>.*?</select>', html, flags=re.S)
    if not match:
        return []
    lines = []
    seen = set()
    for encoded in re.findall(r'<option[^>]*value="([^"]+)"', match.group(0), flags=re.S):
        line = unescape(encoded).strip()
        if not line or line in seen:
            continue
        seen.add(line)
        lines.append(line)
    return lines


def flew_imagery_url(line: str) -> str:
    return f"{FLEW_IMAGERY_URL}?{urlencode({'line': line})}"


def cached_flew_page_hashes(lines: list[str]) -> dict[str, str]:
    cache_dir = get_cache_options().cache_dir
    page_hashes = {}
    for line in lines:
        payload = load_cached_bytes(flew_imagery_url(line), cache_dir=cache_dir)
        if payload is not None:
            page_hashes[line] = md5_text(payload.decode("utf-8", errors="replace"))
    return page_hashes


def flew_source_token(catalog_hash: str, lines: list[str], page_hashes: dict[str, str] | None = None) -> str:
    page_hashes = page_hashes if page_hashes is not None else cached_flew_page_hashes(lines)
    missing = sorted(set(lines) - set(page_hashes))
    parts = [f"catalog={catalog_hash}", f"lines={len(lines)}"]
    if missing:
        parts.append(f"missing={len(missing)}")
    else:
        digest_source = "\n".join(f"{line}\t{page_hashes[line]}" for line in sorted(lines))
        parts.append(f"pages={md5_text(digest_source)}")
    return "flew-html:" + ":".join(parts)


def parse_html_table_pairs(html: str) -> dict[str, str]:
    pairs = {}
    pattern = re.compile(r"<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*</tr>", re.S)
    for raw_key, raw_value in pattern.findall(html):
        key = strip_html(raw_key)
        value = strip_html(raw_value)
        if key and value:
            pairs[key] = value
    return pairs


def parse_summary_table_pairs(html: str) -> dict[str, str]:
    pairs = {}
    tables = re.findall(r'<table[^>]*class="summary"[^>]*>.*?</table>', html, re.S)
    for table in tables:
        pairs.update(parse_html_table_pairs(table))
    return pairs


def parse_flew_title_parts(title: str) -> tuple[str, str | None]:
    match = re.match(r"(.+?)\s*\(([^)]+)\)\s*$", title)
    if not match:
        return title, None
    return match.group(1).strip(), match.group(2).strip()


def parse_flew_urls(html: str) -> list[str]:
    urls = []
    seen = set()
    for raw_url in re.findall(r"https?://[^'\"<> ]+", html):
        url = unescape(raw_url).strip()
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def parse_flew_line_html(line: str, html: str) -> dict[str, Any]:
    summary_match = re.search(r'<div id="summaryarea"[^>]*>(.*?)<div style="clear:both;">&nbsp;</div></div>', html, re.S)
    line_summary = parse_summary_table_pairs(summary_match.group(1)) if summary_match else {}
    robot_id = line_summary.get("Robot ID", "")
    gene = line_summary.get("Gene", "")
    publication = line_summary.get("Publication", "")

    images = []
    sections = re.findall(r'<div class="boxed"[^>]*>.*?(?=<div class="boxed"|<div id="footer"|</body>)', html, re.S)
    for section in sections:
        title_match = re.search(r"<h3[^>]*>(.*?)</h3>", section, re.S)
        if not title_match:
            continue
        title = strip_html(title_match.group(1))
        if title == "Gene map":
            continue
        urls = parse_flew_urls(section)
        if not urls:
            continue
        area, driver = parse_flew_title_parts(title)
        section_id_match = re.search(r"toggleVis\(&quot;([^&]+)&quot;\)", section)
        section_id = section_id_match.group(1) if section_id_match else safe_slug(title)
        image_id_match = re.search(r"(\d+)", section_id)
        download_id_match = re.search(r"download\.cgi\?id=(\d+)", unescape(section))
        image_id = int((download_id_match or image_id_match).group(1)) if (download_id_match or image_id_match) else None
        section_summary = parse_summary_table_pairs(section)
        annotations = [f"{key}: {value}" for key, value in {**line_summary, **section_summary}.items()]
        download_url = None
        download_match = re.search(r"download\.cgi\?id=\d+[^'\"&<]*", unescape(section))
        if download_match:
            download_url = urljoin(FLEW_IMAGERY_URL, download_match.group(0))
        payload = {
            "line": line,
            "id": image_id,
            "section_id": section_id,
            "title": title,
            "area": area,
            "roi": area,
            "driver": driver,
            "robot_id": robot_id,
            "gene": gene,
            "publication": publication,
            "gender": section_summary.get("Gender", ""),
            "annotations": annotations,
            "urls": urls,
            "download_url": download_url,
            "line_summary": line_summary,
            "section_summary": section_summary,
            "_metadata_key": f"{flew_imagery_url(line)}#{section_id}",
        }
        if image_id is None:
            payload.pop("id")
        images.append(payload)

    return {"line": line, "line_summary": line_summary, "images": images}
