from __future__ import annotations

import hashlib
import re
from html import unescape


def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def safe_slug(text: str) -> str:
    out = []
    for ch in text.lower():
        out.append(ch if ch.isalnum() else "_")
    slug = "".join(out).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "release"


def strip_html(raw: str) -> str:
    text = re.sub(r"<[^>]+>", "", raw)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()
