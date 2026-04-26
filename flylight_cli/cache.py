from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen


DEFAULT_CACHE_DIR = Path("data/http_cache")


@dataclass
class CacheOptions:
    cache_dir: Path = DEFAULT_CACHE_DIR
    offline: bool = False
    refresh: bool = False


class OfflineCacheMiss(RuntimeError):
    pass


_cache_options = CacheOptions()


def get_cache_options() -> CacheOptions:
    return _cache_options


def set_cache_options(
    cache_dir: Path | None = None,
    offline: bool | None = None,
    refresh: bool | None = None,
) -> CacheOptions:
    global _cache_options
    next_options = replace(_cache_options)
    if cache_dir is not None:
        next_options.cache_dir = cache_dir
    if offline is not None:
        next_options.offline = offline
    if refresh is not None:
        next_options.refresh = refresh
    if next_options.offline and next_options.refresh:
        raise ValueError("cannot combine offline mode with refresh-cache")
    _cache_options = next_options
    return _cache_options


def cache_path_for_url(url: str, cache_dir: Path | None = None) -> Path:
    cache_dir = cache_dir or _cache_options.cache_dir
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    suffix = Path(urlparse(url).path).suffix or ".bin"
    return cache_dir / digest[:2] / f"{digest}{suffix}"


def meta_path_for_cache(cache_path: Path) -> Path:
    return cache_path.with_suffix(cache_path.suffix + ".meta.json")


def ensure_cache_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_cached_bytes(url: str, cache_dir: Path | None = None) -> bytes | None:
    cache_path = cache_path_for_url(url, cache_dir=cache_dir)
    if not cache_path.exists():
        return None
    return cache_path.read_bytes()


def cached_at_for_path(cache_path: Path) -> str | None:
    meta_path = meta_path_for_cache(cache_path)
    if not meta_path.exists():
        return None
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    cached_at = meta.get("cached_at")
    if isinstance(cached_at, str) and cached_at:
        return cached_at
    return None


def cache_data_paths(cache_dir: Path) -> list[Path]:
    return [path for path in cache_dir.rglob("*") if path.is_file() and not path.name.endswith(".meta.json")]


def cache_entry_for_url(url: str, cache_dir: Path | None = None) -> dict[str, int | str] | None:
    cache_path = cache_path_for_url(url, cache_dir=cache_dir)
    if not cache_path.exists():
        return None
    payload: dict[str, int | str] = {
        "url": url,
        "cache_path": str(cache_path),
        "bytes": cache_path.stat().st_size,
        "suffix": cache_path.suffix,
    }
    cached_at = cached_at_for_path(cache_path)
    if cached_at:
        payload["cached_at"] = cached_at
    return payload


def write_cached_bytes(url: str, payload: bytes, cache_dir: Path | None = None) -> Path:
    cache_path = cache_path_for_url(url, cache_dir=cache_dir)
    ensure_cache_parent(cache_path)
    tmp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
    tmp_path.write_bytes(payload)
    tmp_path.replace(cache_path)
    meta_path_for_cache(cache_path).write_text(
        json.dumps(
            {
                "url": url,
                "cached_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "bytes": len(payload),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return cache_path


def fetch_bytes(url: str, user_agent: str) -> bytes:
    options = get_cache_options()
    cached = None if options.refresh else load_cached_bytes(url, cache_dir=options.cache_dir)
    if cached is not None:
        return cached
    if options.offline:
        raise OfflineCacheMiss(f"offline cache miss for {url}; rerun without --offline to warm the cache")
    req = Request(url, headers={"User-Agent": user_agent})
    with urlopen(req) as resp:
        payload = resp.read()
    write_cached_bytes(url, payload, cache_dir=options.cache_dir)
    return payload


def cache_stats(cache_dir: Path | None = None) -> dict[str, int | str]:
    cache_dir = cache_dir or _cache_options.cache_dir
    if not cache_dir.exists():
        return {
            "cache_dir": str(cache_dir),
            "entries": 0,
            "bytes": 0,
            "suffix_counts": {},
            "oldest_cached_at": None,
            "newest_cached_at": None,
        }
    paths = cache_data_paths(cache_dir)
    suffix_counts: dict[str, int] = {}
    cached_at_values: list[str] = []
    for path in paths:
        suffix = path.suffix or ".bin"
        suffix_counts[suffix] = suffix_counts.get(suffix, 0) + 1
        cached_at = cached_at_for_path(path)
        if cached_at:
            cached_at_values.append(cached_at)
    return {
        "cache_dir": str(cache_dir),
        "entries": len(paths),
        "bytes": sum(path.stat().st_size for path in paths),
        "suffix_counts": suffix_counts,
        "oldest_cached_at": min(cached_at_values) if cached_at_values else None,
        "newest_cached_at": max(cached_at_values) if cached_at_values else None,
    }
