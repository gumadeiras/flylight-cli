from __future__ import annotations

import json
import sqlite3
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .cache import cache_stats
from .db import ensure_parent


SNAPSHOT_MANIFEST = "snapshot_manifest.json"


def iter_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*") if path.is_file())


def build_snapshot_manifest(db_path: Path, raw_dir: Path, cache_dir: Path) -> dict[str, Any]:
    raw_files = iter_files(raw_dir)
    cache_info = cache_stats(cache_dir)
    db_exists = db_path.exists()
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "db_path": db_path.name,
        "db_present": db_exists,
        "db_bytes": db_path.stat().st_size if db_exists else 0,
        "raw_file_count": len(raw_files),
        "cache_entries": int(cache_info["entries"]),
        "cache_bytes": int(cache_info["bytes"]),
    }


def export_snapshot(archive_path: Path, db_path: Path, raw_dir: Path, cache_dir: Path) -> dict[str, Any]:
    manifest = build_snapshot_manifest(db_path, raw_dir, cache_dir)
    ensure_parent(archive_path)
    if db_path.exists():
        checkpoint_sqlite(db_path)
    with tarfile.open(archive_path, "w:gz") as tar:
        manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
        info = tarfile.TarInfo(SNAPSHOT_MANIFEST)
        info.size = len(manifest_bytes)
        info.mtime = int(datetime.now(timezone.utc).timestamp())
        tar.addfile(info, fileobj=_BytesReader(manifest_bytes))
        if db_path.exists():
            tar.add(db_path, arcname=f"db/{db_path.name}")
        for root_name, root_path in [("raw_manifests", raw_dir), ("http_cache", cache_dir)]:
            for path in iter_files(root_path):
                tar.add(path, arcname=f"{root_name}/{path.relative_to(root_path)}")
    return {**manifest, "archive_path": str(archive_path)}


def import_snapshot(
    archive_path: Path,
    db_path: Path,
    raw_dir: Path,
    cache_dir: Path,
    force: bool = False,
) -> dict[str, Any]:
    if not archive_path.exists():
        raise SystemExit(f"snapshot not found: {archive_path}")
    if db_path.exists() and not force:
        raise SystemExit(f"target db exists: {db_path}; rerun with --force to overwrite it")

    manifest: dict[str, Any] | None = None
    imported = {"db": False, "raw_files": 0, "cache_files": 0}

    with tarfile.open(archive_path, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            if member.name == SNAPSHOT_MANIFEST:
                extracted = tar.extractfile(member)
                if extracted is not None:
                    manifest = json.loads(extracted.read().decode("utf-8"))
                continue
            if member.name.startswith("db/"):
                extracted = tar.extractfile(member)
                if extracted is None:
                    continue
                ensure_parent(db_path)
                db_path.write_bytes(extracted.read())
                imported["db"] = True
                continue
            if member.name.startswith("raw_manifests/"):
                _extract_member(tar, member, raw_dir / member.name.removeprefix("raw_manifests/"))
                imported["raw_files"] += 1
                continue
            if member.name.startswith("http_cache/"):
                _extract_member(tar, member, cache_dir / member.name.removeprefix("http_cache/"))
                imported["cache_files"] += 1
                continue

    return {
        "archive_path": str(archive_path),
        "db_path": str(db_path),
        "raw_dir": str(raw_dir),
        "cache_dir": str(cache_dir),
        "imported": imported,
        "manifest": manifest,
    }


def checkpoint_sqlite(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()


def _extract_member(tar: tarfile.TarFile, member: tarfile.TarInfo, target: Path) -> None:
    extracted = tar.extractfile(member)
    if extracted is None:
        return
    ensure_parent(target)
    target.write_bytes(extracted.read())


class _BytesReader:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.payload) - self.offset
        start = self.offset
        end = min(len(self.payload), self.offset + size)
        self.offset = end
        return self.payload[start:end]
