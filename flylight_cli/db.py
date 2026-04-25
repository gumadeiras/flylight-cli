from __future__ import annotations

import sqlite3
from pathlib import Path


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_column(conn: sqlite3.Connection, table: str, column: str, spec: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
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
        CREATE VIRTUAL TABLE IF NOT EXISTS line_search_fts USING fts5(
          release UNINDEXED,
          line,
          annotations_text,
          rois_text,
          robot_ids_text,
          expressed_in_text,
          genotype_text,
          ad_text,
          dbd_text,
          tokenize='unicode61'
        );
        """
    )


def connect_db(path: Path) -> sqlite3.Connection:
    ensure_parent(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    return conn


def refresh_release_fts(conn: sqlite3.Connection, release: str) -> None:
    conn.execute("DELETE FROM line_search_fts WHERE release = ?", (release,))
    conn.execute(
        """
        INSERT INTO line_search_fts(
          release, line, annotations_text, rois_text, robot_ids_text,
          expressed_in_text, genotype_text, ad_text, dbd_text
        )
        SELECT release, line, annotations_text, rois_text, robot_ids_text,
               expressed_in_text, genotype_text, ad_text, dbd_text
        FROM line_releases
        WHERE release = ?
        """,
        (release,),
    )
