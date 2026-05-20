# Changelog

## Unreleased

### Changes

- Added the FlyLight GAL4/LexA catalog and per-line imagery metadata from `flew.cgi` as a syncable `flew-html` source with page-aware incremental tokens.
- Added simpler `update`, `sources`, `find`, `images`, `line`, `image`, and `release` commands plus expanded examples.
- Made bare `flylight` print the main help menu and bare subcommands print command-specific help; `update` now requires explicit `--all`.
- Added stderr progress reporting for long `sync` and `update` runs without changing JSON stdout.
- Renamed the published PyPI and Homebrew package to `flylight`.
- Updated release automation to the current GitHub Actions checkout, Python setup, and release actions.
- Added a local release wrapper for version sync, package validation, tagging, and release workflow verification.

## 0.12.2 - 2026-05-05

Initial PyPI release.

### Features

- Added FlyLight Split-GAL4 sync CLI with manifest, fallback metadata, and cache-first sync support.
- Added installation packaging and CI build verification.
- Added normalized export records, stats, richer search, lookup commands, and full-text line search.
- Added cross-release line comparison and release comparison summaries.
- Added offline mode, portable offline snapshots, and compare-result NDJSON exports.
- Added schema introspection, canned agent workflow examples, sync planning, and cache coverage reporting.
- Added EM cell type annotation indexing for search.
- Added `--version` support for packaging.

### Fixes

- Hardened full sync release discovery and cache writes.

### Changes

- Documented advanced query, snapshot, diff export, schema, examples, sync planning, and EM cell type workflows.
