# Changelog

## Unreleased

### Changes

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
