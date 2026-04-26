# AGENTS.md

Scope: this repo.

Purpose:
- sync, cache, query, diff Janelia FlyLight Split-GAL4 data
- prefer local/offline data once warmed
- emit NDJSON for agent consumption

Use:
- install: `pip install -e .`
- entrypoint: `flylight`

Fetch order:
1. release manifest json
2. per-line/per-image S3 metadata json
3. CGI release summary html

Default paths:
- sqlite db: `data/janelia_splitgal4.sqlite`
- HTTP cache: `data/http_cache`
- raw manifests: `data/raw_manifests`

Cache/offline:
- HTTP is cache-first by default
- use `--refresh-cache` to re-fetch upstream
- use `--offline` to forbid network and use cache only
- inspect cache: `flylight cache-info`
- dry-run sync: `flylight sync-plan --all`
- rebuild derived fields: `flylight reindex`

Warm data:
- one release: `flylight sync --release 'MB Paper 2014'`
- all releases: `flylight sync --all`
- inspect sync coverage first: `flylight sync-plan --all`
- refresh cache while syncing: `flylight sync --all --refresh-cache`
- offline sync from warmed cache: `flylight sync --all --offline`

Portable offline bundle:
- export snapshot: `flylight snapshot-export --out data/flylight-snapshot.tar.gz`
- import snapshot: `flylight snapshot-import data/flylight-snapshot.tar.gz --force`

Best query surfaces:
- line filters: `flylight search ...`
- EM term filter: `flylight search --em-cell-type EPG`
- full-text line search: `flylight search-text 'DNp04 AND 31B08'`
- image filters: `flylight search-images ...`
- line detail: `flylight show-line SS00724 --release 'Descending Neurons 2018'`
- image detail: `flylight show-image 6878306`
- release detail: `flylight show-release 'MB Paper 2014' --include-lines`
- line diff: `flylight compare-line MB005B`
- release diff: `flylight compare-release 'MB Paper 2014' 'MB Paper 2015'`

Example: find EPG lines, then get expression images
- exact EM-tagged lines: `flylight search --em-cell-type EPG`
- fuzzy family search: `flylight search-text 'EPG OR E-PG OR ellipsoid'`
- line detail with embedded images: `flylight show-line SS00090`
- pull image-level matches: `flylight search-images --em-cell-type EPG`
- extract representative 20x/63x PNG urls:
```bash
flylight show-line SS00090 \
  | jq -r '.releases[] | .images[] | select((.em_cell_types // []) | index("EPG")) | .asset_urls[] | select(test("signals_mip\\.png$"))'
```
- current exact `EPG` lines in full synced db: `SS00090`, `SS00098`
- representative 63x examples:
  - `https://s3.amazonaws.com/janelia-flylight-imagery/Wolff+et+al+2024/SS00090/SS00090-20130426_20_C2-f-63x-central-Split_GAL4-signals_mip.png`
  - `https://s3.amazonaws.com/janelia-flylight-imagery/Wolff+et+al+2024/SS00098/SS00098-20130419_31_B4-f-63x-brain-Split_GAL4-signals_mip.png`

Best agent export surfaces:
- lines: `flylight export-ndjson --entity line --release 'Descending Neurons 2018'`
- images: `flylight export-ndjson --entity image --term MB005B`
- releases: `flylight export-ndjson --entity release`
- line diffs: `flylight export-ndjson --entity compare-line --line MB005B`
- release diffs: `flylight export-ndjson --entity compare-release --left-release 'MB Paper 2014' --right-release 'MB Paper 2015'`
- schema introspection: `flylight schema --entity line`
- canned recipes: `flylight examples --topic release-diff`

Notes:
- normalized arrays are included in exports where relevant: annotations, rois, robot_ids, expressed_in, genotype/ad/dbd parts
- EM cell type terms are normalized into `em_cell_types` when upstream raw metadata includes `em_cell_type`
- `--raw` includes fuller embedded records/payloads when supported
- for repeated agent workflows, prefer NDJSON export over parsing human CLI output
- `sync-plan` is the best preflight for offline reuse; it shows cache coverage and whether incremental sync would skip or run
