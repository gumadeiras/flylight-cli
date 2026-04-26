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

Warm data:
- one release: `flylight sync --release 'MB Paper 2014'`
- all releases: `flylight sync --all`
- refresh cache while syncing: `flylight sync --all --refresh-cache`
- offline sync from warmed cache: `flylight sync --all --offline`

Portable offline bundle:
- export snapshot: `flylight snapshot-export --out data/flylight-snapshot.tar.gz`
- import snapshot: `flylight snapshot-import data/flylight-snapshot.tar.gz --force`

Best query surfaces:
- line filters: `flylight search ...`
- full-text line search: `flylight search-text 'DNp04 AND 31B08'`
- image filters: `flylight search-images ...`
- line detail: `flylight show-line SS00724 --release 'Descending Neurons 2018'`
- image detail: `flylight show-image 6878306`
- release detail: `flylight show-release 'MB Paper 2014' --include-lines`
- line diff: `flylight compare-line MB005B`
- release diff: `flylight compare-release 'MB Paper 2014' 'MB Paper 2015'`

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
- `--raw` includes fuller embedded records/payloads when supported
- for repeated agent workflows, prefer NDJSON export over parsing human CLI output
