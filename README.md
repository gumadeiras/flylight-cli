# flylight-cli

Agent-friendly local index for Janelia FlyLight Split-GAL4 resources.

Surfaces used:

- CGI site: [splitgal4.janelia.org](https://splitgal4.janelia.org/cgi-bin/splitgal4.cgi)
- Public bucket: [janelia-flylight-imagery](https://s3.amazonaws.com/janelia-flylight-imagery/)
- Bucket docs: [README.md](https://s3.amazonaws.com/janelia-flylight-imagery/README.md)

Source order:

1. release manifest json
2. per-line/per-image S3 metadata json
3. CGI release summary html

## CLI

Install:

```bash
pip install -e .
```

Entry:

```bash
flylight --help
python3 janelia_splitgal4.py
```

Examples:

```bash
flylight releases
flylight sync --release 'MB Paper 2014'
flylight sync --all
flylight sync --all --force
flylight sync --all --offline
flylight sync --all --refresh-cache
flylight sync --release 'Descending Neurons 2018' --workers 8
flylight cache-info
flylight search --expressed-in DNp04 --ad 31B08 --source-kind line-metadata
flylight search-text 'DNp04 AND 31B08'
flylight search-images --area Brain --objective 20x --robot-id 3007645
flylight show-line SS00724 --release 'Descending Neurons 2018'
flylight show-image 6878306
flylight compare-line MB005B
flylight compare-release 'MB Paper 2014' 'MB Paper 2015'
flylight show-release 'MB Paper 2014' --include-lines --genotype 34A03
flylight stats
flylight export-ndjson --entity line --release 'Descending Neurons 2018'
flylight export-ndjson --entity image --term MB005B --out data/mb005b.ndjson
flylight export-ndjson --entity release
```

## Notes

- HTTP fetches are cache-first by default; cached responses are reused until you pass `--refresh-cache`.
- `--offline` disables network access and uses cached HTTP responses only.
- cache path: `data/http_cache`
- `sync --all` is incremental by default; unchanged releases skip.
- missing release manifest: fallback walks line dirs + metadata jsons.
- CGI summary enriches line-level fields like expressed-in, genotype, AD, DBD.
- line/image exports include normalized arrays alongside text fields.
- `search` supports field filters over line metadata: AD, DBD, genotype, expressed-in, robot-id, source-kind.
- `search-text` uses SQLite FTS for faster boolean/full-text matching over line text fields.
- `search-images` supports field filters over image metadata: area, objective, gender, robot-id, roi.
- `compare-line` shows shared fields for the same line across synced releases.
- `compare-release` summarizes added, removed, changed, and unchanged lines between two synced releases.
- local db path: `data/janelia_splitgal4.sqlite`
- raw manifest cache: `data/raw_manifests/*.json`
