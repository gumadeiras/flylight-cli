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
flylight sync --release 'Descending Neurons 2018' --workers 8
flylight search --term DNg14
flylight show-line SS00724 --release 'Descending Neurons 2018'
flylight stats
flylight export-ndjson --entity line --release 'Descending Neurons 2018'
flylight export-ndjson --entity image --term MB005B --out data/mb005b.ndjson
flylight export-ndjson --entity release
```

## Notes

- `sync --all` is incremental by default; unchanged releases skip.
- missing release manifest: fallback walks line dirs + metadata jsons.
- CGI summary enriches line-level fields like expressed-in, genotype, AD, DBD.
- line/image exports include normalized arrays alongside text fields.
- local db path: `data/janelia_splitgal4.sqlite`
- raw manifest cache: `data/raw_manifests/*.json`
