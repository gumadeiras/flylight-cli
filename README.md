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

Entry:

```bash
python3 janelia_splitgal4.py
```

Examples:

```bash
python3 janelia_splitgal4.py releases
python3 janelia_splitgal4.py sync --release 'MB Paper 2014'
python3 janelia_splitgal4.py sync --all
python3 janelia_splitgal4.py sync --all --force
python3 janelia_splitgal4.py search --term DNg14
python3 janelia_splitgal4.py show-line SS00724 --release 'Descending Neurons 2018'
python3 janelia_splitgal4.py export-ndjson --entity line --release 'Descending Neurons 2018'
python3 janelia_splitgal4.py export-ndjson --entity image --term MB005B --out data/mb005b.ndjson
```

## Notes

- `sync --all` is incremental by default; unchanged releases skip.
- missing release manifest: fallback walks line dirs + metadata jsons.
- CGI summary enriches line-level fields like expressed-in, genotype, AD, DBD.
- local db path: `data/janelia_splitgal4.sqlite`
- raw manifest cache: `data/raw_manifests/*.json`
