from __future__ import annotations


EXAMPLES = {
    "quick-start": {
        "description": "Use the shortest commands for the common loop: update, find, inspect images.",
        "commands": [
            "flylight update --all",
            "flylight sources --json",
            "flylight find DNp04",
            "flylight images DNp04",
            "flylight line SS00724",
        ],
    },
    "warm-offline": {
        "description": "Warm the local cache and then operate fully offline.",
        "commands": [
            "flylight update --all --refresh-cache",
            "flylight cache-info --json",
            "flylight update --all --offline",
            "flylight sources --offline --json",
        ],
    },
    "line-investigation": {
        "description": "Find a line, inspect its images, and export machine-readable rows.",
        "commands": [
            "flylight find SS00724",
            "flylight search --expressed-in DNp04 --ad 31B08 --source-kind line-metadata",
            "flylight search --source-kind flew-html --line R10A",
            "flylight show-line SS00724 --release 'Descending Neurons 2018'",
            "flylight export-ndjson --entity line --release 'Descending Neurons 2018' --line SS00724",
            "flylight export-ndjson --entity image --line SS00724 --raw",
        ],
    },
    "image-investigation": {
        "description": "Find images by line, anatomy, source, or image id.",
        "commands": [
            "flylight images MB005B",
            "flylight search-images --line MB005B --area Brain --objective 20x",
            "flylight search-images --source-kind flew-html --line R10A --area Brain",
            "flylight image 6878306",
            "flylight export-ndjson --entity image --line MB005B --out data/mb005b.ndjson",
        ],
    },
    "gal4-lexa": {
        "description": "Work with the FlyLight GAL4/LexA collection from flweb.",
        "commands": [
            "flylight sync --release 'FlyLight GAL4/LexA Collection'",
            "flylight find R10A --source-kind flew-html",
            "flylight images R10A01 --source-kind flew-html",
            "flylight search-images --source-kind flew-html --line R10A01 --area Brain",
        ],
    },
    "em-cell-types": {
        "description": "Search split lines and images by normalized EM cell type annotations.",
        "commands": [
            "flylight search --em-cell-type EPG",
            "flylight search-text 'EPG OR E-PG OR ellipsoid'",
            "flylight search-images --em-cell-type EPG",
            "flylight line SS00090",
        ],
    },
    "release-inspection": {
        "description": "Inspect one synced release and embed filtered line records.",
        "commands": [
            "flylight release 'MB Paper 2014'",
            "flylight show-release 'MB Paper 2014' --include-lines --genotype 34A03",
            "flylight export-ndjson --entity release",
            "flylight stats --json",
        ],
    },
    "release-diff": {
        "description": "Compare two releases and export the diff rows for agent ingest.",
        "commands": [
            "flylight compare-release 'MB Paper 2014' 'MB Paper 2015'",
            "flylight export-ndjson --entity compare-release --left-release 'MB Paper 2014' --right-release 'MB Paper 2015'",
            "flylight export-ndjson --entity compare-release --left-release 'MB Paper 2014' --right-release 'MB Paper 2015' --raw",
        ],
    },
    "snapshot-transfer": {
        "description": "Move a warmed local dataset to another machine or working copy.",
        "commands": [
            "flylight snapshot-export --out data/flylight-snapshot.tar.gz",
            "flylight snapshot-import data/flylight-snapshot.tar.gz --force",
            "flylight update --all --offline",
        ],
    },
    "schema-introspection": {
        "description": "Inspect agent-facing row shapes before consuming exports.",
        "commands": [
            "flylight schema --json",
            "flylight schema --entity line",
            "flylight schema --entity compare-release --json",
        ],
    },
}


def examples_for_topic(topic: str | None = None) -> dict[str, dict[str, object]]:
    if topic is None:
        return EXAMPLES
    if topic not in EXAMPLES:
        raise SystemExit(f"unknown examples topic: {topic}")
    return {topic: EXAMPLES[topic]}
