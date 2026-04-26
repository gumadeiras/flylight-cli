from __future__ import annotations


EXAMPLES = {
    "warm-offline": {
        "description": "Warm the local cache and then operate fully offline.",
        "commands": [
            "flylight sync --all --refresh-cache",
            "flylight cache-info",
            "flylight sync --all --offline",
            "flylight releases --offline",
        ],
    },
    "line-investigation": {
        "description": "Find a line, inspect its images, and export machine-readable rows.",
        "commands": [
            "flylight search --expressed-in DNp04 --ad 31B08 --source-kind line-metadata",
            "flylight show-line SS00724 --release 'Descending Neurons 2018'",
            "flylight export-ndjson --entity line --release 'Descending Neurons 2018' --line SS00724",
            "flylight export-ndjson --entity image --line SS00724 --raw",
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
            "flylight sync --all --offline",
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
