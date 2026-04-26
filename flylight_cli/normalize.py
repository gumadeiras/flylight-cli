from __future__ import annotations

import json
from typing import Any


def _split_text(value: str, separator: str) -> list[str]:
    return [part.strip() for part in value.split(separator) if part.strip()]


def split_pipe_text(value: str) -> list[str]:
    return _split_text(value, "|")


def split_comma_text(value: str) -> list[str]:
    return _split_text(value, ",")


def split_semicolon_text(value: str) -> list[str]:
    return _split_text(value, ";")


def parse_json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def extract_em_cell_type_terms(payload: dict[str, Any]) -> list[str]:
    terms = []
    for item in parse_json_array(payload.get("em_cell_type")):
        if not isinstance(item, dict):
            continue
        term = str(item.get("term", "") or "").strip()
        if term:
            terms.append(term)
    return sorted(set(terms))


def normalize_line_record(record: dict[str, Any]) -> dict[str, Any]:
    expressed_in_text = str(record.get("expressed_in_text", "") or "").strip()
    return {
        **record,
        "annotations": split_pipe_text(str(record.get("annotations_text", "") or "")),
        "rois": split_pipe_text(str(record.get("rois_text", "") or "")),
        "robot_ids": split_pipe_text(str(record.get("robot_ids_text", "") or "")),
        "expressed_in": split_comma_text(expressed_in_text) or ([expressed_in_text] if expressed_in_text else []),
        "genotype_parts": split_semicolon_text(str(record.get("genotype_text", "") or "")),
        "ad_parts": split_semicolon_text(str(record.get("ad_text", "") or "")),
        "dbd_parts": split_semicolon_text(str(record.get("dbd_text", "") or "")),
        "em_cell_types": split_pipe_text(str(record.get("em_cell_types_text", "") or "")),
    }


def normalize_image_record(record: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    roi = str(record.get("roi", "") or "").strip()
    em_cell_types = split_pipe_text(str(record.get("em_cell_types_text", "") or "")) or extract_em_cell_type_terms(payload)
    return {
        **record,
        "annotations": split_pipe_text(str(record.get("annotations_text", "") or "")),
        "roi_terms": split_comma_text(roi),
        "genotype_parts": split_semicolon_text(str(payload.get("genotype", "") or "")),
        "ad_parts": split_semicolon_text(str(payload.get("ad", "") or "")),
        "dbd_parts": split_semicolon_text(str(payload.get("dbd", "") or "")),
        "em_cell_types": em_cell_types,
    }


def normalize_release_record(record: dict[str, Any], publication: Any) -> dict[str, Any]:
    return {
        **record,
        "publication": publication,
    }
