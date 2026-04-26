from __future__ import annotations

from typing import Any


def add_eq_clause(clauses: list[str], params: list[Any], column: str, value: str | None) -> None:
    if value:
        clauses.append(f"{column} = ?")
        params.append(value)


def add_like_clause(clauses: list[str], params: list[Any], column: str, value: str | None) -> None:
    if value:
        clauses.append(f"{column} LIKE ?")
        params.append(f"%{value}%")


def add_pipe_token_clause(clauses: list[str], params: list[Any], column: str, value: str | None) -> None:
    if value:
        clauses.append(f"({column} = ? OR {column} LIKE ? OR {column} LIKE ? OR {column} LIKE ?)")
        params.extend([value, f"{value} | %", f"% | {value} | %", f"% | {value}"])


def add_min_clause(clauses: list[str], params: list[Any], column: str, value: int | None) -> None:
    if value is not None:
        clauses.append(f"{column} >= ?")
        params.append(value)


def build_line_search_sql(args: Any) -> tuple[str, list[Any]]:
    clauses = ["1=1"]
    params: list[Any] = []
    add_eq_clause(clauses, params, "lr.release", getattr(args, "release", None))
    add_eq_clause(clauses, params, "r.source_kind", getattr(args, "source_kind", None))
    add_like_clause(clauses, params, "lr.line", getattr(args, "line", None))
    add_like_clause(clauses, params, "lr.annotations_text", getattr(args, "annotation", None))
    add_like_clause(clauses, params, "lr.rois_text", getattr(args, "roi", None))
    add_like_clause(clauses, params, "lr.robot_ids_text", getattr(args, "robot_id", None))
    add_like_clause(clauses, params, "lr.expressed_in_text", getattr(args, "expressed_in", None))
    add_like_clause(clauses, params, "lr.genotype_text", getattr(args, "genotype", None))
    add_like_clause(clauses, params, "lr.ad_text", getattr(args, "ad", None))
    add_like_clause(clauses, params, "lr.dbd_text", getattr(args, "dbd", None))
    add_pipe_token_clause(clauses, params, "lr.em_cell_types_text", getattr(args, "em_cell_type", None))
    add_min_clause(clauses, params, "lr.image_count", getattr(args, "min_images", None))
    add_min_clause(clauses, params, "lr.sample_count", getattr(args, "min_samples", None))
    term = getattr(args, "term", None)
    if term:
        like = f"%{term}%"
        clauses.append(
            "("
            "lr.line LIKE ? OR lr.annotations_text LIKE ? OR lr.rois_text LIKE ? OR lr.expressed_in_text LIKE ? "
            "OR lr.genotype_text LIKE ? OR lr.ad_text LIKE ? OR lr.dbd_text LIKE ? OR lr.robot_ids_text LIKE ? "
            "OR lr.em_cell_types_text LIKE ?"
            ")"
        )
        params.extend([like, like, like, like, like, like, like, like, like])
    sql = f"""
      SELECT lr.release, lr.line, lr.image_count, lr.sample_count, lr.annotations_text, lr.rois_text,
             lr.robot_ids_text, lr.expressed_in_text, lr.genotype_text, lr.ad_text, lr.dbd_text, lr.em_cell_types_text,
             r.source_kind, r.source_locator, r.source_token
      FROM line_releases lr
      JOIN releases r ON r.name = lr.release
      WHERE {' AND '.join(clauses)}
      ORDER BY lr.line, lr.release
      LIMIT ?
    """
    params.append(getattr(args, "limit"))
    return sql, params


def build_image_search_sql(args: Any) -> tuple[str, list[Any]]:
    clauses = ["1=1"]
    params: list[Any] = []
    add_eq_clause(clauses, params, "i.release", getattr(args, "release", None))
    add_eq_clause(clauses, params, "r.source_kind", getattr(args, "source_kind", None))
    add_like_clause(clauses, params, "i.line", getattr(args, "line", None))
    add_like_clause(clauses, params, "i.annotations_text", getattr(args, "annotation", None))
    add_like_clause(clauses, params, "i.roi", getattr(args, "roi", None))
    add_like_clause(clauses, params, "i.robot_id", getattr(args, "robot_id", None))
    add_like_clause(clauses, params, "i.area", getattr(args, "area", None))
    add_like_clause(clauses, params, "i.objective", getattr(args, "objective", None))
    add_like_clause(clauses, params, "i.gender", getattr(args, "gender", None))
    add_pipe_token_clause(clauses, params, "i.em_cell_types_text", getattr(args, "em_cell_type", None))
    term = getattr(args, "term", None)
    if term:
        like = f"%{term}%"
        clauses.append(
            "("
            "i.line LIKE ? OR i.roi LIKE ? OR i.annotations_text LIKE ? OR i.robot_id LIKE ? "
            "OR i.slide_code LIKE ? OR i.area LIKE ? OR i.objective LIKE ? OR i.gender LIKE ? "
            "OR i.em_cell_types_text LIKE ?"
            ")"
        )
        params.extend([like, like, like, like, like, like, like, like, like])
    sql = f"""
      SELECT i.image_id, i.release, i.line, i.robot_id, i.slide_code, i.objective, i.area, i.tile, i.gender, i.roi,
             i.annotations_text, i.em_cell_types_text, i.metadata_key, i.metadata_url, i.raw_json, r.source_kind
      FROM images i
      JOIN releases r ON r.name = i.release
      WHERE {' AND '.join(clauses)}
      ORDER BY i.release, i.line, i.slide_code, i.image_id
      LIMIT ?
    """
    params.append(getattr(args, "limit"))
    return sql, params


def build_line_text_search_sql(args: Any) -> tuple[str, list[Any]]:
    clauses = ["line_search_fts MATCH ?"]
    params: list[Any] = [getattr(args, "query")]
    add_eq_clause(clauses, params, "lr.release", getattr(args, "release", None))
    add_eq_clause(clauses, params, "r.source_kind", getattr(args, "source_kind", None))
    sql = f"""
      SELECT lr.release, lr.line, lr.image_count, lr.sample_count, lr.annotations_text, lr.rois_text,
             lr.robot_ids_text, lr.expressed_in_text, lr.genotype_text, lr.ad_text, lr.dbd_text, lr.em_cell_types_text,
             r.source_kind, r.source_locator, r.source_token,
             bm25(line_search_fts, 5.0, 1.5, 1.5, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0) AS rank
      FROM line_search_fts
      JOIN line_releases lr ON lr.release = line_search_fts.release AND lr.line = line_search_fts.line
      JOIN releases r ON r.name = lr.release
      WHERE {' AND '.join(clauses)}
      ORDER BY rank, lr.line, lr.release
      LIMIT ?
    """
    params.append(getattr(args, "limit"))
    return sql, params
