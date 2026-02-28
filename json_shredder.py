from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import polars as pl


_POLARS_DTYPES: Dict[str, pl.DataType] = {
    "Int8": pl.Int8, "Int16": pl.Int16, "Int32": pl.Int32, "Int64": pl.Int64,
    "UInt8": pl.UInt8, "UInt16": pl.UInt16, "UInt32": pl.UInt32, "UInt64": pl.UInt64,
    "Float32": pl.Float32, "Float64": pl.Float64,
    "String": pl.String,
    "Boolean": pl.Boolean,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
}


@dataclass(frozen=True)
class ColRule:
    target_name: str
    value_path: str
    dtype: pl.DataType
    nullable: bool
    default: str | None


class CSVShredder:
    """
    Shred JSON -> Polars DataFrame based purely on a CSV config.

    Parent references:
      - @.a.b        -> from current row dict
      - ^1.a.b       -> from nearest dict parent
      - ^2.a.b       -> from next-nearest dict parent, etc.

    IMPORTANT CHANGE vs previous version:
      - The "parents" stack now stores ONLY dict parents (lists are skipped),
        so ^1 refers to the nearest dict parent regardless of list nesting.
    """

    def __init__(self, config_dir: str | Path):
        self.config_dir = Path(config_dir)

    def shred(self, data: dict, table_name: str) -> pl.DataFrame:
        cfg_path = self.config_dir / f"{table_name}.csv"
        if not cfg_path.exists():
            raise FileNotFoundError(f"Missing config: {cfg_path}")

        cfg = pl.read_csv(cfg_path, infer_schema_length=0)
        if "row_path" not in cfg.columns:
            raise ValueError(f"{cfg_path} must include row_path column")

        # All rows share same row_path (take first)
        row_path = cfg["row_path"][0]

        # Build rules
        rules: List[ColRule] = []
        for r in cfg.to_dicts():
            dtype_str = str(r["dtype"]).strip()
            if dtype_str not in _POLARS_DTYPES:
                raise ValueError(f"{cfg_path}: invalid dtype '{dtype_str}'")

            nullable = str(r["nullable"]).strip().lower() == "true"

            default_raw = r.get("default")
            default = (str(default_raw).strip() if default_raw is not None else "")
            default = default if default != "" and default.lower() != "none" else None

            rules.append(
                ColRule(
                    target_name=str(r["target_name"]).strip(),
                    value_path=str(r["value_path"]).strip(),
                    dtype=_POLARS_DTYPES[dtype_str],
                    nullable=nullable,
                    default=default,
                )
            )

        # Select row objects + dict-parent stack
        row_matches = self._select_rows_with_dict_parents(data, row_path)

        # Build output rows dynamically
        out_rows: List[Dict[str, Any]] = []
        for row_obj, dict_parents in row_matches:
            row_dict: Dict[str, Any] = {}
            for rule in rules:
                val = self._eval_value_path(rule.value_path, row_obj, dict_parents)
                if val is None and rule.default is not None:
                    val = rule.default
                row_dict[rule.target_name] = val
            out_rows.append(row_dict)

        df = pl.DataFrame(out_rows)

        # Cast types (+ parse dates if needed)
        df = self._cast_df(df, rules)

        # Fill defaults for non-nullable cols (after cast)
        for rule in rules:
            if not rule.nullable and rule.default is not None:
                df = df.with_columns(
                    pl.col(rule.target_name)
                    .fill_null(self._coerce_default(rule))
                    .alias(rule.target_name)
                )

        # Enforce column order from CSV
        return df.select([r.target_name for r in rules])

    # ---------- internals ----------

    def _tokenize(self, path: str) -> List[str]:
        # Supports dot paths + [*]
        # Example: MRData.StandingsTable.StandingsLists[*].ConstructorStandings[*]
        path = path.replace("[*]", ".*")
        return [p for p in path.split(".") if p]

    def _select_rows_with_dict_parents(self, data: Any, row_path: str) -> List[Tuple[Any, List[dict]]]:
        """
        Returns list of (row_obj, dict_parents_stack)

        dict_parents_stack contains ONLY dict nodes encountered during traversal
        (lists are skipped). This makes ^1 / ^2 stable even with list nesting.
        """
        tokens = self._tokenize(row_path)
        results: List[Tuple[Any, List[dict]]] = []

        def walk(node: Any, idx: int, dict_parents: List[dict]):
            if idx == len(tokens):
                results.append((node, dict_parents))
                return

            t = tokens[idx]

            if t == "*":
                if isinstance(node, list):
                    for item in node:
                        # node here is a list; do NOT add to dict_parents
                        walk(item, idx + 1, dict_parents)
                return

            # normal key step
            if isinstance(node, dict) and t in node:
                next_node = node[t]
                # node is a dict -> push onto dict_parents
                walk(next_node, idx + 1, dict_parents + [node])
                return

            # no match
            return

        walk(data, 0, [])
        return results

    def _eval_value_path(self, expr: str, row_obj: Any, dict_parents: List[dict]) -> Any:
        """
        expr formats:
          - @.a.b.c        (from current row object)
          - ^1.season      (from nearest dict parent)
          - ^2.foo.bar     (from next-nearest dict parent)
        """
        expr = expr.strip()

        if expr.startswith("@."):
            return self._get_by_tokens(row_obj, self._tokenize(expr[2:]))

        if expr.startswith("^"):
            rest = expr[1:]
            num = ""
            i = 0
            while i < len(rest) and rest[i].isdigit():
                num += rest[i]
                i += 1
            if not num:
                raise ValueError(f"Invalid parent reference (missing level): {expr}")
            level = int(num)
            if i < len(rest) and i < len(rest) and rest[i] == ".":
                i += 1
            path = rest[i:]

            if level <= 0 or level > len(dict_parents):
                return None

            base = dict_parents[-level]
            return self._get_by_tokens(base, self._tokenize(path))

        # Treat plain path as @.path
        return self._get_by_tokens(row_obj, self._tokenize(expr))

    def _get_by_tokens(self, node: Any, tokens: List[str]) -> Any:
        cur = node
        for t in tokens:
            if t == "*":
                # value paths with wildcard -> return list (if present)
                return cur if isinstance(cur, list) else None
            if isinstance(cur, dict):
                cur = cur.get(t)
            else:
                return None
        return cur

    def _cast_df(self, df: pl.DataFrame, rules: List[ColRule]) -> pl.DataFrame:
        exprs = []
        for r in rules:
            col = pl.col(r.target_name)

            if r.dtype == pl.Date:
                expr = (
                    col.str.strptime(pl.Date, strict=False).alias(r.target_name)
                    if df.schema[r.target_name] == pl.String
                    else col.cast(pl.Date, strict=False).alias(r.target_name)
                )
            elif r.dtype == pl.Datetime:
                expr = (
                    col.str.strptime(pl.Datetime, strict=False).alias(r.target_name)
                    if df.schema[r.target_name] == pl.String
                    else col.cast(pl.Datetime, strict=False).alias(r.target_name)
                )
            else:
                expr = col.cast(r.dtype, strict=False).alias(r.target_name)

            exprs.append(expr)

        return df.with_columns(exprs)

    def _coerce_default(self, rule: ColRule) -> Any:
        if rule.default is None:
            return None
        d = rule.default
        if rule.dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            return int(d)
        if rule.dtype in (pl.Float32, pl.Float64):
            return float(d)
        if rule.dtype == pl.Boolean:
            return str(d).strip().lower() in {"true", "1", "yes", "y"}
        return d
