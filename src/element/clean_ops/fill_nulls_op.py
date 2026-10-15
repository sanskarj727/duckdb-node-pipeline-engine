from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops


class FillNullsOp(Operation):
    def apply(self, node):
        for col, cfg in node.fill_nulls.items():
            if not isinstance(cfg, dict):
                val = cfg
                v_expr = "NULL" if val is None or val == "NULL" else f"'{val}'"
                node.dbcon.execute(
                    f"update {node.out_table} "
                    f"set {col} = {v_expr} where {col} is null"
                )
                continue

            strategy = (cfg.get("strategy") or "").lower().strip()
            cast_as = cfg.get("cast_as")
            part_by = cfg.get("partition_by") or []

            if strategy in ("", "constant"):
                v = cfg.get("value")
                v_expr = "NULL" if v is None or v == "NULL" else f"'{v}'"
                node.dbcon.execute(
                    f"update {node.out_table} "
                    f"set {col} = {v_expr} where {col} is null"
                )
            elif strategy in ("mean", "median", "mode"):
                node.fill_global_stat(col, strategy, cast_as)
            elif strategy in ("group_mean", "group_median", "group_mode"):
                if part_by:
                    node.fill_group_stat(col, strategy, part_by, cast_as)
            elif strategy == "fallback":
                for step in cfg.get("steps", []):
                    st = (step.get("strategy") or "").lower().strip()
                    if not st:
                        continue
                    if st in ("", "constant"):
                        v = step.get("value")
                        v_expr = "NULL" if v is None or v == "NULL" else f"'{v}'"
                        node.dbcon.execute(
                            f"update {node.out_table} "
                            f"set {col} = {v_expr} where {col} is null"
                        )
                    elif st in ("mean", "median", "mode"):
                        node.fill_global_stat(col, st, cast_as)
                    elif st in ("group_mean", "group_median", "group_mode"):
                        pb = step.get("partition_by") or []
                        if pb:
                            node.fill_group_stat(col, st, pb, cast_as)