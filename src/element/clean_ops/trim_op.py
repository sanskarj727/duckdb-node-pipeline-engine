from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class TrimUpperLowerOp(Operation):
    def apply(self, node):
        ops_cfg = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops_cfg:
            return

        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        mapping = {}

        for cfg in ops_cfg:
            op = str(cfg.get("op", "")).lower().strip()
            cols = [c for c in cfg.get("columns", []) if c in all_cols]
            if not cols and op != "proper":
                continue

            if op == "trim":
                for c in cols:
                    mapping[c] = f"trim({node._sql_ident(c)})"
            if op == "upper":
                for c in cols:
                    mapping[c] = f"upper({node._sql_ident(c)})"
            if op == "lower":
                for c in cols:
                    mapping[c] = f"lower({node._sql_ident(c)})"

        if mapping:
            node._apply_sql_projection(node.out_table, mapping)