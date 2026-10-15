from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class ExtractOps(Operation):
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
            if not cols:
                continue

            if op == "left":
                n = int(cfg.get("n", 0))
                for c in cols:
                    mapping[c] = (
                        f"substr({node._sql_ident(c)}, 1, {n})"
                    )

            if op == "right":
                n = int(cfg.get("n", 0))
                for c in cols:
                    cid = node._sql_ident(c)
                    mapping[c] = (
                        f"substr({cid}, greatest(length({cid}) - {n} + 1, 1), {n})"
                    )

            if op == "mid":
                start = int(cfg.get("start", 1))
                length = int(cfg.get("length", 0))
                for c in cols:
                    mapping[c] = (
                        f"substr({node._sql_ident(c)}, {start}, {length})"
                    )

        if mapping:
            node._apply_sql_projection(node.out_table, mapping)