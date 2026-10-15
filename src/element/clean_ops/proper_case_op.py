from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class ProperCaseOp(Operation):
    def apply(self, node):
        ops_cfg = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops_cfg:
            return

        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        targets = []

        for cfg in ops_cfg:
            if str(cfg.get("op", "")).lower().strip() == "proper":
                cols = [c for c in cfg.get("columns", []) if c in all_cols]
                targets.extend(cols)

        if not targets:
            return

        data = node.dbcon.execute(f"select * from {node.out_table}").fetch_df()
        for c in targets:
            data[c] = data[c].astype("string").map(node._proper_case_py)

        node.dbcon.register("clean_py_df", data[data.columns.tolist()])
        node.dbcon.execute(
            f"create or replace table {node.out_table} as "
            f"select * from clean_py_df"
        )