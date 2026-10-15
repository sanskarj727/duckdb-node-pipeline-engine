from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class ValidationOpsOp(Operation):
    def apply(self, node):
        ops_cfg = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops_cfg:
            return

        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()

        select_items = [node._sql_ident(c) for c in all_cols]

        for cfg in ops_cfg:
            op = str(cfg.get("op", "")).lower().strip()
            if op not in ("isblank", "isnumber", "istext", "isnontext", "iserror"):
                continue

            cols = cfg.get("columns") or []
            target = cfg.get("output")

            for c in cols:
                if c not in all_cols:
                    continue

                cid = node._sql_ident(c)
                if op == "isblank":
                    expr = f"({cid} is null)"
                    out_name = target or f"IsBlank_{c}"
                elif op == "isnumber":
                    expr = f"(try_cast({cid} as double) is not null)"
                    out_name = target or f"IsNumber_{c}"
                elif op == "istext":
                    expr = (
                        f"({cid} is not null and "
                        f"try_cast({cid} as double) is null)"
                    )
                    out_name = target or f"IsText_{c}"
                elif op == "isnontext":
                    expr = (
                        f"({cid} is null or "
                        f"try_cast({cid} as double) is not null)"
                    )
                    out_name = target or f"IsNonText_{c}"
                elif op == "iserror":
                    expr = "(false)"
                    out_name = target or f"IsError_{c}"
                else:
                    continue

                select_items.append(
                    f"{expr} as {node._sql_ident(out_name)}"
                )

        if len(select_items) == len(all_cols):
            return

        sql = (
            f"create or replace table {node.out_table} as "
            f"select {', '.join(select_items)} from {node.out_table}"
        )
        node.dbcon.execute(sql)