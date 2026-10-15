from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class FuzzyDetectOp(Operation):
    def apply(self, node):
        cfg = getattr(node, "fuzzy_detect", None)
        if not (cfg and isinstance(cfg, dict)):
            return

        column = cfg.get("column")
        new_col = cfg.get("output_col", "similarity_score")
        threshold = cfg.get("threshold", 0.8)
        label_col = cfg.get("label_col")

        if not column:
            return

        label_expr = f"b.{column}" if label_col else "NULL"

        similarity_expr = (
            f"(1-cast(levenshtein(a.{column}, b.{column}) as double) "
            f"/ greatest(length(a.{column}), length(b.{column})))"
        )

        select_clause = (
            f"a.*, m.sim_score as {new_col}"
            + (f", m.sim_label as {label_col}" if label_col else "")
        )

        where_clause = f"a.{column} != b.{column}"

        sql = f"""
create or replace table {node.out_table} as
select {select_clause}
from {node.out_table} a
left join (
    select a_rowid, sim_score, sim_label from (
        select
            a.rowid as a_rowid,
            {similarity_expr} as sim_score,
            {label_expr} as sim_label,
            row_number() over (
                partition by a.rowid
                order by (1-cast(levenshtein(a.{column}, b.{column}) as double)
                          / greatest(length(a.{column}), length(b.{column}))) desc
            ) as rn
        from {node.out_table} a
        join {node.out_table} b
        on {where_clause}
    ) m
    where rn = 1
) m
on a.rowid = m.a_rowid
"""
        node.dbcon.execute(sql)

        if threshold is not None:
            node.dbcon.execute(
                f"""
create or replace table {node.out_table} as
select * from {node.out_table} where {new_col} >= {float(threshold)}
"""
            )