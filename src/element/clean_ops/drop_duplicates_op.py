from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class DropDuplicatesOp(Operation):
    def apply(self, node):
        if not node.drop_duplicates:
            return

        subset_cols = (
            node.dup_subset
            if isinstance(node.dup_subset, list)
            else [node.dup_subset]
            if node.dup_subset
            else []
        )

        if not subset_cols:
            subset_cols = [
                desc[1]
                for desc in node.dbcon.execute(
                    f"pragma table_info('{node.out_table}')"
                ).fetchall()
            ]

        cols_str = ", ".join(subset_cols)
        tmp_table = f"{node.out_table}_dedup"

        node.dbcon.execute(
            f"create or replace table {tmp_table} as "
            f"select * from ("
            f"  select *, "
            f"         row_number() over (partition by {cols_str}) as rn "
            f"  from {node.out_table}"
            f") where rn = 1"
        )
        node.dbcon.execute(f"drop table {node.out_table}")
        node.dbcon.execute(
            f"alter table {tmp_table} rename to {node.out_table}"
        )