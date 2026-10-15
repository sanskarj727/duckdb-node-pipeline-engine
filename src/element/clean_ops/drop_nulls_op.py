from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class DropNullsOp(Operation):
    def apply(self, node):
        if not node.drop_nulls:
            return
        conds = [f"{c} is not null" for c in node.drop_nulls]
        where_clause = " AND ".join(conds)
        node.dbcon.execute(
            f"delete from {node.out_table} where not ({where_clause})"
        )