from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class ConstraintsOp(Operation):
    def apply(self, node):
        for col, constraint in node.constraints.items():
            if isinstance(constraint, dict):
                if "min" in constraint:
                    node.dbcon.execute(
                        f"delete from {node.out_table} "
                        f"where {col} < {constraint['min']}"
                    )
                if "max" in constraint:
                    node.dbcon.execute(
                        f"delete from {node.out_table} "
                        f"where {col} > {constraint['max']}"
                    )
            elif isinstance(constraint, list):
                vals = ", ".join([f"'{v}'" for v in constraint])
                node.dbcon.execute(
                    f"delete from {node.out_table} "
                    f"where {col} not in ({vals})"
                )