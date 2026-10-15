from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops


class ReplacementsOp(Operation):
    def apply(self, node):
        for col, val_map in node.replacements.items():
            for old_val, new_val in val_map.items():
                node.dbcon.execute(
                    f"update {node.out_table} "
                    f"set {col} = '{new_val}' where {col} = '{old_val}'"
                )