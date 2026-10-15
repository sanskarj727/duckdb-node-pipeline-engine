import duckdb
import reactivex as rx
from reactivex import operators as ops
from uuid import uuid4
import re
import json

import numpy as np
import pandas as pd
import os
import joblib
from datetime import datetime
from src.element.table_utils import make_table_name
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import LinearSVC
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, classification_report
from sklearn.metrics import confusion_matrix

class TransposeNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.name_prefix = config.get("namePrefix") or "T"
        self.cast_type = config.get("castType")  
        self.order_by = config.get("orderBy")    
        self.out_table = make_table_name("transpose")

    def _sql_ident(self, s):
        return s if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(s)) else f'"{s}"'

    def create_observable(self, upstream):
        def run(in_table):
            return self._transpose_full(in_table)
        return upstream.pipe(ops.map(run))

    def get_query(self, input_table):
        return f"select * from {self.out_table}"

    def _transpose_full(self, in_table):
        tmp = make_table_name("trows")
        if self.order_by:
            self.dbcon.execute(
                f"create or replace table {tmp} as "
                f"select row_number() over (order by {self.order_by}) as _rn, * from {in_table}"
            )
        else:
            self.dbcon.execute(
                f"create or replace table {tmp} as "
                f"select row_number() over () as _rn, * from {in_table}"
            )

        cols_df = self.dbcon.execute(f"describe {tmp}").fetch_df()
        cols = [c for c in cols_df["column_name"].tolist() if c != "_rn"]
        if not cols:
            self.dbcon.execute(f"create or replace table {self.out_table} as select 1 where 0=1")
            return self.out_table

        unp_cols = ", ".join([self._sql_ident(c) for c in cols])
        cast_clause = "" if not self.cast_type else f":: {self.cast_type}"
        long_tbl = make_table_name("long_t")
        self.dbcon.execute(
            f"create or replace table {long_tbl} as "
            f"select _rn, column_name as colname, value{cast_clause} as val "
            f"from {tmp} "
            f"unpivot (value for column_name in ({unp_cols}))"
        )

        n_rows = self.dbcon.execute(f"select max(_rn) from {tmp}").fetchone()[0] or 0
        out_cols = [f"max(case when _rn={k} then val end) as {self._sql_ident(self.name_prefix+str(k))}" for k in range(1, n_rows + 1)]

        order_case = " ".join([f"when '{c}' then {i}" for i, c in enumerate(cols)])
        order_expr = f"(case colname {order_case} end)"

        self.dbcon.execute(
            f"create or replace table {self.out_table} as "
            f"select colname as {self._sql_ident(self.name_prefix+'_row')}, {', '.join(out_cols)} "
            f"from {long_tbl} "
            f"group by colname "
            f"order by {order_expr}"
        )
        return self.out_table