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

class LookupNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.mode = config.get("mode", "scalar")                # scalar vs map behaviour
        self.function = str(config.get("function","")).lower()  # hlookup/index/match/indirect
        self.args = config.get("args", {})                      # function-specific args
        self.input_col = config.get("input_col")
        self.output_col = config.get("output_col","lookup_value")
        self.join_on = config.get("join_on")                    # map mode join config
        self.order_by = config.get("order_by")                  # row numbering order
        self.out_table = make_table_name("lookup")              # table to store result

    def _sql_ident(self, s):
        return s if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(s)) else f'"{s}"' 

    def _ensure_rowid(self, table):
        tmp = make_table_name("rowid")
        if self.order_by:
            ob = self.order_by
            self.dbcon.execute(
                f"create or replace table {tmp} as "
                f"select row_number() over (order by {ob}) as _rn, * from {table}"
            )  # stable row numbers with custom ordering
        else:
            self.dbcon.execute(
                f"create or replace table {tmp} as "
                f"select row_number() over () as _rn, * from {table}"
            )  # row numbers with arbitrary order
        return tmp

    def _hlookup_scalar(self):
        rng = self.args["range_table"]
        return_row = int(self.args.get("return_row", 2))
        header_key = str(self.args["lookup_key"])
        t = self._ensure_rowid(rng)
        cols_df = self.dbcon.execute(f"describe {t}").fetch_df()
        vis_cols = [c for c in cols_df["column_name"].tolist() if c != "_rn"]  
        try:
            idx = vis_cols.index(header_key)
        except ValueError:
            self.dbcon.execute(f"create or replace table {self.out_table} as select NULL as {self._sql_ident(self.output_col)}")
            return self.out_table  # header not found then return NULL
        target_col = vis_cols[idx]
        self.dbcon.execute(
            f"create or replace table {self.out_table} as "
            f"select {self._sql_ident(target_col)} as {self._sql_ident(self.output_col)} "
            f"from {t} where _rn={return_row}"
        )  # take value from requested row in matching column
        return self.out_table

    def _index_scalar(self):
        rng = self.args["range_table"]
        row_num = int(self.args["row_num"])
        col_num = int(self.args["col_num"])
        t = self._ensure_rowid(rng)
        cols_df = self.dbcon.execute(f"describe {t}").fetch_df()
        vis_cols = [c for c in cols_df["column_name"].tolist() if c not in ("_rn",)]
        if col_num < 1 or col_num > len(vis_cols):
            self.dbcon.execute(f"create or replace table {self.out_table} as select NULL as {self._sql_ident(self.output_col)}")
            return self.out_table  # out-of-range column
        colname = vis_cols[col_num-1]
        self.dbcon.execute(
            f"create or replace table {self.out_table} as "
            f"select {self._sql_ident(colname)} as {self._sql_ident(self.output_col)} from {t} where _rn={row_num}"
        )  # index style row/column lookup
        return self.out_table

    def _match_scalar(self):
        rng = self.args["range_table"]
        target_col = self.args["target_col"]
        lookup_value = self.args["lookup_value"]
        norm = (self.args.get("normalize") or "").lower()
        t = self._ensure_rowid(rng)
        col = self._sql_ident(target_col)
        if norm == "trim_lower":
            pred = f"lower(trim({col})) = lower(trim('{lookup_value}'))"
        else:
            pred = f"{col} = '{lookup_value}'"
        self.dbcon.execute(
            f"create or replace table {self.out_table} as "
            f"select _rn as position from {t} where {pred} limit 1"
        )  # on match return first matching position
        if self.dbcon.execute(f"select count(*) from {self.out_table}").fetchone()[0] == 0:
            self.dbcon.execute(f"create or replace table {self.out_table} as select NULL as position")  # no match → NULL
        return self.out_table

    def _indirect_scalar(self):
        ref_table = self.args.get("table")
        ref_col = self.args.get("column")
        if ref_table and ref_col:
            self.dbcon.execute(
                f"create or replace table {self.out_table} as "
                f"select {self._sql_ident(ref_col)} as {self._sql_ident(self.output_col)} from {self._sql_ident(ref_table)}"
            ) 
        elif ref_table:
            self.dbcon.execute(
                f"create or replace table {self.out_table} as "
                f"select * from {self._sql_ident(ref_table)}"
            )  
        else:
            self.dbcon.execute(f"create or replace table {self.out_table} as select NULL as {self._sql_ident(self.output_col)}")
        return self.out_table

    def create_observable(self, upstream):
        def run(in_table):
            if self.mode == "scalar":
                if self.function == "hlookup":
                    return self._hlookup_scalar()
                if self.function == "index":
                    return self._index_scalar()
                if self.function == "match":
                    return self._match_scalar()
                if self.function == "indirect":
                    return self._indirect_scalar()
                raise ValueError(f"Unknown function {self.function}")  
            if self.mode == "map" and self.function in ("match", "index", "hlookup"):
                if self.join_on and self.function == "match":
                    left_col = self._sql_ident(self.join_on["left_col"])
                    right_table = self._sql_ident(self.join_on["right_table"])
                    right_col = self._sql_ident(self.join_on["right_col"])
                    self.dbcon.execute(
                        f"create or replace table {self.out_table} as "
                        f"select a.*, b._rn as {self._sql_ident(self.output_col)} "
                        f"from {in_table} a "
                        f"left join ({self._ensure_rowid(self.join_on['right_table'])}) b "
                        f"on a.{left_col} = b.{right_col}"
                    )  
                    return self.out_table
                raise ValueError("map mode currently supports match via join_on")
            raise ValueError("Unsupported mode")  
        return upstream.pipe(ops.map(run))

    def _ensure_materialized(self):
        try:
            cnt = self.dbcon.execute(
                f"select count(*) from information_schema.tables where table_name = '{self.out_table}'"
            ).fetchone()[0]
        except Exception:
            cnt = 0
        if cnt == 0:
            if self.mode == "scalar":
                if self.function == "hlookup":
                    self._hlookup_scalar()
                elif self.function == "index":
                    self._index_scalar()
                elif self.function == "match":
                    self._match_scalar()
                elif self.function == "indirect":
                    self._indirect_scalar()  

    def get_query(self, input_table):
        self._ensure_materialized()  
        return f"select * from {self.out_table}"
