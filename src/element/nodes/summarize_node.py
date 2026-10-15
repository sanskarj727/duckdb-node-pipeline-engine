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


class SummarizeNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.group_fields = config.get('groupFields', [])
        self.summary_fields = config.get('summaryFields', [])
        self.having = config.get('having', None)
        self.out_table = make_table_name("summary")


    def _sql_ident(self, name):
        return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)) else f'"{name}"'


    def _agg_expr(self, s):
        agg = str(s["aggFunction"]).lower()
        src = str(s["sourceFieldId"]).strip()
        tgt = str(s["newFieldName"]).strip()
        cond = s.get("condition")
        col = "*" if src == "*" else self._sql_ident(src)


        if agg in ("countif", "sumif", "averageif", "avgif"):
            if not cond or not isinstance(cond, str):
                raise ValueError(f"{agg} requires 'condition' string")
            if agg == "countif":
                return f"count(*) FILTER (WHERE {cond}) as {self._sql_ident(tgt)}"
            if agg == "sumif":
                return f"sum({col}) FILTER (WHERE {cond}) as {self._sql_ident(tgt)}"
            if agg in ("averageif", "avgif"):
                return f"avg({col}) FILTER (WHERE {cond}) as {self._sql_ident(tgt)}"


        if agg in ("count_distinct", "nunique", "distinct"):
            if col == "*":
                return f"count(*) as {self._sql_ident(tgt)}"
            return f"count(distinct {col}) as {self._sql_ident(tgt)}"


        if agg == "count":
            return f"cast(count({col}) as integer) as {self._sql_ident(tgt)}"
        if agg == "sum":
            return f"sum({col}) as {self._sql_ident(tgt)}"
        if agg in ("avg","average"):
            return f"avg({col}) as {self._sql_ident(tgt)}"
        if agg == "min":
            return f"min({col}) as {self._sql_ident(tgt)}"
        if agg == "max":
            return f"max({col}) as {self._sql_ident(tgt)}"

        # --- ADDED: product aggregate (DuckDB supports product(arg)) ---
        if agg == "product":
            return f"product({col}) as {self._sql_ident(tgt)}"

        # --- ADDED: productsum aggregate (sum(x * y)) ---
        if agg == "productsum":
            x = s.get("xFieldId")
            y = s.get("yFieldId")
            if not x or not y:
                raise ValueError("productsum requires 'xFieldId' and 'yFieldId'")
            x_col = self._sql_ident(x)
            y_col = self._sql_ident(y)
            return f"sum({x_col} * {y_col}) as {self._sql_ident(tgt)}"


        if agg == "median":
            return f"median({col}) as {self._sql_ident(tgt)}"
        if agg == "mode":
            return f"mode({col}) as {self._sql_ident(tgt)}"
        if agg in ("stdev", "stddev", "stddev_samp"):
            return f"stddev_samp({col}) as {self._sql_ident(tgt)}"
        if agg in ("stdevp", "stddev_pop"):
            return f"stddev_pop({col}) as {self._sql_ident(tgt)}"


        if agg == "percentile":
            p = float(s.get("percentile", 0.5))
            return f"quantile_cont({col}, {p}) as {self._sql_ident(tgt)}"


        if agg == "percentrank":
            order_by_col = s.get("order_by") or col
            return f"percent_rank() over (order by {self._sql_ident(order_by_col)}) as {self._sql_ident(tgt)}"


        if agg == "quartile":
            q = float(s.get("quartile", 0.25))
            return f"quantile_cont({col}, {q}) as {self._sql_ident(tgt)}"


        if agg == "covar":
            y_col = self._sql_ident(s.get("yFieldId"))
            covar_type = s.get("covarType", "pop").lower()
            fn = "covar_pop" if covar_type == "pop" else "covar_samp"
            return f"{fn}({col}, {y_col}) as {self._sql_ident(tgt)}"


        return f"{agg}({col}) as {self._sql_ident(tgt)}"


    def create_observable(self, upstream):
        def do_summary(in_table):
            aggs = [self._agg_expr(s) for s in self.summary_fields]
            if self.group_fields:
                gb = ", ".join([self._sql_ident(g) for g in self.group_fields])
                sel = f"{gb}, {', '.join(aggs)}" if aggs else gb
                having_clause = f" having {self.having}" if self.having else ""
                sql = f"create or replace table {self.out_table} as select {sel} from {in_table} group by {gb}{having_clause}"
            else:
                sel = ", ".join(aggs) if aggs else "count(*) as rows"
                sql = f"create or replace table {self.out_table} as select {sel} from {in_table}"
            self.dbcon.execute(sql)
            return self.out_table
        return upstream.pipe(ops.map(do_summary))


    def get_query(self, input_table):
        aggs = [self._agg_expr(s) for s in self.summary_fields]
        if self.group_fields:
            gb = ", ".join([self._sql_ident(g) for g in self.group_fields])
            sel = f"{gb}, {', '.join(aggs)}" if aggs else gb
            having_clause = f" having {self.having}" if self.having else ""
            return f"create or replace table {self.out_table} as select {sel} from {input_table} group by {gb}{having_clause}"
        sel = ", ".join(aggs) if aggs else "count(*) as rows"
        return f"create or replace table {self.out_table} as select {sel} from {input_table}"
