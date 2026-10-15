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

class ValidationNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.rules = []
        self.out_table = make_table_name("valid")
        self._parse_config(config)

    def _parse_config(self, config):
        for c in (config if isinstance(config, list) else [config]):
            if c.get("type") == "validation":
                self.rules = c.get("rules") or []

    def _sql_ident(self, name):
        return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)) else f'"{name}"'

    def _expr_for_rule(self, rule, colname):
        op = str(rule.get("op","")).lower().strip()
        cid = self._sql_ident(colname)
        if op == "isblank":
            return f"({cid} is null)"
        if op == "isnumber":
            return f"(try_cast({cid} as double) is not null)"
        if op == "istext":
            return f"({cid} is not null and try_cast({cid} as double) is null)"
        if op == "isnontext":
            return f"({cid} is null or try_cast({cid} as double) is not null)"
        if op == "iserror":
            return "(false)"
        raise ValueError(f"Unsupported validation op: {op}")

    def _apply_validation(self, in_tbl):
        cols_df = self.dbcon.execute(f"describe {in_tbl}").fetch_df()
        all_cols = cols_df["column_name"].tolist()
        select_items = [self._sql_ident(c) for c in all_cols]
        for rule in self.rules:
            op = str(rule.get("op","")).lower().strip()
            cols = rule.get("columns") or []
            target = rule.get("output")
            for c in cols:
                if c not in all_cols:
                    continue
                expr = self._expr_for_rule(rule, c)
                out_name = target or f"{op}_{c}"
                select_items.append(f"{expr} as {self._sql_ident(out_name)}")
        sql = f"create or replace table {self.out_table} as select {', '.join(select_items)} from {in_tbl}"
        self.dbcon.execute(sql)
        return self.out_table

    def create_observable(self, upstream):
        def run(in_table):
            return self._apply_validation(in_table)
        return upstream.pipe(ops.map(run))

    def get_query(self, input_table):
        return f"select * from {self.out_table}"