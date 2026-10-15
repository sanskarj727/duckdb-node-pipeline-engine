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

class TextTransformNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.operation = str(config.get("operation", "trim")).strip().lower()
        self.columns = [str(c) for c in config.get("columns", [])]
        self.keep_others = bool(config.get("keep_others", True))
        self.pattern_old = config.get("pattern_old")
        self.pattern_new = config.get("pattern_new")
        self.start = config.get("start")
        self.length_n = config.get("length")
        self.new_text = config.get("new_text")
        self.target_type = config.get("target_type", "DOUBLE")
        self.fmt = config.get("fmt")
        self.out_table = make_table_name("textxf")
        if self.operation not in {"trim", "upper", "lower", "substitute", "replace", "value", "text", "proper"}:
            raise ValueError(f"Unsupported text operation: {self.operation}")

    def _sql_ident(self, name):
        return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)) else f'"{name}"'

    def _normalize(self, s):
        return str(s).strip().lower()

    def _proper_case_py(self, s):
        if s is None:
            return s
        if not isinstance(s, str):
            s = str(s)
        s = s.lower()
        if not s:
            return s
        s = s[0].upper() + s[1:]
        out = []
        prev = None
        for ch in s:
            if prev == ' ' and ch.isalpha():
                out.append(ch.upper())
            else:
                out.append(ch)
            prev = ch
        s = ''.join(out)
        for delim in ("-", "'"):
            out = []
            prev = None
            for ch in s:
                if prev == delim and ch.isalpha():
                    out.append(ch.upper())
                else:
                    out.append(ch)
                prev = ch
            s = ''.join(out)
        return s

    def _substitute_sql(self, col_id):
        if self.pattern_old is None or self.pattern_new is None:
            raise ValueError("substitute requires pattern_old and pattern_new")
        po = self.pattern_old.replace("'", "''")
        pn = self.pattern_new.replace("'", "''")
        return f"replace({col_id}, '{po}', '{pn}')"

    def _replace_sql(self, col_id):
        if self.start is None or self.length_n is None or self.new_text is None:
            raise ValueError("replace requires start, length, new_text")
        start = int(self.start)
        length = int(self.length_n)
        newt = self.new_text.replace("'", "''")
        left_part = f"case when {start} > 1 then substr({col_id}, 1, {start}-1) else '' end"
        right_start = f"case when {start}+{length} <= length({col_id}) then {start}+{length} else length({col_id})+1 end"
        right_part = f"case when {right_start} <= length({col_id}) then substr({col_id}, {right_start}) else '' end"
        return f"({left_part} || '{newt}' || {right_part})"

    def _value_sql(self, col_id):
        tt = str(self.target_type).upper()
        return f"try_cast({col_id} as {tt})"

    def _text_sql(self, col_id):
        if self.fmt:
            fmt = self.fmt.replace("'", "''")
            return f"printf('{fmt}', cast({col_id} as DOUBLE))"
        return f"cast({col_id} as VARCHAR)"

    def _transform_expr(self, raw_col):
        col_id = self._sql_ident(raw_col)
        op = self.operation
        if op == "trim":
            return f"trim({col_id}) as {col_id}"
        if op == "upper":
            return f"upper({col_id}) as {col_id}"
        if op == "lower":
            return f"lower({col_id}) as {col_id}"
        if op == "substitute":
            return f"{self._substitute_sql(col_id)} as {col_id}"
        if op == "replace":
            return f"{self._replace_sql(col_id)} as {col_id}"
        if op == "value":
            return f"{self._value_sql(col_id)} as {col_id}"
        if op == "text":
            return f"{self._text_sql(col_id)} as {col_id}"
        if op == "proper":
            raise ValueError("proper is handled via Python path; call create_observable for routing")
        raise ValueError(f"Unsupported text operation: {op}")

    def _build_sql_query(self, input_table):
        cols_df = self.dbcon.execute(f"describe {input_table}").fetch_df()
        all_cols = cols_df["column_name"].tolist()
        targets = {self._normalize(c) for c in self.columns}
        select_items = []
        for raw in all_cols:
            if self._normalize(raw) in targets and self.operation != "proper":
                select_items.append(self._transform_expr(raw))
            else:
                select_items.append(self._sql_ident(raw))
        select_list = ", ".join(select_items)
        return f"create or replace table {self.out_table} as select {select_list} from {input_table}"

    def _run_sql(self, in_table):
        query = self._build_sql_query(in_table)
        self.dbcon.execute(query)
        return self.out_table

    def _run_proper_python(self, in_table):
        cols_df = self.dbcon.execute(f"describe {in_table}").fetch_df()
        all_cols = cols_df["column_name"].tolist()
        norm_map = {self._normalize(c): c for c in all_cols}
        requested = [self._normalize(c) for c in self.columns]
        target_cols = [norm_map[n] for n in requested if n in norm_map]
        if not target_cols:
            self.dbcon.execute(f"create or replace table {self.out_table} as select * from {in_table}")
            return self.out_table
        select_cols = all_cols if self.keep_others else target_cols
        cols_csv = ", ".join([self._sql_ident(c) for c in select_cols])
        df = self.dbcon.execute(f"select {cols_csv} from {in_table}").fetch_df()
        for c in target_cols:
            df[c] = df[c].astype("string").map(self._proper_case_py)
        self.dbcon.register("textxf_py_df", df[select_cols])
        self.dbcon.execute(f"create or replace table {self.out_table} as select * from textxf_py_df")
        return self.out_table

    def get_query(self, input_table):
        if self.operation == "proper":
            return f"-- proper/title-case via Python on {input_table} -> {self.out_table}"
        return self._build_sql_query(input_table)

    def create_observable(self, upstream):
        def _run(in_table):
            if not self.columns:
                self.dbcon.execute(f"create or replace table {self.out_table} as select * from {in_table}")
                return self.out_table
            if self.operation == "proper":
                return self._run_proper_python(in_table)
            return self._run_sql(in_table)
        return upstream.pipe(ops.take(1), ops.map(_run), ops.publish(), ops.ref_count())