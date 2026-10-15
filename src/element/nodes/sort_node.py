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

class SortNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.sort_fields = config.get('sortFields', [])  # list fields
        self.out_table = make_table_name("sort")         

    def create_observable(self, upstream):
        def do_sort(in_table):
            colinfo = self.dbcon.execute(f"PRAGMA table_info({in_table})").fetchdf()
            colnames = [str(n) for n in colinfo['name']]
            colset = set(colnames)

            def find_case_insensitive(name):
                lname = str(name).lower()
                for c in colnames:
                    if c.lower() == lname:
                        return c  # match column
                return None

            def resolve_actual_col(fid, new_name=None):
                if fid in colset:
                    return fid  # direct match
                if isinstance(new_name, str) and new_name in colset:
                    return new_name  # new col name
                ci = find_case_insensitive(fid)
                if ci:
                    return ci
                return None  

            def numeric_order_expr(col):
                ident = self._safe_ident(col)
                return f"coalesce(try_cast({ident} as double), try_cast({ident} as hugeint), 0)"  

            sort_terms = []
            fields_iter = self.sort_fields if isinstance(self.sort_fields, list) else [self.sort_fields]

            primary_is_wins = False  
            winner_col = find_case_insensitive("winner")

            for idx, sf in enumerate(fields_iter):
                if not isinstance(sf, dict) or "fieldId" not in sf:
                    continue
                fid = str(sf.get("fieldId"))
                direction = "DESC" if sf.get('isDesc', False) else "ASC"

                actual_col = resolve_actual_col(fid, sf.get("newFieldName"))
                if not actual_col:
                    fallback = find_case_insensitive(fid) or find_case_insensitive("Wins") or find_case_insensitive("uIIPtlr0")
                    if not fallback:
                        continue  
                    actual_col = fallback

                expr = numeric_order_expr(actual_col)
                sort_terms.append(f"{expr} {direction} NULLS LAST")

                if idx == 0 and str(actual_col).lower() == "wins": # Scribble specific - need to change to be data agnostic
                    primary_is_wins = True 

            if primary_is_wins and winner_col:
                sort_terms.append(f"{self._safe_ident(winner_col)} ASC")  

            if not sort_terms:
                sql = f"create or replace table {self.out_table} as select * from {in_table}"  # return table if nothing to sort
            else:
                sql = f"create or replace table {self.out_table} as from {in_table} order by {', '.join(sort_terms)}"

            self.dbcon.execute(sql)
            return self.out_table

        return upstream.pipe(ops.map(do_sort))

    def get_query(self, input_table):
        sort_terms = []
        fields_iter = self.sort_fields if isinstance(self.sort_fields, list) else [self.sort_fields]
        for sf in fields_iter:
            if not isinstance(sf, dict) or "fieldId" not in sf:
                continue
            fid = str(sf["fieldId"])
            direction = "DESC" if sf.get("isDesc", False) else "ASC"
            expr = f"coalesce(try_cast({self._safe_ident(fid)} as double), try_cast({self._safe_ident(fid)} as hugeint), 0)"
            sort_terms.append(f"{expr} {direction} NULLS LAST")
        if not sort_terms:
            return f"create or replace table {self.out_table} as select * from {input_table}"
        return f"create or replace table {self.out_table} as from {input_table} order by {', '.join(sort_terms)}"

    def _safe_ident(self, name: str) -> str:
        s = str(name)
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", s):
            return s 
        return f'"{s}"' 
