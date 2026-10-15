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

from src.element.clean_ops import PIPELINE_OPS

from src.element.clean_ops import PIPELINE_OPS  # list of cleaning operations to run


class CleaningNode:
    def __init__(self, config, dbcon):
        self.drop_nulls = config.get('drop_nulls', [])          # drop rows config
        self.fill_nulls = config.get('fill_nulls', {})          # fill null config
        self.replacements = config.get('replacements', {})      # replace config
        self.drop_duplicates = config.get('drop_duplicates', False)  # drop duplicates config 
        self.dup_subset = config.get('dup_subset', None)        # check duplicates
        self.constraints = config.get('constraints', {})        # constraint handling config
        self.iqr_outliers = config.get('iqr_outliers', None)    # IQR outlier config
        self.fuzzy_detect = config.get('fuzzy_detect', None)    # fuzzy similarity config
        self.operations = config.get('operations', [])          # generic ops (trim, etc.)
        self.text_ops = config.get('text_ops', [])              # string operations config
        self.arithmetic_ops = config.get('arithmetic_ops', [])  # numeric operations config
        self.dbcon = dbcon
        self.out_table = make_table_name('clean')               

    def _sql_ident(self, name):
        return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)) else f'"{name}"'

    def _apply_sql_projection(self, in_table, mapping):
        cols_df = self.dbcon.execute(f"describe {in_table}").fetch_df()
        all_cols = cols_df["column_name"].tolist()
        select_items = []
        for c in all_cols:
            cid = self._sql_ident(c)
            if c in mapping:
                select_items.append(f"{mapping[c]} as {cid}")  # rewrite column expression
            else:
                select_items.append(cid)                       # keep column 
        self.dbcon.execute(
            f"create or replace table {self.out_table} as "
            f"select {', '.join(select_items)} from {in_table}"
        )
        return self.out_table  # new table including transformations

    @staticmethod
    def _split_top_level_commas_static(s):
        out, buf, depth = [], [], 0
        i = 0
        while i < len(s):
            ch = s[i]
            if ch in ("'", '"'):
                q = ch; buf.append(ch); i += 1
                while i < len(s):
                    c2 = s[i]; buf.append(c2)
                    if c2 == q and s[i-1] != '\\': break
                    i += 1
                i += 1; continue
            if ch in "([{": depth += 1
            elif ch in ")]}": depth -= 1
            if ch == ',' and depth == 0:
                out.append(''.join(buf).strip()); buf = []
            else:
                buf.append(ch)
            i += 1
        if buf: out.append(''.join(buf).strip())
        return out  # split args while respecting nesting/quotes

    def _normalize(self, s):
        return str(s).strip().lower()  # for easier comparisons

    def _proper_case_py(self, s):
        if s is None:
            return s
        if not isinstance(s, str):
            s = str(s)
        s = s.lower()
        if not s:
            return s
        s = s[0].upper() + s[1:]  # capitalize first char
        out = []
        prev = None
        for ch in s:
            if prev == ' ' and ch.isalpha():
                out.append(ch.upper())  # capitalize after space
            else:
                out.append(ch)
            prev = ch
        s = ''.join(out)
        for delim in ("-", "'"):
            out2 = []
            prev = None
            for ch in s:
                if prev == delim and ch.isalpha():
                    out2.append(ch.upper())  # capitalize after hyphen/apostrophe
                else:
                    out2.append(ch)
                prev = ch
            s = ''.join(out2)
        return s  # done using python only

    def create_observable(self, upstream):
        def clean_batch(input_table):
            self.dbcon.execute(f"create or replace table {self.out_table} as select * from {input_table}") 
            for op in PIPELINE_OPS:
                op.apply(self)  
            return self.out_table
        return upstream.pipe(ops.map(clean_batch))

    def get_query(self, input_table):
        ops_desc = []  
        if self.drop_nulls:
            ops_desc.append(f"drop nulls in {self.drop_nulls}")
        if self.fill_nulls:
            ops_desc.append(f"fill nulls: {self.fill_nulls}")
        if self.replacements:
            ops_desc.append(f"replacements: {self.replacements}")
        if self.drop_duplicates:
            ops_desc.append(f"drop duplicates {('on ' + str(self.dup_subset)) if self.dup_subset else ''}")
        if self.constraints:
            ops_desc.append(f"constraints: {self.constraints}")
        if self.iqr_outliers:
            ops_desc.append(f"iqr {self.iqr_outliers.get('method', 'remove')}: cols={self.iqr_outliers.get('columns', [])}, k={self.iqr_outliers.get('whisker', 1.5)}")
        if self.fuzzy_detect:
            ops_desc.append(f"fuzzy detect: col={self.fuzzy_detect.get('column')}, out={self.fuzzy_detect.get('output_col', 'similarity_score')}, thr={self.fuzzy_detect.get('threshold', 0.8)}")
        return f"create or replace table {self.out_table} as select * from {input_table} clean: {'; '.join(ops_desc)}" if ops_desc else f"create or replace table {self.out_table} as from {input_table} no cleaning"

    def fill_global_stat(self, col, strategy, cast_as):
        col_cast = f"try_cast({col} as double)" if cast_as in ("double", "int") else col  # numeric-only stats
        if strategy == "mean":
            stat_expr = f"(select avg({col_cast}) from {self.out_table} where {col} is not null)"
        elif strategy == "median":
            stat_expr = f"(select quantile_cont({col_cast}, 0.5) from {self.out_table} where {col} is not null)"
        elif strategy == "mode":
            stat_expr = (
                f"(with m as ("
                f" select {col} as val, count(*) as cnt"
                f" from {self.out_table} where {col} is not null"
                f" group by {col}"
                f" order by cnt desc, val asc"
                f" limit 1"
                f") select val from m)"
            )
        else:
            return  
        self.dbcon.execute(f"update {self.out_table} set {col} = {stat_expr} where {col} is null")  # fill missing
        if cast_as in ("double", "int", "varchar"):
            self.dbcon.execute(f"alter table {self.out_table} alter {col} type {cast_as} using try_cast({col} as {cast_as})")

    def fill_group_stat(self, col, strategy, part_by, cast_as):
        keys = ", ".join(part_by)  # group-by key list
        if strategy == "group_mean":
            stat_sel = f"avg(try_cast({col} as double)) as stat_val"
        elif strategy == "group_median":
            stat_sel = f"quantile_cont(try_cast({col} as double), 0.5) as stat_val"
        elif strategy == "group_mode":
            tmp_mode = f"{self.out_table}_{col}_grp_mode"
            self.dbcon.execute(
                f"""
                create or replace table {tmp_mode} as
                select {keys}, val as stat_val
                from (
                  select {keys}, {col} as val, count(*) as cnt,
                         row_number() over (partition by {keys} order by count(*) desc, {col} asc) as rn
                  from {self.out_table}
                  where {col} is not null
                  group by {keys}, {col}
                ) s
                where rn = 1
                """
            )
            self.dbcon.execute(
                f"""
                update {self.out_table}
                set {col} = gm.stat_val
                from {tmp_mode} gm
                where {" AND ".join([f"{self.out_table}.{k} = gm.{k}" for k in part_by])}
                  and {col} is null
                """
            )
            self.dbcon.execute(f"drop table if exists {tmp_mode}")
            if cast_as in ("double", "int", "varchar"):
                self.dbcon.execute(f"alter table {self.out_table} alter {col} type {cast_as} using try_cast({col} as {cast_as})")
            return
        else:
            return  

        tmp = f"{self.out_table}_{col}_grp_stat"
        self.dbcon.execute(
            f"""
            create or replace table {tmp} as
            select {keys}, {stat_sel}
            from {self.out_table}
            where {col} is not null
            group by {keys}
            """
        )
        self.dbcon.execute(
            f"""
            update {self.out_table}
            set {col} = gs.stat_val
            from {tmp} gs
            where {" AND ".join([f"{self.out_table}.{k} = gs.{k}" for k in part_by])}
              and {col} is null
            """
        )
        self.dbcon.execute(f"drop table if exists {tmp}")
        if cast_as in ("double", "int", "varchar"):
            self.dbcon.execute(f"alter table {self.out_table} alter {col} type {cast_as} using try_cast({col} as {cast_as})")
