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


def sql_ident(name):
    s = str(name)
    return s if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", s) else '"' + s.replace('"', '""') + '"'


def sql_literal(value):
    if value is None:
        return "NULL"
    s = str(value).replace("'", "''")
    return "'" + s + "'"


def combin_udf(n, k):
    if n is None or k is None or n < 0 or k < 0 or n < k:
        return None
    n, k = int(n), int(k)
    if n > 12 or k > 12:
        return None
    result = 1
    for i in range(1, k + 1):
        result = result * (n - k + i) // i
    return result


def permut_udf(n, k):
    if n is None or k is None or n < 0 or k < 0 or n < k:
        return None
    n, k = int(n), int(k)
    if n > 12 or k > 12:
        return None
    result = 1
    for i in range(n - k + 1, n + 1):
        result *= i
    return result


class ArithmeticNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.steps = []
        self.out_table = make_table_name("arith")

        try:
            self.dbcon.create_function("combin", combin_udf, ['BIGINT', 'BIGINT'], 'BIGINT')
            self.dbcon.create_function("permut", permut_udf, ['BIGINT', 'BIGINT'], 'BIGINT')
        except:
            pass

        self._parse_config(config)

        self._op_factory = self._build_op_factory()

    def _build_op_factory(self):
        return {
            "round": lambda a: f"round({a[0]}, {a[1] or 0})",
            "roundup": lambda a: f"ceiling({a[0]})",
            "rounddown": lambda a: f"floor({a[0]})",
            "ceiling": lambda a: (
                f"ceil(greatest(0, {a[0]}))"
                if len(a) == 1
                else f"ceil(greatest(0, {a[0]})) / {a[1]} * {a[1]}"
            ),
            "floor": lambda a: (
                f"floor(greatest(0, {a[0]}))"
                if len(a) == 1
                else f"floor(greatest(0, {a[0]})) / {a[1]} * {a[1]}"
            ),
            "mod": lambda a: f"({a[0]} % {a[1]})",
            "quotient": lambda a: f"cast(greatest(0, {a[0]}) / greatest(1, {a[1]}) as bigint)",
            "int": lambda a: f"floor({a[0]})",
            "trunc": lambda a: f"trunc({a[0]})",
            "sign": lambda a: f"sign({a[0]})",
            "sqrt": lambda a: f"sqrt(greatest(0, {a[0]}))",
            "power": lambda a: f"pow(greatest(0, {a[0]}), {a[1] or 2})",
            "exp": lambda a: f"exp(least(greatest(-10, {a[0]}), 10))",
            "ln": lambda a: f"CASE WHEN {a[0]} > 0 THEN ln({a[0]}) ELSE NULL END",
            "log": lambda a: (lambda base: f"CASE WHEN {a[0]} > 0 THEN log({base}, {a[0]}) ELSE NULL END")(a[1] or "10"),
            "fact": lambda a: f"factorial(CASE WHEN {a[0]} BETWEEN 0 AND 12 THEN CAST({a[0]} AS INTEGER) ELSE 1 END)",
            "combin": lambda a: (
                lambda n, k: f"CASE WHEN {n} IS NOT NULL AND {k} IS NOT NULL AND {n} >= {k} THEN combin({n}, {k}) ELSE NULL END"
            )(
                f"CASE WHEN {a[0]} BETWEEN 0 AND 12 THEN CAST({a[0]} AS INTEGER) ELSE NULL END",
                f"CASE WHEN {a[1]} BETWEEN 0 AND 12 THEN CAST({a[1]} AS INTEGER) ELSE NULL END",
            ),
            "permut": lambda a: (
                lambda n, k: f"CASE WHEN {n} IS NOT NULL AND {k} IS NOT NULL AND {n} >= {k} THEN permut({n}, {k}) ELSE NULL END"
            )(
                f"CASE WHEN {a[0]} BETWEEN 0 AND 12 THEN CAST({a[0]} AS INTEGER) ELSE NULL END",
                f"CASE WHEN {a[1]} BETWEEN 0 AND 12 THEN CAST({a[1]} AS INTEGER) ELSE NULL END",
            ),
            "rand": lambda a: "random()",
            "randbetween": lambda a: f"greatest({a[0]}, least({a[1]}, floor(random() * ({a[1]} - {a[0]} + 1)) + {a[0]}))",
            "pi": lambda a: "pi()",
            "degrees": lambda a: f"degrees({a[0]})",
            "radians": lambda a: f"radians({a[0]})",
            "squareroot": lambda a: f"sqrt(greatest(0, {a[0]}))",
            "cube": lambda a: f"pow({a[0]}, 3)",
        }

    def _parse_config(self, config):
        for c in (config if isinstance(config, list) else [config]):
            if c.get("type") == "arithmetic":
                self.steps = c.get("steps") or []

    def _as_value(self, v):
        if isinstance(v, (int, float)):
            return str(v)
        if v is None:
            return "NULL"
        s = str(v)
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", s):
            return sql_ident(s)
        return sql_literal(s)

    def _expr(self, op, args_sql):
        op = (op or "").lower()
        fn = self._op_factory.get(op)
        if fn is None:
            raise ValueError(f"Unsupported arithmetic op: {op}")
        return fn(args_sql)

    def _apply_step_sql(self, in_tbl, step):
        col = step.get("column")
        op = step.get("op")
        args = step.get("args") or []
        out = step.get("output")
        in_place = bool(step.get("in_place"))
        if not col or not op:
            raise ValueError("Each step requires 'column' and 'op'")

        cols_df = self.dbcon.execute(f"describe {in_tbl}").fetch_df()
        existing = [c for c in cols_df["column_name"].tolist()]
        select_parts = []

        arg_sql = []
        for v in args:
            if isinstance(v, str) and v == "@":
                arg_sql.append(sql_ident(col))
            else:
                arg_sql.append(self._as_value(v))
        if not arg_sql:
            arg_sql = [sql_ident(col)]
        elif "@" not in args and sql_ident(col) not in arg_sql:
            arg_sql = [sql_ident(col)] + arg_sql

        expr_sql = self._expr(op, arg_sql)

        for c in existing:
            if in_place and c == col:
                select_parts.append(f"{expr_sql} as {sql_ident(c)}")
            else:
                select_parts.append(sql_ident(c))

        if not in_place:
            out_col = out or f"{col}_{op}"
            select_parts.append(f"{expr_sql} as {sql_ident(out_col)}")

        sel = ", ".join(select_parts)
        nxt = make_table_name("arith_step")
        self.dbcon.execute(f"create or replace table {nxt} as select {sel} from {in_tbl}")
        return nxt

    def create_observable(self, upstream):
        def run(in_table):
            cur = in_table
            for step in self.steps:
                cur = self._apply_step_sql(cur, step)
            self.dbcon.execute(f"create or replace table {self.out_table} as select * from {cur}")
            return self.out_table

        return upstream.pipe(ops.map(run))

    def get_query(self, input_table):
        return f"select * from {self.out_table}"
