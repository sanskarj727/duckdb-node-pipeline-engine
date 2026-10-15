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



class DeriveFieldsNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.derived = config.get('derivedFields', [])  # list of derived field definitions from UI
        self.out_table = make_table_name('derive')      # table holding original + derived columns



    def _sql_ident(self, name):
        return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)) else f'"{name}"'



    def _split_top_level_commas(self, s):
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
        return out



    def _parse_if_ifs(self, expr):
        # parse Excel IF(cond, true, false) into SQL
        m = re.match(r'^\s*if\s*\((.*)\)\s*$', expr, re.IGNORECASE)
        if m:
            parts = self._split_top_level_commas(m.group(1))
            if len(parts) == 3:
                return f"(case when {parts[0]} then {parts[1]} else {parts[2]} end)"
            return None
        m = re.match(r'^\s*ifs\s*\((.*)\)\s*$', expr, re.IGNORECASE)
        if m:
            parts = self._split_top_level_commas(m.group(1))
            if len(parts) >= 3 and len(parts) % 2 == 1:
                default = parts[-1]
                pairs = parts[:-1]
                whens = []
                for i in range(0, len(pairs), 2):
                    whens.append(f"when {pairs[i]} then {pairs[i+1]}")
                return f"(case {' '.join(whens)} else {default} end)"
        return None



    def _parse_extraction_fn(self, expr):
        m = re.match(
            r'^\s*(left|right|mid)\s*\(\s*([A-Za-z_][\w]*)\s*(?:,\s*([0-9]+)\s*(?:,\s*([0-9]+)\s*)?)?\)\s*$',
            expr, re.IGNORECASE
        )
        if not m:
            return None
        fn, col, a1, a2 = m.groups()
        fn = fn.lower()
        col_id = self._sql_ident(col)
        if fn == "left":
            n = int(a1) if a1 else 0
            return f"substr({col_id}, 1, {n})"
        if fn == "right":
            n = int(a1) if a1 else 0
            start = f"greatest(length({col_id}) - {n} + 1, 1)"
            return f"substr({col_id}, {start}, {n})"
        if fn == "mid":
            start = int(a1) if a1 else 1
            length = int(a2) if a2 else 0
            return f"substr({col_id}, {start}, {length})"
        return None



    def _parse_date_time_fn(self, expr):
        # parse date(y, m, d) into DuckDB make_date()
        def _build_op_factory():
            def _date(args):
                if len(args) != 3:
                    return None
                def _tok(t):
                    return t if re.fullmatch(r'^\d+$', t) else self._sql_ident(t)
                yy, mm, dd = _tok(args[0]), _tok(args[1]), _tok(args[2])
                return f"make_date({yy}, {mm}, {dd})"

            # parse year(col), month(col), day(col)
            def _year(args):
                if len(args) != 1:
                    return None
                return f"extract(year from {self._sql_ident(args[0])})"

            def _month(args):
                if len(args) != 1:
                    return None
                return f"extract(month from {self._sql_ident(args[0])})"

            def _day(args):
                if len(args) != 1:
                    return None
                return f"extract(day from {self._sql_ident(args[0])})"

            def _hour(args):
                if len(args) != 1:
                    return None
                return f"extract(hour from {self._sql_ident(args[0])})"

            def _minute(args):
                if len(args) != 1:
                    return None
                return f"extract(minute from {self._sql_ident(args[0])})"

            def _second(args):
                if len(args) != 1:
                    return None
                return f"extract(second from {self._sql_ident(args[0])})"

            def _monthname(args):
                if len(args) != 1:
                    return None
                col = self._sql_ident(args[0])
                return f"strftime({col}, '%B')"

            def _isoweeknum(args):
                if len(args) != 1:
                    return None
                col = self._sql_ident(args[0])
                return f"cast(strftime({col}, '%V') as integer)"

            def _weeknum(args):
                if len(args) != 1:
                    return None
                col = self._sql_ident(args[0])
                return f"cast(strftime({col}, '%W') as integer)"

            def _days(args):
                if len(args) != 2:
                    return None
                endd = self._sql_ident(args[0])
                startd = self._sql_ident(args[1])
                return f"date_diff('day', {startd}, {endd})"

            def _eomonth(args):
                if len(args) != 2:
                    return None
                dt = self._sql_ident(args[0])
                months = args[1]
                months_sql = months if re.fullmatch(r'^\-?\d+$', str(months)) else self._sql_ident(months)
                return f"last_day({dt} + ({months_sql} * interval '1 month'))"

            return {
                "date": _date,
                "year": _year,
                "month": _month,
                "day": _day,
                "hour": _hour,
                "minute": _minute,
                "second": _second,
                "monthname": _monthname,
                "weeknum": _weeknum,
                "isoweeknum": _isoweeknum,
                "days": _days,
                "eomonth": _eomonth
            }

        m = re.match(r'^\s*([A-Za-z_][\w]*)\s*\((.*)\)\s*$', expr, re.IGNORECASE)
        if not m:
            return None

        op = str(m.group(1)).lower().strip()
        args_raw = m.group(2).strip()
        args = self._split_top_level_commas(args_raw) if args_raw else []

        op_factory = _build_op_factory()
        fn = op_factory.get(op)
        if fn is None:
            return None
        return fn(args)



    def create_observable(self, upstream):
        def do_derive(in_table):
            colinfo = (
                self.dbcon
                .execute(f"PRAGMA table_info({in_table})")
                .fetchdf()
                .set_index('name')['type']
                .to_dict()
            )
            sels = ["*"]  # keep all original columns
            for d in self.derived:
                expr = d['userExpression']      # raw expression from UI
                new_name = d['newFieldName']    # derived column name



                t_if = self._parse_if_ifs(expr)
                if t_if:
                    sels.append(f"{t_if} AS {self._sql_ident(new_name)}")
                    continue



                m_tern = re.match(r'^\(([^)]+)\)\s*\?\s*([^:]+)\s*:\s*(.+)$', expr)
                if m_tern:
                    cond, tval, fval = m_tern.groups()
                    sels.append(
                        f"case when {cond} then {tval} else {fval} end AS {self._sql_ident(new_name)}"
                    )
                    continue



                t_dt = self._parse_date_time_fn(expr)
                if t_dt:
                    sels.append(f"{t_dt} AS {self._sql_ident(new_name)}")
                    continue



                t_ext = self._parse_extraction_fn(expr)
                if t_ext:
                    sels.append(f"{t_ext} AS {self._sql_ident(new_name)}")
                    continue



                m_bin = re.match(r'^\s*([A-Za-z_][\w]*)\s*([+\-/*])\s*([A-Za-z_][\w]*)\s*$', expr) # simple binary arithmetic with implicit casting for strings
                if m_bin:
                    left, op, right = m_bin.groups()
                    lt = (colinfo.get(left, '') or '').upper()
                    rt = (colinfo.get(right, '') or '').upper()
                    l_expr = left if 'CHAR' not in lt else f"cast({self._sql_ident(left)} as double)"
                    r_expr = right if 'CHAR' not in rt else f"cast({self._sql_ident(right)} as double)"
                    sels.append(f"{l_expr} {op} {r_expr} AS {self._sql_ident(new_name)}")
                    continue



                m_logic_and = re.match(r'^\s*AND\s*\((.+)\)\s*$', expr, re.IGNORECASE) # logical AND(expr1, expr2, ...)
                if m_logic_and:
                    inner = m_logic_and.group(1)
                    parts = self._split_top_level_commas(inner)
                    parts = [p for p in parts if p]
                    if parts:
                        bool_expr = "(" + " AND ".join([f"({p})" for p in parts]) + ")"
                        sels.append(f"{bool_expr} AS {self._sql_ident(new_name)}")
                        continue



                m_logic_or = re.match(r'^\s*OR\s*\((.+)\)\s*$', expr, re.IGNORECASE) # logical OR(expr1, expr2, ...)
                if m_logic_or:
                    inner = m_logic_or.group(1)
                    parts = self._split_top_level_commas(inner)
                    parts = [p for p in parts if p]
                    if parts:
                        bool_expr = "(" + " OR ".join([f"({p})" for p in parts]) + ")"
                        sels.append(f"{bool_expr} AS {self._sql_ident(new_name)}")
                        continue



                m_logic_not = re.match(r'^\s*NOT\s*\((.+)\)\s*$', expr, re.IGNORECASE)
                if m_logic_not:
                    inner = m_logic_not.group(1).strip()
                    if inner:
                        bool_expr = f"(NOT ({inner}))"
                        sels.append(f"{bool_expr} AS {self._sql_ident(new_name)}")
                        continue



                sels.append(f"{expr} AS {self._sql_ident(new_name)}")
            sql = f"create or replace table {self.out_table} as select {', '.join(sels)} from {in_table}"
            self.dbcon.execute(sql)
            return self.out_table
        return upstream.pipe(ops.map(do_derive))



    def get_query(self, input_table):
        exprs = []
        for d in self.derived:
            expr = d['userExpression']
            new_name = d['newFieldName']



            t_if = self._parse_if_ifs(expr)
            if t_if:
                exprs.append(f"{t_if} as {new_name}")
                continue



            m = re.match(r'^\(([^)]+)\)\s*\?\s*([^:]+)\s*:\s*(.+)$', expr)
            if m:
                cond, tval, fval = m.groups()
                exprs.append(f"case when {cond} then {tval} else {fval} end as {new_name}")
                continue



            t_dt = self._parse_date_time_fn(expr)
            if t_dt:
                exprs.append(f"{t_dt} as {new_name}")
                continue



            t_ext = self._parse_extraction_fn(expr)
            if t_ext:
                exprs.append(f"{t_ext} as {new_name}")
                continue



            m_logic_and = re.match(r'^\s*AND\s*\((.+)\)\s*$', expr, re.IGNORECASE)
            if m_logic_and:
                inner = m_logic_and.group(1)
                parts = self._split_top_level_commas(inner)
                parts = [p for p in parts if p]
                if parts:
                    bool_expr = "(" + " AND ".join([f"({p})" for p in parts]) + ")"
                    exprs.append(f"{bool_expr} as {new_name}")
                    continue



            m_logic_or = re.match(r'^\s*OR\s*\((.+)\)\s*$', expr, re.IGNORECASE)
            if m_logic_or:
                inner = m_logic_or.group(1)
                parts = self._split_top_level_commas(inner)
                parts = [p for p in parts if p]
                if parts:
                    bool_expr = "(" + " OR ".join([f"({p})" for p in parts]) + ")"
                    exprs.append(f"{bool_expr} as {new_name}")
                    continue



            m_logic_not = re.match(r'^\s*NOT\s*\((.+)\)\s*$', expr, re.IGNORECASE)
            if m_logic_not:
                inner = m_logic_not.group(1).strip()
                if inner:
                    bool_expr = f"(NOT ({inner}))"
                    exprs.append(f"{bool_expr} as {new_name}")
                    continue


            exprs.append(f"{expr} as {new_name}")
        return f"create or replace table {self.out_table} as select *, {', '.join(exprs)} from {input_table}"
