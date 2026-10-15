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

def make_table_name(prefix):
    return f"{prefix}_{uuid4().hex[:8]}"


class SourceNode:
    def __init__(self, config, dbcon, data_source_path, table_name, file_type='csv', delimiter=','):
        self.dbcon = dbcon
        self.data_source_path = data_source_path
        self.table_name = table_name
        self.file_type = file_type
        self.delimiter = delimiter

    def create_observable(self):
        if self.file_type in ("csv", "dsv"):
            sql = (
                f"create or replace table {self.table_name} "
                f"as from read_csv('{self.data_source_path}', header=True, AUTO_DETECT=TRUE, delim='{self.delimiter}')"
            )
            self.dbcon.execute(sql)
        elif str(self.file_type).lower() == "parquet":
            sql = (
                f"create or replace table {self.table_name} "
                f"as from read_parquet('{self.data_source_path}')"
            )
            self.dbcon.execute(sql)
        else:
            pass
        return rx.just(self.table_name)

    def get_query(self, input_table=None):
        if self.file_type in ("csv", "dsv"):
            return (
                f"create or replace table {self.table_name} "
                f"as from read_csv('{self.data_source_path}', header=True, AUTO_DETECT=TRUE, delim='{self.delimiter}')"
            )
        elif str(self.file_type).lower() == "parquet":
            return (
                f"create or replace table {self.table_name} "
                f"as from read_parquet('{self.data_source_path}')"
            )
        return f"-- no-op for fileType={self.file_type}"


class SelectFieldsNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.fields = config.get('fieldSelection', [])
        self.out_table = make_table_name("select")

    def create_observable(self, upstream):
        def do_select(in_table):
            sql = f"create or replace table {self.out_table} as select {', '.join(self.fields)} from {in_table}"
            self.dbcon.execute(sql)
            return self.out_table
        return upstream.pipe(ops.map(do_select))

    def get_query(self, input_table):
        return f"create or replace table {self.out_table} as select {', '.join(self.fields)} from {input_table}"


class FilterNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.expr = config.get('userExpression')
        self.out_table = make_table_name("filter")

    def create_observable(self, upstream):
        def do_filter(in_table):
            expr = self.expr
            m = re.match(r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([^\']+)\s*$', expr)
            if m:
                field, value = m.groups()
                try:
                    colinfo = self.dbcon.execute(f"PRAGMA table_info({in_table})").fetchdf()
                    coltype = colinfo.set_index('name').loc[field, 'type']
                    if 'CHAR' in coltype.upper() and not (value.strip().startswith("'") or value.strip().startswith('"')):
                        expr = f"{field} = '{value.strip()}'"
                except Exception:
                    pass
            sql = f"create or replace table {self.out_table} as from {in_table} where {expr}"
            self.dbcon.execute(sql)
            return self.out_table
        return upstream.pipe(ops.map(do_filter))

    def get_query(self, input_table):
        expr = self.expr
        m = re.match(r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([^\']+)\s*$', expr)
        if m:
            field, value = m.groups()
            if not (value.strip().startswith("'") or value.strip().startswith('"')):
                expr = f"{field} = '{value.strip()}'"
        return f"create or replace table {self.out_table} as from {input_table} where {expr}"


class DeriveFieldsNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.derived = config.get('derivedFields', [])
        self.out_table = make_table_name('derive')

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
        m = re.match(r'^\s*(left|right|mid)\s*\(\s*([A-Za-z_][\w]*)\s*(?:,\s*([0-9]+)\s*(?:,\s*([0-9]+)\s*)?)?\)\s*$', expr, re.IGNORECASE)
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
        m = re.match(r'^\s*date\s*\(\s*([A-Za-z_][\w]*|\d+)\s*,\s*([A-Za-z_][\w]*|\d+)\s*,\s*([A-Za-z_][\w]*|\d+)\s*\)\s*$', expr, re.IGNORECASE)
        if m:
            y, mo, d = m.groups()
            def _tok(t):
                return t if re.fullmatch(r'^\d+$', t) else self._sql_ident(t)
            yy, mm, dd = _tok(y), _tok(mo), _tok(d)
            return f"make_date({yy}, {mm}, {dd})"
        m = re.match(r'^\s*year\s*\(\s*([A-Za-z_][\w]*)\s*\)\s*$', expr, re.IGNORECASE)
        if m:
            col = self._sql_ident(m.group(1))
            return f"extract(year from {col})"
        m = re.match(r'^\s*month\s*\(\s*([A-Za-z_][\w]*)\s*\)\s*$', expr, re.IGNORECASE)
        if m:
            col = self._sql_ident(m.group(1))
            return f"extract(month from {col})"
        m = re.match(r'^\s*day\s*\(\s*([A-Za-z_][\w]*)\s*\)\s*$', expr, re.IGNORECASE)
        if m:
            col = self._sql_ident(m.group(1))
            return f"extract(day from {col})"
        return None

    def create_observable(self, upstream):
        def do_derive(in_table):
            colinfo = self.dbcon.execute(f"PRAGMA table_info({in_table})").fetchdf().set_index('name')['type'].to_dict()
            sels = ["*"]
            for d in self.derived:
                expr = d['userExpression']
                new_name = d['newFieldName']

                t_if = self._parse_if_ifs(expr)
                if t_if:
                    sels.append(f"{t_if} AS {self._sql_ident(new_name)}")
                    continue

                m_tern = re.match(r'^\(([^)]+)\)\s*\?\s*([^:]+)\s*:\s*(.+)$', expr)
                if m_tern:
                    cond, tval, fval = m_tern.groups()
                    sels.append(f"case when {cond} then {tval} else {fval} end AS {self._sql_ident(new_name)}")
                    continue

                t_dt = self._parse_date_time_fn(expr)
                if t_dt:
                    sels.append(f"{t_dt} AS {self._sql_ident(new_name)}")
                    continue

                t_ext = self._parse_extraction_fn(expr)
                if t_ext:
                    sels.append(f"{t_ext} AS {self._sql_ident(new_name)}")
                    continue

                m_bin = re.match(r'^\s*([A-Za-z_][\w]*)\s*([+\-/*])\s*([A-Za-z_][\w]*)\s*$', expr)
                if m_bin:
                    left, op, right = m_bin.groups()
                    lt = (colinfo.get(left, '') or '').upper()
                    rt = (colinfo.get(right, '') or '').upper()
                    l_expr = left if 'CHAR' not in lt else f"cast({self._sql_ident(left)} as double)"
                    r_expr = right if 'CHAR' not in rt else f"cast({self._sql_ident(right)} as double)"
                    sels.append(f"{l_expr} {op} {r_expr} AS {self._sql_ident(new_name)}")
                    continue

                m_logic_and = re.match(r'^\s*AND\s*\((.+)\)\s*$', expr, re.IGNORECASE)
                if m_logic_and:
                    inner = m_logic_and.group(1)
                    parts = self._split_top_level_commas(inner)
                    parts = [p for p in parts if p]
                    if parts:
                        bool_expr = "(" + " AND ".join([f"({p})" for p in parts]) + ")"
                        sels.append(f"{bool_expr} AS {self._sql_ident(new_name)}")
                        continue

                m_logic_or = re.match(r'^\s*OR\s*\((.+)\)\s*$', expr, re.IGNORECASE)
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



class SortNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.sort_fields = config.get('sortFields', [])
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
                        return c
                return None

            def resolve_actual_col(fid, new_name=None):
                if fid in colset:
                    return fid
                if isinstance(new_name, str) and new_name in colset:
                    return new_name
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

                if idx == 0 and str(actual_col).lower() == "wins":
                    primary_is_wins = True

            if primary_is_wins and winner_col:
                sort_terms.append(f"{self._safe_ident(winner_col)} ASC")

            if not sort_terms:
                sql = f"create or replace table {self.out_table} as select * from {in_table}"
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


class MergeNode:
    def __init__(self, config, dbcon):
        self.operation = config.get('operation', 'union')
        self.keep_duplicates = config.get('keep_duplicates', True)
        self.dbcon = dbcon
        self.out_table = make_table_name('merge')

    def create_observable(self, left_obs, right_obs):
        def merge_batches(pair):
            l, r = pair
            if self.operation == "union":
                sql = f"create or replace table {self.out_table} as select * from {l} union{' all' if self.keep_duplicates else ''} select * from {r}"
            elif self.operation == "intersection":
                sql = f"create or replace table {self.out_table} as select * from {l} intersect select * from {r}"
            elif self.operation == "difference":
                sql = f"create or replace table {self.out_table} as select * from {l} except select * from {r}"
            else:
                raise NotImplementedError(f"Unsupported merge operation: {self.operation}")
            self.dbcon.execute(sql)
            return self.out_table
        return rx.zip(left_obs, right_obs).pipe(ops.map(merge_batches))

    def get_query(self, left_table, right_table):
        if self.operation == "union":
            return f"create or replace table {self.out_table} as select * from {left_table} union{' all' if self.keep_duplicates else ''} select * from {right_table}"
        elif self.operation == "intersection":
            return f"create or replace table {self.out_table} as select * from {left_table} intersect select * from {right_table}"
        elif self.operation == "difference":
            return f"create or replace table {self.out_table} as select * from {left_table} except select * from {right_table}"
        else:
            raise NotImplementedError(f"Unsupported merge operation: {self.operation}")


class JoinNode:
    def __init__(self, config, dbcon):
        self.left_on = config.get('left_on', [])
        self.right_on = config.get('right_on', [])
        self.join_type = config.get('join_type', 'inner')
        self.suffixes = tuple(config.get('suffixes', ['_left', '_right']))
        self.dbcon = dbcon
        self.out_table = make_table_name('join')

    def create_observable(self, left_obs, right_obs):
        def join_batches(pair):
            l, r = pair
            on_clause = " AND ".join([f"l.{lk} = r.{rk}" for lk, rk in zip(self.left_on, self.right_on)])
            sql = f"create or replace table {self.out_table} as select * from {l} as l {self.join_type} join {r} as r on {on_clause}"
            self.dbcon.execute(sql)
            return self.out_table
        return rx.zip(left_obs, right_obs).pipe(ops.map(join_batches))

    def get_query(self, left_table, right_table):
        on_clause = " AND ".join([f"l.{lk} = r.{rk}" for lk, rk in zip(self.left_on, self.right_on)])
        return f"create or replace table {self.out_table} as select * from {left_table} as l {self.join_type} join {right_table} as r on {on_clause}"


def make_table_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def make_table_name(prefix):
    return f"{prefix}_{uuid4().hex[:8]}"
'''
class Operation:
    def apply(self, node): raise NotImplementedError

class ReplacementsOp(Operation):
    def apply(self, node):
        for col, val_map in node.replacements.items():
            for old_val, new_val in val_map.items():
                node.dbcon.execute(f"update {node.out_table} set {col} = '{new_val}' where {col} = '{old_val}'")

class FillNullsOp(Operation):
    def apply(self, node):
        for col, cfg in node.fill_nulls.items():
            if not isinstance(cfg, dict):
                val = cfg
                v_expr = "NULL" if val is None or val == "NULL" else f"'{val}'"
                node.dbcon.execute(f"update {node.out_table} set {col} = {v_expr} where {col} is null")
                continue
            strategy = (cfg.get("strategy") or "").lower().strip()
            cast_as = cfg.get("cast_as")
            part_by = cfg.get("partition_by") or []
            if strategy in ("", "constant"):
                v = cfg.get("value")
                v_expr = "NULL" if v is None or v == "NULL" else f"'{v}'"
                node.dbcon.execute(f"update {node.out_table} set {col} = {v_expr} where {col} is null")
            elif strategy in ("mean", "median", "mode"):
                node.fill_global_stat(col, strategy, cast_as)
            elif strategy in ("group_mean", "group_median", "group_mode"):
                if part_by:
                    node.fill_group_stat(col, strategy, part_by, cast_as)
            elif strategy == "fallback":
                for step in cfg.get("steps", []):
                    st = (step.get("strategy") or "").lower().strip()
                    if not st: continue
                    if st in ("", "constant"):
                        v = step.get("value")
                        v_expr = "NULL" if v is None or v == "NULL" else f"'{v}'"
                        node.dbcon.execute(f"update {node.out_table} set {col} = {v_expr} where {col} is null")
                    elif st in ("mean", "median", "mode"):
                        node.fill_global_stat(col, st, cast_as)
                    elif st in ("group_mean", "group_median", "group_mode"):
                        pb = step.get("partition_by") or []
                        if pb:
                            node.fill_group_stat(col, st, pb, cast_as)

class DropNullsOp(Operation):
    def apply(self, node):
        if not node.drop_nulls: return
        conds = [f"{c} is not null" for c in node.drop_nulls]
        where_clause = " AND ".join(conds)
        node.dbcon.execute(f"delete from {node.out_table} where not ({where_clause})")

class DropDuplicatesOp(Operation):
    def apply(self, node):
        if not node.drop_duplicates: return
        subset_cols = node.dup_subset if isinstance(node.dup_subset, list) else [node.dup_subset] if node.dup_subset else []
        if not subset_cols:
            subset_cols = [desc for desc in node.dbcon.execute(f"pragma table_info('{node.out_table}')").fetchall()]
        cols_str = ", ".join(subset_cols)
        tmp_table = f"{node.out_table}_dedup"
        node.dbcon.execute(
            f"create or replace table {tmp_table} as "
            f"select * from (select *, row_number() over (partition by {cols_str}) as rn from {node.out_table}) where rn = 1"
        )
        node.dbcon.execute(f"drop table {node.out_table}")
        node.dbcon.execute(f"alter table {tmp_table} rename to {node.out_table}")

class ConstraintsOp(Operation):
    def apply(self, node):
        for col, constraint in node.constraints.items():
            if isinstance(constraint, dict):
                if 'min' in constraint:
                    node.dbcon.execute(f"delete from {node.out_table} where {col} < {constraint['min']}")
                if 'max' in constraint:
                    node.dbcon.execute(f"delete from {node.out_table} where {col} > {constraint['max']}")
            elif isinstance(constraint, list):
                vals = ", ".join([f"'{v}'" for v in constraint])
                node.dbcon.execute(f"delete from {node.out_table} where {col} not in ({vals})")

class IQROp(Operation):
    def apply(self, node):
        cfg = node.iqr_outliers
        if not (cfg and isinstance(cfg, dict)): return
        cols = cfg.get("columns", [])
        method = cfg.get("method", "remove")
        whisker = cfg.get("whisker", 1.5)
        if not cols: return
        colinfo_df = node.dbcon.execute(f"PRAGMA table_info({node.out_table})").fetchdf()
        schema_types = {row['name']: str(row['type']).upper() for _, row in colinfo_df.iterrows()}
        for c in cols:
            node.dbcon.execute(
                f"""
                create or replace table {node.out_table} as
                with q as (
                  select
                    *,
                    quantile_cont(try_cast({c} as double), 0.25) over () as q1_{c},
                    quantile_cont(try_cast({c} as double), 0.75) over () as q3_{c}
                  from {node.out_table}
                ),
                b as (
                  select
                    *,
                    (q3_{c} - q1_{c}) as iqr_{c},
                    (q1_{c} - {whisker} * (q3_{c} - q1_{c})) as lower_{c},
                    (q3_{c} + {whisker} * (q3_{c} - q1_{c})) as upper_{c}
                  from q
                )
                select * from b
                """
            )
            if method == "remove":
                node.dbcon.execute(
                    f"""
                    delete from {node.out_table}
                    where try_cast({c} as double) < (select min(lower_{c}) from {node.out_table})
                       or try_cast({c} as double) > (select max(upper_{c}) from {node.out_table})
                    """
                )
            elif method == "winsorize":
                tmp_wins = f"{node.out_table}_wins_{uuid4().hex[:6]}"
                node.dbcon.execute(
                    f"""
                    create or replace table {tmp_wins} as
                    select
                      {", ".join([f"case when try_cast({c} as double) < lower_{c} then lower_{c} "
                                  f"when try_cast({c} as double) > upper_{c} then upper_{c} "
                                  f"else {c} end as {c}" if name == c else name
                                  for name in schema_types.keys()])}
                    from {node.out_table}
                    """
                )
                node.dbcon.execute(f"drop table {node.out_table}")
                node.dbcon.execute(f"alter table {tmp_wins} rename to {node.out_table}")
            cols_only = ", ".join(schema_types.keys())
            node.dbcon.execute(
                f"create or replace table {node.out_table} as select {cols_only} from {node.out_table}"
            )

class FuzzyDetectOp(Operation):
    def apply(self, node):
        cfg = getattr(node, "fuzzy_detect", None)
        if not (cfg and isinstance(cfg, dict)): return
        column = cfg.get('column')
        new_col = cfg.get('output_col', 'similarity_score')
        threshold = cfg.get('threshold', 0.8)
        label_col = cfg.get('label_col')
        if not column: return
        label_expr = f"b.{column}" if label_col else "NULL"
        similarity_expr = (
            f"(1-cast(levenshtein(a.{column}, b.{column}) as double) "
            f"/ greatest(length(a.{column}), length(b.{column})))"
        )
        select_clause = (
            f"a.*, m.sim_score as {new_col}"
            + (f", m.sim_label as {label_col}" if label_col else "")
        )
        where_clause = f"a.{column} != b.{column}"
        sql = f"""
        create or replace table {node.out_table} as
        select {select_clause}
        from {node.out_table} a
        left join (
            select a_rowid, sim_score, sim_label from (
                select
                    a.rowid as a_rowid,
                    {similarity_expr} as sim_score,
                    {label_expr} as sim_label,
                    row_number() over (partition by a.rowid order by (1-cast(levenshtein(a.{column}, b.{column}) as double)/greatest(length(a.{column}), length(b.{column}))) desc) as rn
                from {node.out_table} a
                join {node.out_table} b
                on {where_clause}
            )
            where rn = 1
        ) m
        on a.rowid = m.a_rowid
        """
        node.dbcon.execute(sql)
        if threshold is not None:
            node.dbcon.execute(
                f"create or replace table {node.out_table} as select * from {node.out_table} where {new_col} >= {float(threshold)}"
            )

class TrimUpperLowerOp(Operation):
    def apply(self, node):
        ops = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops: return
        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        mapping = {}
        for cfg in ops:
            op = str(cfg.get("op","")).lower().strip()
            cols = [c for c in cfg.get("columns", []) if c in all_cols]
            if not cols and op != "proper": continue
            if op == "trim":
                for c in cols: mapping[c] = f"trim({node._sql_ident(c)})"
            if op == "upper":
                for c in cols: mapping[c] = f"upper({node._sql_ident(c)})"
            if op == "lower":
                for c in cols: mapping[c] = f"lower({node._sql_ident(c)})"
        if mapping:
            node._apply_sql_projection(node.out_table, mapping)

class SubstituteReplaceValueTextOp(Operation):
    def apply(self, node):
        ops = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops: return
        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        mapping = {}
        for cfg in ops:
            op = str(cfg.get("op","")).lower().strip()
            cols = [c for c in cfg.get("columns", []) if c in all_cols]
            if not cols and op != "proper": continue
            if op == "substitute":
                po = (cfg.get("old") or "").replace("'","''")
                pn = (cfg.get("new") or "").replace("'","''")
                for c in cols: mapping[c] = f"replace({node._sql_ident(c)}, '{po}', '{pn}')"
            if op == "replace":
                start = int(cfg.get("start",1)); length = int(cfg.get("length",0)); nt = (cfg.get("new_text","")).replace("'","''")
                for c in cols:
                    cid = node._sql_ident(c)
                    leftp = f"case when {start} > 1 then substr({cid}, 1, {start}-1) else '' end"
                    rstart = f"case when {start}+{length} <= length({cid}) then {start}+{length} else length({cid})+1 end"
                    rightp = f"case when {rstart} <= length({cid}) then substr({cid}, {rstart}) else '' end"
                    mapping[c] = f"({leftp} || '{nt}' || {rightp})"
            if op == "value":
                tt = str(cfg.get("target_type","DOUBLE")).upper()
                for c in cols: mapping[c] = f"try_cast({node._sql_ident(c)} as {tt})"
            if op == "text":
                fmt = cfg.get("fmt")
                for c in cols:
                    cid = node._sql_ident(c)
                    if fmt:
                        fmt_esc = fmt.replace("'","''")
                        m = re.match(r'%\.([0-9]+)f$', fmt.strip())
                        if m:
                            d = int(m.group(1))
                            mapping[c] = f"printf('%.{d}f', cast({cid} as DOUBLE))"
                        else:
                            mapping[c] = f"printf('{fmt_esc}', cast({cid} as DOUBLE))"
                    else:
                        mapping[c] = f"cast({cid} as VARCHAR)"
        if mapping:
            node._apply_sql_projection(node.out_table, mapping)

class ExtractOps(Operation):
    def apply(self, node):
        ops = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops: return
        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        mapping = {}
        for cfg in ops:
            op = str(cfg.get("op","")).lower().strip()
            cols = [c for c in cfg.get("columns", []) if c in all_cols]
            if not cols: continue
            if op == "left":
                n = int(cfg.get("n",0))
                for c in cols:
                    mapping[c] = f"substr({node._sql_ident(c)}, 1, {n})"
            if op == "right":
                n = int(cfg.get("n",0))
                for c in cols:
                    cid = node._sql_ident(c)
                    mapping[c] = f"substr({cid}, greatest(length({cid}) - {n} + 1, 1), {n})"
            if op == "mid":
                start = int(cfg.get("start",1)); length = int(cfg.get("length",0))
                for c in cols:
                    mapping[c] = f"substr({node._sql_ident(c)}, {start}, {length})"
        if mapping:
            node._apply_sql_projection(node.out_table, mapping)

class ConditionalOps(Operation):
    def apply(self, node):
        ops = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops: return
        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        mapping = {}
        for cfg in ops:
            op = str(cfg.get("op","")).lower().strip()

            if op in ("if", "ifs"):
                cols = [c for c in cfg.get("columns", []) if c in all_cols]
                expr = str(cfg.get("expr","")).strip()
                if not (cols and expr):
                    continue
                case_expr = self._to_case(expr)
                if not case_expr:
                    continue
                for c in cols:
                    mapping[c] = case_expr
                continue

            if op in ("and", "or"):
                target = cfg.get("target")
                exprs = cfg.get("exprs") or []
                exprs = [str(e).strip() for e in exprs if str(e).strip()]
                if not (target and exprs):
                    continue
                joiner = " AND " if op == "and" else " OR "
                bool_expr = "(" + joiner.join([f"({e})" for e in exprs]) + ")"
                mapping[target] = bool_expr
                continue

            if op == "not":
                target = cfg.get("target")
                expr = str(cfg.get("expr","")).strip()
                if not (target and expr):
                    continue
                bool_expr = f"(NOT ({expr}))"
                mapping[target] = bool_expr
                continue

        if mapping:
            node._apply_sql_projection(node.out_table, mapping)

    def _to_case(self, expr):
        m = re.match(r'^\s*if\s*\((.*)\)\s*$', expr, re.IGNORECASE)
        if m:
            parts = CleaningNode._split_top_level_commas_static(m.group(1))
            if len(parts) == 3:
                return f"(case when {parts[0]} then {parts[1]} else {parts[2]} end)"
            return None
        m = re.match(r'^\s*ifs\s*\((.*)\)\s*$', expr, re.IGNORECASE)
        if m:
            parts = CleaningNode._split_top_level_commas_static(m.group(1))
            if len(parts) >= 3 and len(parts) % 2 == 1:
                default = parts[-1]
                pairs = parts[:-1]
                whens = []
                for i in range(0, len(pairs), 2):
                    whens.append(f"when {pairs[i]} then {pairs[i+1]}")
                return f"(case {' '.join(whens)} else {default} end)"
        return None



class ProperCaseOp(Operation):
    def apply(self, node):
        ops = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops: return
        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        targets = []
        for cfg in ops:
            if str(cfg.get("op","")).lower().strip() == "proper":
                cols = [c for c in cfg.get("columns", []) if c in all_cols]
                targets.extend(cols)
        if not targets: return
        data = node.dbcon.execute(f"select * from {node.out_table}").fetch_df()
        for c in targets:
            data[c] = data[c].astype("string").map(node._proper_case_py)
        node.dbcon.register("clean_py_df", data[data.columns.tolist()])
        node.dbcon.execute(f"create or replace table {node.out_table} as select * from clean_py_df")

class ArithmeticOpsOp(Operation):
    def apply(self, node):
        ops = getattr(node, "arithmetic_ops", [])
        if not ops:
            return
        cols_df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = cols_df["column_name"].tolist()
        mapping = {}
        for cfg in ops:
            op = str(cfg.get("op", "")).lower().strip()
            cols = [c for c in cfg.get("columns", []) if c in all_cols]
            args = cfg.get("args") or []
            output = cfg.get("output")
            in_place = bool(cfg.get("in_place", False))
            if not cols or not op:
                continue

            def a_val(arg):
                if isinstance(arg, (int, float)):
                    return str(arg)
                if arg in all_cols:
                    return node._sql_ident(arg)
                return str(arg)

            for c in cols:
                cid = node._sql_ident(c)
                arg_sql = []
                for v in args:
                    if isinstance(v, str) and v == "@":
                        arg_sql.append(cid)
                    else:
                        arg_sql.append(a_val(v))
                if not arg_sql:
                    arg_sql = [cid]
                elif "@" not in args and cid not in arg_sql:
                    arg_sql = [cid] + arg_sql

                if op in ("quotient", "mod", "power", "pow") and len(arg_sql) < 2:
                    continue

                expr = None
                if op == "ceiling":
                    expr = f"ceil({arg_sql[0]})" if len(arg_sql) == 1 else f"ceil({arg_sql[0]} / {arg_sql[1]}) * {arg_sql[1]}"
                elif op == "floor":
                    expr = f"floor({arg_sql[0]})" if len(arg_sql) == 1 else f"floor({arg_sql[0]} / {arg_sql[1]}) * {arg_sql[1]}"
                elif op == "mod":
                    expr = f"({arg_sql[0]} % {arg_sql[1]})"
                elif op == "quotient":
                    expr = f"cast(({arg_sql[0]}) / ({arg_sql[1]}) as bigint)"
                elif op == "int":
                    expr = f"floor({arg_sql[0]})"
                elif op == "trunc":
                    expr = f"trunc({arg_sql[0]})"
                elif op == "sign":
                    expr = f"sign({arg_sql[0]})"
                elif op == "sqrt":
                    expr = f"sqrt({arg_sql[0]})"
                elif op in ("power", "pow"):
                    expr = f"pow({arg_sql[0]}, {arg_sql[1]})"
                if expr:
                    target = c if in_place else (output or f"{c}_{op}")
                    mapping[target] = expr
        if mapping:
            node._apply_sql_projection(node.out_table, mapping)

class ValidationOpsOp(Operation):
    def apply(self, node):
        ops = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops:
            return

        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        select_items = [node._sql_ident(c) for c in all_cols]

        for cfg in ops:
            op = str(cfg.get("op", "")).lower().strip()
            if op not in ("isblank", "isnumber", "istext", "isnontext", "iserror"):
                continue
            cols = cfg.get("columns") or []
            target = cfg.get("output")
            for c in cols:
                if c not in all_cols:
                    continue
                cid = node._sql_ident(c)
                if op == "isblank":
                    expr = f"({cid} is null)"
                    out_name = target or f"IsBlank_{c}"
                elif op == "isnumber":
                    expr = f"(try_cast({cid} as double) is not null)"
                    out_name = target or f"IsNumber_{c}"
                elif op == "istext":
                    expr = f"({cid} is not null and try_cast({cid} as double) is null)"
                    out_name = target or f"IsText_{c}"
                elif op == "isnontext":
                    expr = f"({cid} is null or try_cast({cid} as double) is not null)"
                    out_name = target or f"IsNonText_{c}"
                elif op == "iserror":
                    expr = "(false)"
                    out_name = target or f"IsError_{c}"
                else:
                    continue
                select_items.append(f"{expr} as {node._sql_ident(out_name)}")

        if len(select_items) == len(all_cols):
            return

        sql = (
            f"create or replace table {node.out_table} as "
            f"select {', '.join(select_items)} from {node.out_table}"
        )
        node.dbcon.execute(sql)


PIPELINE_OPS = [
    ReplacementsOp(),
    FillNullsOp(),
    DropNullsOp(),
    DropDuplicatesOp(),
    ConstraintsOp(),
    IQROp(),
    FuzzyDetectOp(),
    TrimUpperLowerOp(),
    SubstituteReplaceValueTextOp(),
    ArithmeticOpsOp(),
    ExtractOps(),
    ConditionalOps(),
    ProperCaseOp(),
    ValidationOpsOp()
]
'''
# prefer sequence of operations
# order of actions within a single node call

from src.element.clean_ops import PIPELINE_OPS

class CleaningNode:
    def __init__(self, config, dbcon):
        self.drop_nulls = config.get('drop_nulls', [])
        self.fill_nulls = config.get('fill_nulls', {})
        self.replacements = config.get('replacements', {})
        self.drop_duplicates = config.get('drop_duplicates', False)
        self.dup_subset = config.get('dup_subset', None)
        self.constraints = config.get('constraints', {})
        self.iqr_outliers = config.get('iqr_outliers', None)
        self.fuzzy_detect = config.get('fuzzy_detect', None)
        self.operations = config.get('operations', [])
        self.text_ops = config.get('text_ops', [])
        self.arithmetic_ops = config.get('arithmetic_ops', [])
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
                select_items.append(f"{mapping[c]} as {cid}")
            else:
                select_items.append(cid)
        self.dbcon.execute(
            f"create or replace table {self.out_table} as "
            f"select {', '.join(select_items)} from {in_table}"
        )
        return self.out_table

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
        return out

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
            out2 = []
            prev = None
            for ch in s:
                if prev == delim and ch.isalpha():
                    out2.append(ch.upper())
                else:
                    out2.append(ch)
                prev = ch
            s = ''.join(out2)
        return s

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
        col_cast = f"try_cast({col} as double)" if cast_as in ("double", "int") else col
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
        self.dbcon.execute(f"update {self.out_table} set {col} = {stat_expr} where {col} is null")
        if cast_as in ("double", "int", "varchar"):
            self.dbcon.execute(f"alter table {self.out_table} alter {col} type {cast_as} using try_cast({col} as {cast_as})")

    def fill_group_stat(self, col, strategy, part_by, cast_as):
        keys = ", ".join(part_by)
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


class FuzzyDetectionNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.column = config.get('column')              
        self.new_col = config.get('output_col', 'similarity_score')
        self.threshold = float(config.get('threshold', 0.8))  
        self.label_col = config.get('label_col', None)  
        self.out_table = make_table_name("fuzzy")

    def create_observable(self, upstream):
        def do_fuzzy(in_table):
            label_expr = f"b.{self.column}" if self.label_col else "NULL"
            similarity_expr = (
                f"(1-cast(levenshtein(a.{self.column}, b.{self.column}) as double) "
                f"/ greatest(length(a.{self.column}), length(b.{self.column})))"
            )
            select_clause = (
                f"a.*, m.sim_score as {self.new_col}"
                + (f", m.sim_label as {self.label_col}" if self.label_col else "")
            )
            where_clause = f"a.{self.column} != b.{self.column}"
            sql = f"""
            create or replace table {self.out_table} as
            select {select_clause}
            from {in_table} a
            left join (
                select a_rowid, sim_score, sim_label from (
                    select
                        a.rowid as a_rowid,
                        {similarity_expr} as sim_score,
                        {label_expr} as sim_label,
                        row_number() over (partition by a.rowid order by (1-cast(levenshtein(a.{self.column}, b.{self.column}) as double)/greatest(length(a.{self.column}), length(b.{self.column}))) desc) as rn
                    from {in_table} a
                    join {in_table} b
                    on {where_clause}
                )
                where rn = 1
            ) m
            on a.rowid = m.a_rowid
            """
            self.dbcon.execute(sql)
            if self.threshold is not None:
                self.dbcon.execute(
                    f"create or replace table {self.out_table} as select * from {self.out_table} where {self.new_col} >= {self.threshold}"
                )
            return self.out_table
        return upstream.pipe(ops.map(do_fuzzy))

    def get_query(self, input_table):
        sim = self.new_col
        lbl = self.label_col if self.label_col else "most_similar"
        return f"create or replace table {self.out_table} -- computes {sim} and {lbl} for each {self.column} by fuzzy self-join"
    

class TrainNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.features = list(config.get("features", []))
        self.target = config.get("target")
        self.standardize = bool(config.get("standardize", True))
        self.test_size = float(config.get("test_size", 0.2))
        self.random_state = int(config.get("random_state", 42))
        self.model = str(config.get("model", "logreg")).strip().lower()
        self.model_params = dict(config.get("model_params", {}))
        self.model_path = config.get("model_path")
        self.model_path_versioned = bool(config.get("model_path_versioned", False))
        self.train_returns = str(config.get("train_returns", "metrics")).strip().lower()
        self.out_table = make_table_name("cls_pred")
        self.eval_table = make_table_name("cls_eval")
        self.metrics_table = make_table_name("cls_metrics")

    def _make_estimator(self):
        if self.model == "logreg":
            allowed = {"C", "solver", "max_iter", "random_state"}
            params = {"solver": "lbfgs", "max_iter": 1000, "random_state": self.random_state}
            params.update({k: v for k, v in self.model_params.items() if k in allowed})
            return LogisticRegression(**params)
        if self.model in ("random_forest", "randomforest"):
            allowed = {"n_estimators", "max_depth", "random_state"}
            params = {"n_estimators": 300, "random_state": self.random_state}
            params.update({k: v for k, v in self.model_params.items() if k in allowed})
            return RandomForestClassifier(**params)
        if self.model in ("linear_svc", "linearsvc"):
            allowed = {"C", "max_iter", "random_state"}
            params = {"C": 1.0, "max_iter": 2000, "random_state": self.random_state}
            params.update({k: v for k, v in self.model_params.items() if k in allowed})
            return LinearSVC(**params)
        if self.model in ("gbt", "gradient_boosting"):
            allowed = {"n_estimators", "learning_rate", "max_depth", "random_state"}
            params = {"n_estimators": 200, "learning_rate": 0.05, "random_state": self.random_state}
            params.update({k: v for k, v in self.model_params.items() if k in allowed})
            return GradientBoostingClassifier(**params)
        raise ValueError(f"Unsupported model: {self.model}")

    def _build_preprocessor(self, X_df):
        num_cols = [c for c in X_df.columns if np.issubdtype(X_df[c].dtype, np.number)]
        cat_cols = [c for c in X_df.columns if c not in num_cols]
        transformers = []
        if cat_cols:
            transformers.append(("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols))
        if num_cols:
            transformers.append(("num", StandardScaler() if self.standardize else FunctionTransformer(lambda x: x), num_cols))
        return ColumnTransformer(transformers=transformers, remainder="drop") if transformers else None

    def _make_pipeline(self, preprocessor, estimator):
        steps = []
        if preprocessor is not None:
            steps.append(("prep", preprocessor))
        steps.append(("clf", estimator))
        return Pipeline(steps)

    def _ensure_dir(self, path):
        if path:
            d = path if os.path.isdir(path) else os.path.dirname(path)
            if d and not os.path.exists(d):
                os.makedirs(d, exist_ok=True)

    def _versioned_path(self, base_path):
        if not self.model_path_versioned or not base_path:
            return base_path
        root, ext = os.path.splitext(base_path)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{root}_{ts}{ext}" if ext else f"{base_path}_{ts}"

    def _save_schema(self, save_path, X_df):
        meta = {
            "features": list(X_df.columns),
            "target": self.target,
            "dtypes": {c: str(X_df[c].dtype) for c in X_df.columns},
            "target_dtype": "string"
        }
        with open(save_path + ".schema.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

    def get_query(self, input_table):
        return f"-- train on {input_table} using {self.model}; outputs {self.train_returns}"

    def create_observable(self, upstream):
        def run_training(in_table):
            if not self.features:
                raise ValueError("features must be provided")
            if not self.target:
                raise ValueError("target must be provided")

            cols = list(self.features) + [self.target]
            cols_csv = ", ".join([f'"{c}"' if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(c)) else c for c in cols])
            df = self.dbcon.execute(f"select {cols_csv} from {in_table}").fetch_df()

            X_df = df[self.features].copy()
            y = df[self.target].astype(str).values
            counts = pd.Series(y).value_counts()
            stratify_var = y if len(counts) >= 2 and (counts.min() >= 2) else None

            X_tr, X_te, y_tr, y_te = train_test_split(
                X_df, y, test_size=self.test_size, random_state=self.random_state, stratify=stratify_var
            )

            preprocessor = self._build_preprocessor(X_tr)
            estimator = self._make_estimator()
            pipe = self._make_pipeline(preprocessor, estimator)

            pipe.fit(X_tr, y_tr)
            y_pred = pipe.predict(X_te)

            self.dbcon.register("pred_df", pd.DataFrame(y_pred, columns=["predicted"]))
            self.dbcon.execute(f"create or replace table {self.out_table} as select * from pred_df")

            eval_df = pd.DataFrame({self.target: y_te, "predicted": y_pred})
            self.dbcon.register("eval_df", eval_df)
            self.dbcon.execute(f"create or replace table {self.eval_table} as select * from eval_df")

            acc = float(accuracy_score(y_te, y_pred))
            prec_w, rec_w, f1_w, _ = precision_recall_fscore_support(y_te, y_pred, average="weighted", zero_division=0)
            prec_m, rec_m, f_m, _ = precision_recall_fscore_support(y_te, y_pred, average="macro", zero_division=0)
            report_dict = classification_report(y_te, y_pred, output_dict=True, zero_division=0)

            metrics_rows = [
                {"metric": "accuracy", "value": acc, "average": "none", "mode": "train"},
                {"metric": "precision", "value": float(prec_w), "average": "weighted", "mode": "train"},
                {"metric": "recall", "value": float(rec_w), "average": "weighted", "mode": "train"},
                {"metric": "f1", "value": float(f1_w), "average": "weighted", "mode": "train"},
                {"metric": "precision", "value": float(prec_m), "average": "macro", "mode": "train"},
                {"metric": "recall", "value": float(rec_m), "average": "macro", "mode": "train"},
                {"metric": "f1", "value": float(f_m), "average": "macro", "mode": "train"},
                {"metric": "classification_report_json", "value": json.dumps(report_dict), "average": "none", "mode": "train"}
            ]
            metrics_df = pd.DataFrame(metrics_rows)
            self.dbcon.register("metrics_df", metrics_df)
            self.dbcon.execute(f"create or replace table {self.metrics_table} as select * from metrics_df")

            try:
                print(self.dbcon.execute(f"from {self.metrics_table}").fetch_df())
            except Exception:
                pass

            if self.model_path:
                save_path = self._versioned_path(self.model_path)
                self._ensure_dir(save_path)
                joblib.dump(pipe, save_path)
                self._save_schema(save_path, X_tr)

            if self.train_returns == "pred":
                return self.out_table
            if self.train_returns == "eval":
                return self.eval_table
            return self.metrics_table

        return upstream.pipe(ops.take(1), ops.map(run_training), ops.publish(), ops.ref_count())



class PredictNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.features = list(config.get("features", []))
        self.target = config.get("target")
        self.model_path = config.get("model_path")
        self.eval_with_target = bool(config.get("eval_with_target", False))
        self.pred_table = make_table_name("pred")
        self.metrics_table = make_table_name("pred_metrics")
        self.sink_table = make_table_name("sink_pred")

    def _load_schema(self):
        sidecar = self.model_path + ".schema.json"
        if not os.path.exists(sidecar):
            return None
        with open(sidecar, "r", encoding="utf-8") as f:
            return json.load(f)

    def _coerce_df_to_schema(self, df, meta):
        required = meta.get("features", self.features)
        dtypes = meta.get("dtypes", {})
        missing = [c for c in required if c not in df.columns]
        if missing:
            for c in missing:
                df[c] = pd.Series([pd.NA] * len(df), dtype="object")
        df = df[required].copy()
        for c in required:
            want = dtypes.get(c, "string")
            try:
                if want.startswith("int") or want.startswith("Int"):
                    df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
                elif want.startswith("float"):
                    df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
                elif want in ("bool", "boolean"):
                    df[c] = df[c].astype("boolean")
                else:
                    df[c] = df[c].astype("string")
            except Exception:
                df[c] = df[c].astype("string")
        return df, required

    def get_query(self, input_table):
        return f"-- predict on {input_table} with schema enforcement"

    def create_observable(self, upstream):
        def run_predict(in_table):
            if not self.model_path or not os.path.exists(self.model_path):
                raise FileNotFoundError(f"model_path missing or not found: {self.model_path}")

            cols = list(set(self.features + ([self.target] if self.target and self.eval_with_target else [])))
            cols_csv = ", ".join([f'"{c}"' if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(c)) else c for c in cols])
            df = self.dbcon.execute(f"select {cols_csv} from {in_table}").fetch_df()

            meta = self._load_schema()
            if meta is not None:
                df, feat_order = self._coerce_df_to_schema(df, meta)
            else:
                feat_order = self.features

            pipe = joblib.load(self.model_path)
            y_pred = pipe.predict(df[feat_order])

            self.dbcon.register("pred_df", pd.DataFrame(y_pred, columns=["predicted"]))
            self.dbcon.execute(f"create or replace table {self.pred_table} as select * from pred_df")

            try:
                print(self.dbcon.execute(f"from {self.pred_table}").fetch_df())
            except Exception:
                pass

            if self.eval_with_target and self.target in df.columns:
                y_true = df[self.target].astype(str).values
                acc = float(accuracy_score(y_true, pd.Series(y_pred).astype(str).values))
                mdf = pd.DataFrame([{"metric": "accuracy", "value": acc, "average": "none", "mode": "infer"}])
                self.dbcon.register("mdf", mdf)
                self.dbcon.execute(f"create or replace table {self.metrics_table} as select * from mdf")
                try:
                    print(self.dbcon.execute(f"from {self.metrics_table}").fetch_df())
                except Exception:
                    pass

            self.dbcon.execute(f"create or replace view {self.sink_table} as select * from {self.pred_table}")
            return self.sink_table

        return upstream.pipe(ops.take(1), ops.map(run_predict), ops.publish(), ops.ref_count())





'''
class ConfusionMatrixNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.true_col = config.get("true_col", "Attrition")
        self.pred_col = config.get("pred_col", "predicted")
        self.metrics_table = make_table_name("confusion_metrics")
        self.matrix_table = make_table_name("confusion_matrix")

    def create_observable(self, upstream):
        def run_confusion(input_table):
            df = self.dbcon.execute(f"select * from {input_table}").fetch_df()
            cols = set([str(c) for c in df.columns])
            need = {self.true_col, self.pred_col}
            if not need.issubset(cols):
                if {"metric", "value", "average"}.issubset(cols):
                    raise ValueError(
                        f"ConfusionMatrix received a metrics table ({input_table}). "
                        f"Connect it to the eval table with columns '{self.true_col}' and '{self.pred_col}'."
                    )
                raise ValueError(f"Required columns not found in {input_table}: {self.true_col}, {self.pred_col}")

            y_true = pd.Series(df[self.true_col]).astype(str).values
            y_pred = pd.Series(df[self.pred_col]).astype(str).values

            labels = sorted(list({*pd.Series(y_true).dropna().unique(), *pd.Series(y_pred).dropna().unique()}))
            cm = confusion_matrix(y_true, y_pred, labels=labels)

            cm_rows = []
            for i, li in enumerate(labels):
                for j, lj in enumerate(labels):
                    cm_rows.append({"true_label": str(li), "pred_label": str(lj), "count": int(cm[i, j])})
            cm_df = pd.DataFrame(cm_rows)
            self.dbcon.execute(f"create or replace table {self.matrix_table} as select * from cm_df")

            acc = float(accuracy_score(y_true, y_pred))
            prec_w, rec_w, f1_w, _ = precision_recall_fscore_support(y_true, y_pred, average="weighted", zero_division=0)
            prec_m, rec_m, f1_m, _ = precision_recall_fscore_support(y_true, y_pred, average="macro", zero_division=0)
            metrics_rows = [
                {"metric": "accuracy", "value": acc, "average": "none"},
                {"metric": "precision", "value": float(prec_w), "average": "weighted"},
                {"metric": "recall", "value": float(rec_w), "average": "weighted"},
                {"metric": "f1", "value": float(f1_w), "average": "weighted"},
                {"metric": "precision", "value": float(prec_m), "average": "macro"},
                {"metric": "recall", "value": float(rec_m), "average": "macro"},
                {"metric": "f1", "value": float(f1_m), "average": "macro"}
            ]
            metrics_df = pd.DataFrame(metrics_rows)
            self.dbcon.execute(f"create or replace table {self.metrics_table} as select * from metrics_df")

            return self.metrics_table

        return upstream.pipe(ops.map(run_confusion))

    def get_query(self, input_table):
        return f"-- confusion matrix on {input_table}; outputs metrics {self.metrics_table} and matrix {self.matrix_table}"
'''
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

'''
Standardization	proper		Converts text to proper case (first letter capitalized).
Extraction	left		Extracts the specified number of characters from the start of a string.
Extraction	right		Extracts the specified number of characters from the end of a string.
Extraction	mid		Extracts a substring from a string starting at a given position.
Text Manipulation	substitute		Substitutes one text string for another in a given string.
Text Manipulation	replace		Replaces part of a string with another string.
Text Manipulation	value		Converts text to a number, if possible.
Text Manipulation	text		Formats a number into a specific text format.
'''


'''

Date & Time	date		Creates a date from year, month, and day components.
Date & Time	year		Extracts the year from a date.
Date & Time	month		Extracts the month from a date.
Date & Time	day		Extracts the day from a date.

'''

# in place, use alias

class LookupNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.mode = config.get("mode", "scalar")
        self.function = str(config.get("function","")).lower()
        self.args = config.get("args", {})
        self.input_col = config.get("input_col")
        self.output_col = config.get("output_col","lookup_value")
        self.join_on = config.get("join_on")
        self.order_by = config.get("order_by")
        self.out_table = make_table_name("lookup")

    def _sql_ident(self, s):
        return s if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(s)) else f'"{s}"'

    def _ensure_rowid(self, table):
        tmp = make_table_name("rowid")
        if self.order_by:
            ob = self.order_by
            self.dbcon.execute(
                f"create or replace table {tmp} as "
                f"select row_number() over (order by {ob}) as _rn, * from {table}"
            )
        else:
            self.dbcon.execute(
                f"create or replace table {tmp} as "
                f"select row_number() over () as _rn, * from {table}"
            )
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
            return self.out_table
        target_col = vis_cols[idx]
        self.dbcon.execute(
            f"create or replace table {self.out_table} as "
            f"select {self._sql_ident(target_col)} as {self._sql_ident(self.output_col)} "
            f"from {t} where _rn={return_row}"
        )
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
            return self.out_table
        colname = vis_cols[col_num-1]
        self.dbcon.execute(
            f"create or replace table {self.out_table} as "
            f"select {self._sql_ident(colname)} as {self._sql_ident(self.output_col)} from {t} where _rn={row_num}"
        )
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
        )
        if self.dbcon.execute(f"select count(*) from {self.out_table}").fetchone()[0] == 0:
            self.dbcon.execute(f"create or replace table {self.out_table} as select NULL as position")
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

'''
Lookup	hlookup		Searches for a value in the top row of a range and returns a value in the same column from a row you specify.
Lookup	index		Returns the value of a cell based on row and column numbers within a range.
Lookup	match		Returns the position of a specified value in a given range.
Lookup	indirect		Returns a cell reference specified by a text string.

'''

# create a mapping for user expression in json

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
    
'''
Arithmetic Operation	ceiling		Rounds a number up to the nearest multiple of a specified value.
Arithmetic Operation	floor		Rounds a number down to the nearest multiple of a specified value.
Arithmetic Operation	mod		Returns the remainder when one number is divided by another.
Arithmetic Operation	quotient		Returns the integer portion of a division, ignoring the remainder.
Arithmetic Operation	int		Rounds a number down to the nearest integer.
Arithmetic Operation	trunc		Truncates a number to an integer by removing the fractional part.
Arithmetic Operation	sign		Returns the sign of a number (-1 for negative, 1 for positive, 0 for zero).
Arithmetic Operation	sqrt		Returns the square root of a number.
Arithmetic Operation	power		Raises a number to the power of another number.
'''

def sql_ident(name):
    s = str(name)
    return s if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", s) else '"' + s.replace('"', '""') + '"'

def sql_literal(value):
    if value is None:
        return "NULL"
    s = str(value).replace("'", "''")
    return "'" + s + "'"

class ArithmeticNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.steps = []
        self.out_table = make_table_name("arith")
        self._parse_config(config)

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
        a = args_sql
        if op == "ceiling":
            return f"ceil({a[0]})" if len(a) == 1 else f"ceil({a[0]} / {a[1]}) * {a[1]}"
        if op == "floor":
            return f"floor({a[0]})" if len(a) == 1 else f"floor({a[0]} / {a[1]}) * {a[1]}"
        if op == "mod":
            return f"({a[0]} % {a[1]})"
        if op == "quotient":
            return f"cast(({a[0]}) / ({a[1]}) as bigint)"
        if op == "int":
            return f"floor({a[0]})"
        if op == "trunc":
            return f"trunc({a[0]})"
        if op == "sign":
            return f"sign({a[0]})"
        if op == "sqrt":
            return f"sqrt({a[0]})"
        if op in ("power", "pow"):
            return f"pow({a[0]}, {a[1]})"
        raise ValueError(f"Unsupported arithmetic op: {op}")

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
    
'''
Statistical Transformation	percentile		Returns the nth percentile of values in a range.
Statistical Transformation	percentrank		Returns the relative rank of a value in a data set as a percentage.
Statistical Transformation	quartile		Returns the quartile of a data set, based on percentile.
Statistical Transformation	covar		Returns the covariance, which measures the relationship between two datasets.
'''

# append single table output of statistic values to existing table

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


# complete documentation
# separate nodes and ops library


# cleaning node specific:
# check if cleaning node runs different clean ops using 1 node call itself or requires multiple calls. 
# If the latter, modify to enable 1 node call to do different things

# project specific:
# instead of immediate query execution, build query and output another json which includes exec path and sequence
# this can be executed by execution engine
# can include compile (exists) mode or exec mode - this o/p is another graph