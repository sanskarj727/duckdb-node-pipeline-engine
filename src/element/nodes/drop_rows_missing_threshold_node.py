import math
import re
from reactivex import operators as ops
from src.element.table_utils import make_table_name


class DropRowsMissingThresholdNode:

    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.threshold = float(config.get("threshold", 1.0))
        self.subset = config.get("subset") or config.get("columns") or []
        self.keep_others = bool(config.get("keepOthers", True))
        self.keep_columns = config.get("keepColumns") or []
        self.out_table = make_table_name("dropRowsThresh")

    def _sql_ident(self, name):
        return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)) else f'"{name}"'

    def _all_columns(self, table):
        df = self.dbcon.execute(f"DESCRIBE {table}").fetchdf()
        return df["column_name"].tolist()

    def _build(self, in_table):
        allcols = self._all_columns(in_table)

        subset = self.subset[:] if isinstance(self.subset, list) else []
        subset = [c for c in subset if c in allcols]
        if not subset:
            subset = allcols

        n = len(subset)
        if n == 0:
            cond = "TRUE"
        else:
            min_non_null = int(math.ceil(self.threshold * n))   # keep row if non_null_count >= ceil(threshold * n)
            non_null_terms = [
                f"CASE WHEN {self._sql_ident(c)} IS NOT NULL THEN 1 ELSE 0 END"
                for c in subset
            ]
            cond = f"(({ ' + '.join(non_null_terms) }) >= {min_non_null})"

        if self.keep_others:
            select_cols = allcols
        else:
            select_cols = []
            for c in (self.keep_columns + subset):
                if c in allcols and c not in select_cols:
                    select_cols.append(c)

        return cond, select_cols

    def create_observable(self, upstream):
        def do_drop(in_table):
            cond, select_cols = self._build(in_table)
            cols_sql = ", ".join(self._sql_ident(c) for c in select_cols) if select_cols else "*"
            sql = (
                f"create or replace table {self.out_table} as "
                f"select {cols_sql} from {in_table} where {cond}"
            )
            self.dbcon.execute(sql)
            return self.out_table

        return upstream.pipe(ops.map(do_drop))

    def get_query(self, input_table):
        cond, select_cols = self._build(input_table)
        cols_sql = ", ".join(self._sql_ident(c) for c in select_cols) if select_cols else "*"
        return (
            f"create or replace table {self.out_table} as "
            f"select {cols_sql} from {input_table} where {cond}"
        )
