import re
from reactivex import operators as ops
from src.element.table_utils import make_table_name


class DropColsMissingThresholdNode:

    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.threshold = float(config.get("threshold", 1.0))
        self.subset = config.get("subset") or config.get("columns") or []
        self.keep_others = bool(config.get("keepOthers", True))
        self.keep_columns = config.get("keepColumns") or []
        self.out_table = make_table_name("dropColsThresh")

    def _sql_ident(self, name):
        return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)) else f'"{name}"'

    def _all_columns(self, table):
        df = self.dbcon.execute(f"DESCRIBE {table}").fetchdf()
        return df["column_name"].tolist()

    def _survivors(self, in_table):
        allcols = self._all_columns(in_table)

        candidate = self.subset[:] if isinstance(self.subset, list) else []
        candidate = [c for c in candidate if c in allcols]
        if not candidate:
            candidate = allcols

        survivors = []
        for c in candidate:
            q = (
                "select case when count(*) = 0 then 0.0 "
                f"else sum(case when {self._sql_ident(c)} is null then 1 else 0 end)::double / count(*) end "
                f"as miss_frac from {in_table}"
            )
            miss_frac = float(self.dbcon.execute(q).fetchone()[0])
            if miss_frac <= self.threshold:
                survivors.append(c)

        if self.keep_others:    # drop only failing columns among candidate
            drop_set = set(candidate) - set(survivors)
            kept = [c for c in allcols if c not in drop_set]
        else:
            kept = survivors[:]

        for c in self.keep_columns:
            if c in allcols and c not in kept:
                kept.insert(0, c)

        return kept

    def create_observable(self, upstream):
        def do_drop(in_table):
            kept = self._survivors(in_table)
            cols_sql = ", ".join(self._sql_ident(c) for c in kept) if kept else "*"
            sql = f"create or replace table {self.out_table} as select {cols_sql} from {in_table}"
            self.dbcon.execute(sql)
            return self.out_table

        return upstream.pipe(ops.map(do_drop))

    def get_query(self, input_table):
        kept = self._survivors(input_table)
        cols_sql = ", ".join(self._sql_ident(c) for c in kept) if kept else "*"
        return f"create or replace table {self.out_table} as select {cols_sql} from {input_table}"
