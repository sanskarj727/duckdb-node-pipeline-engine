from .base import Operation


class DropNullsThresholdOps(Operation):

    def apply(self, node):
        cfg = getattr(node, "dropnulls_threshold", None)
        if not cfg or not isinstance(cfg, dict):
            return

        columns = cfg.get("columns") or []
        if not columns:
            return

        mode = str(cfg.get("mode", "max_nulls")).lower().strip()

        df = node.dbcon.execute(f"describe {node.outtable}").fetchdf()
        all_cols = df["column_name"].tolist()
        cols = [c for c in columns if c in all_cols]
        if not cols:
            return

        # Per-row NULL count across selected columns
        null_terms = [
            f"case when {node.sqlident(c)} is null then 1 else 0 end"
            for c in cols
        ]
        null_count_expr = "(" + " + ".join(null_terms) + ")"
        total_cols = len(cols)
        if total_cols <= 0:
            return

        if mode == "max_null_fraction":
            thr = cfg.get("maxNullFraction", None)
            if thr is None:
                return
            try:
                thr = float(thr)
            except Exception:
                return
            node.dbcon.execute(
                f"delete from {node.outtable} "
                f"where ({null_count_expr})::double / {total_cols} > {thr}"
            )
            return

        if mode == "min_non_nulls":
            thr = cfg.get("minNonNulls", None)
            if thr is None:
                return
            try:
                thr = int(thr)
            except Exception:
                return
            node.dbcon.execute(
                f"delete from {node.outtable} "
                f"where ({total_cols} - ({null_count_expr})) < {thr}"
            )
            return

        # default: max_nulls
        thr = cfg.get("maxNulls", None)
        if thr is None:
            return
        try:
            thr = int(thr)
        except Exception:
            return
        node.dbcon.execute(
            f"delete from {node.outtable} where ({null_count_expr}) > {thr}"
        )
