from .base import Operation


class DropColsMissingThresholdOps(Operation):

    def apply(self, node):
        cfg = getattr(node, "dropcols_missing_threshold", None)
        if not cfg or not isinstance(cfg, dict):
            return

        df = node.dbcon.execute(f"describe {node.outtable}").fetchdf()
        all_cols = df["column_name"].tolist()
        if not all_cols:
            return

        columns = cfg.get("columns") or all_cols
        cols = [c for c in columns if c in all_cols]
        if not cols:
            return

        mode = str(cfg.get("mode", "max_null_fraction")).lower().strip()

        total_rows = node.dbcon.execute(
            f"select count(*) as n from {node.outtable}"
        ).fetchone()[0]
        if total_rows is None:
            return
        total_rows = int(total_rows)

        keep = []
        for c in cols:
            nn = node.dbcon.execute(
                f"select count(*) from {node.outtable} where {node.sqlident(c)} is not null"
            ).fetchone()[0]
            nn = int(nn or 0)
            nulls = total_rows - nn

            if mode == "min_non_nulls":
                thr = cfg.get("minNonNulls", None)
                if thr is None:
                    keep.append(c)
                    continue
                try:
                    thr = int(thr)
                except Exception:
                    keep.append(c)
                    continue
                if nn >= thr:
                    keep.append(c)
                continue

            if mode == "max_nulls":
                thr = cfg.get("maxNulls", None)
                if thr is None:
                    keep.append(c)
                    continue
                try:
                    thr = int(thr)
                except Exception:
                    keep.append(c)
                    continue
                if nulls <= thr:
                    keep.append(c)
                continue

            if mode == "min_non_null_fraction":
                thr = cfg.get("minNonNullFraction", None)
                if thr is None:
                    keep.append(c)
                    continue
                try:
                    thr = float(thr)
                except Exception:
                    keep.append(c)
                    continue
                frac = (nn / total_rows) if total_rows > 0 else 0.0
                if frac >= thr:
                    keep.append(c)
                continue

            # default: max_null_fraction
            thr = cfg.get("maxNullFraction", None)
            if thr is None:
                keep.append(c)
                continue
            try:
                thr = float(thr)
            except Exception:
                keep.append(c)
                continue
            frac = (nulls / total_rows) if total_rows > 0 else 0.0
            if frac <= thr:
                keep.append(c)

        if not keep: # avoid dropping everything
            keep = cols[:1]

        mapping = {c: node.sqlident(c) for c in keep}
        node.applysqlprojection(node.outtable, mapping)
