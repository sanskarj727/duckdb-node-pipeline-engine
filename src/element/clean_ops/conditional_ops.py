from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class ConditionalOps(Operation):
    def apply(self, node):
        ops_cfg = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops_cfg:
            return

        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        mapping = {}

        for cfg in ops_cfg:
            op = str(cfg.get("op", "")).lower().strip()

            if op in ("if", "ifs"):
                cols = [c for c in cfg.get("columns", []) if c in all_cols]
                expr = str(cfg.get("expr", "")).strip()
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
                expr = str(cfg.get("expr", "")).strip()
                if not (target and expr):
                    continue
                bool_expr = f"(NOT ({expr}))"
                mapping[target] = bool_expr
                continue

        if mapping:
            node._apply_sql_projection(node.out_table, mapping)

    def _split_top_level_commas(self, s: str):
        out, buf, depth = [], [], 0
        i = 0
        while i < len(s):
            ch = s[i]
            if ch in ("'", '"'):
                q = ch
                buf.append(ch)
                i += 1
                while i < len(s):
                    c2 = s[i]
                    buf.append(c2)
                    if c2 == q and s[i - 1] != "\\":
                        break
                    i += 1
                i += 1
                continue
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth -= 1
            if ch == "," and depth == 0:
                out.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
            i += 1
        if buf:
            out.append("".join(buf).strip())
        return out

    def _to_case(self, expr):
        m = re.match(r"^\s*if\s*\((.*)\)\s*$", expr, re.IGNORECASE)
        if m:
            parts = self._split_top_level_commas(m.group(1))
            if len(parts) == 3:
                return f"(case when {parts[0]} then {parts[1]} else {parts[2]} end)"
            return None

        m = re.match(r"^\s*ifs\s*\((.*)\)\s*$", expr, re.IGNORECASE)
        if m:
            parts = self._split_top_level_commas(m.group(1))
            if len(parts) >= 3 and len(parts) % 2 == 1:
                default = parts[-1]
                pairs = parts[:-1]
                whens = []
                for i in range(0, len(pairs), 2):
                    whens.append(f"when {pairs[i]} then {pairs[i + 1]}")
                return f"(case {' '.join(whens)} else {default} end)"
        return None