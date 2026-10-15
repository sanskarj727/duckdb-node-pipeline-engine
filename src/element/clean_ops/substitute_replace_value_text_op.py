from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class SubstituteReplaceValueTextOp(Operation):
    def apply(self, node):
        ops_cfg = getattr(node, "operations", []) or getattr(node, "text_ops", [])
        if not ops_cfg:
            return

        df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = df["column_name"].tolist()
        mapping = {}

        for cfg in ops_cfg:
            op = str(cfg.get("op", "")).lower().strip()
            cols = [c for c in cfg.get("columns", []) if c in all_cols]
            if not cols and op != "proper":
                continue

            if op == "substitute":
                po = (cfg.get("old") or "").replace("'", "''")
                pn = (cfg.get("new") or "").replace("'", "''")
                for c in cols:
                    mapping[c] = (
                        f"replace({node._sql_ident(c)}, '{po}', '{pn}')"
                    )

            if op == "replace":
                start = int(cfg.get("start", 1))
                length = int(cfg.get("length", 0))
                nt = (cfg.get("new_text", "")).replace("'", "''")
                for c in cols:
                    cid = node._sql_ident(c)
                    leftp = (
                        f"case when {start} > 1 then "
                        f"substr({cid}, 1, {start}-1) else '' end"
                    )
                    rstart = (
                        f"case when {start}+{length} <= length({cid}) "
                        f"then {start}+{length} else length({cid})+1 end"
                    )
                    rightp = (
                        f"case when {rstart} <= length({cid}) "
                        f"then substr({cid}, {rstart}) else '' end"
                    )
                    mapping[c] = f"({leftp} || '{nt}' || {rightp})"

            if op == "value":
                tt = str(cfg.get("target_type", "DOUBLE")).upper()
                for c in cols:
                    mapping[c] = f"try_cast({node._sql_ident(c)} as {tt})"

            if op == "text":
                fmt = cfg.get("fmt")
                for c in cols:
                    cid = node._sql_ident(c)
                    if fmt:
                        fmt_esc = fmt.replace("'", "''")
                        m = re.match(r'%\.([0-9]+)f$', fmt.strip())
                        if m:
                            d = int(m.group(1))
                            mapping[c] = (
                                f"printf('%.{d}f', cast({cid} as DOUBLE))"
                            )
                        else:
                            mapping[c] = (
                                f"printf('{fmt_esc}', cast({cid} as DOUBLE))"
                            )
                    else:
                        mapping[c] = f"cast({cid} as VARCHAR)"

        if mapping:
            node._apply_sql_projection(node.out_table, mapping)