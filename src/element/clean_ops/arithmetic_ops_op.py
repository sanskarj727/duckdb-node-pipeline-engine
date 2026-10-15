from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops


class ArithmeticOpsOp(Operation):
    def apply(self, node):
        ops_cfg = getattr(node, "arithmetic_ops", [])
        if not ops_cfg:
            return

        cols_df = node.dbcon.execute(f"describe {node.out_table}").fetch_df()
        all_cols = cols_df["column_name"].tolist()
        mapping = {}

        op_factory = self._build_op_factory()

        def a_val(arg):
            if isinstance(arg, (int, float)):
                return str(arg)
            if arg is None:
                return "NULL"
            if isinstance(arg, str) and arg in all_cols:
                return node._sql_ident(arg)
            if isinstance(arg, str):
                s = str(arg).replace("'", "''")
                return "'" + s + "'"
            return str(arg)

        for cfg in ops_cfg:
            op = str(cfg.get("op", "")).lower().strip()
            cols = [c for c in cfg.get("columns", []) if c in all_cols]
            args = cfg.get("args") or []
            output = cfg.get("output")
            in_place = bool(cfg.get("in_place", False))

            if not cols or not op:
                continue

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

                if op in ("quotient", "mod", "power", "pow", "log") and len(arg_sql) < 2:
                    continue

                fn = op_factory.get(op)
                if fn is None:
                    continue

                expr = fn(arg_sql)
                if not expr:
                    continue

                target = c if in_place else (output or f"{c}_{op}")
                mapping[target] = expr

        if mapping:
            node._apply_sql_projection(node.out_table, mapping)

    def _build_op_factory(self):
        def _ceiling(a):
            return (
                f"ceil({a[0]})"
                if len(a) == 1
                else f"ceil({a[0]} / {a[1]}) * {a[1]}"
            )

        def _floor(a):
            return (
                f"floor({a[0]})"
                if len(a) == 1
                else f"floor({a[0]} / {a[1]}) * {a[1]}"
            )

        def _quotient(a):
            return f"cast(({a[0]}) / ({a[1]}) as bigint)"

        def _mod(a):
            return f"({a[0]} % {a[1]})"

        def _power(a):
            return f"pow({a[0]}, {a[1]})"

        def _round(a):
            scale = a[1] if len(a) > 1 else "0"
            return f"round({a[0]}, {scale})"

        def _roundup(a):
            return f"ceiling({a[0]})"

        def _rounddown(a):
            return f"floor({a[0]})"

        def _int(a):
            return f"floor({a[0]})"

        def _trunc(a):
            return f"trunc({a[0]})"

        def _sign(a):
            return f"sign({a[0]})"

        def _sqrt(a):
            return f"sqrt({a[0]})"

        def _exp(a):
            return f"exp({a[0]})"

        def _ln(a):
            return f"ln({a[0]})"

        def _log(a):
            base = a[1] if len(a) > 1 else "10"
            return f"log({base}, {a[0]})"

        def _fact(a):
            return f"factorial({a[0]})"

        def _combin(a):
            return f"combin({a[0]}, {a[1]})"

        def _permut(a):
            return f"permut({a[0]}, {a[1]})"

        def _rand(a):
            return "random()"

        def _randbetween(a):
            return f"greatest({a[0]}, least({a[1]}, floor(random() * ({a[1]} - {a[0]} + 1)) + {a[0]}))"

        def _pi(a):
            return "pi()"

        def _degrees(a):
            return f"degrees({a[0]})"

        def _radians(a):
            return f"radians({a[0]})"

        def _squareroot(a):
            return f"sqrt({a[0]})"

        def _cube(a):
            return f"pow({a[0]}, 3)"

        return {
            "ceiling": _ceiling,
            "floor": _floor,
            "mod": _mod,
            "quotient": _quotient,
            "int": _int,
            "trunc": _trunc,
            "sign": _sign,
            "sqrt": _sqrt,
            "power": _power,
            "pow": _power,
            "round": _round,
            "roundup": _roundup,
            "rounddown": _rounddown,
            "exp": _exp,
            "ln": _ln,
            "log": _log,
            "fact": _fact,
            "combin": _combin,
            "permut": _permut,
            "rand": _rand,
            "randbetween": _randbetween,
            "pi": _pi,
            "degrees": _degrees,
            "radians": _radians,
            "squareroot": _squareroot,
            "cube": _cube,
        }
