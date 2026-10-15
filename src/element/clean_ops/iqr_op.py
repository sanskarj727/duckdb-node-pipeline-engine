from .base import Operation

import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class IQROp(Operation):
    def apply(self, node):
        cfg = node.iqr_outliers
        if not (cfg and isinstance(cfg, dict)):
            return

        cols = cfg.get("columns", [])
        method = cfg.get("method", "remove")
        whisker = cfg.get("whisker", 1.5)
        if not cols:
            return

        colinfo_df = node.dbcon.execute(
            f"PRAGMA table_info({node.out_table})"
        ).fetchdf()
        schema_types = {
            row["name"]: str(row["type"]).upper()
            for _, row in colinfo_df.iterrows()
        }

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
where try_cast({c} as double) <
      (select min(lower_{c}) from {node.out_table})
   or try_cast({c} as double) >
      (select max(upper_{c}) from {node.out_table})
"""
                )
            elif method == "winsorize":
                tmp_wins = f"{node.out_table}_wins_{uuid4().hex[:6]}"
                node.dbcon.execute(
                    f"""
create or replace table {tmp_wins} as
select
{", ".join([
    f"case when try_cast({c} as double) < lower_{c} then lower_{c} "
    f"when try_cast({c} as double) > upper_{c} then upper_{c} "
    f"else {c} end as {c}" if name == c else name
    for name in schema_types.keys()
])}
from {node.out_table}
"""
                )
                node.dbcon.execute(f"drop table {node.out_table}")
                node.dbcon.execute(
                    f"alter table {tmp_wins} rename to {node.out_table}"
                )
                cols_only = ", ".join(schema_types.keys())
                node.dbcon.execute(
                    f"create or replace table {node.out_table} "
                    f"as select {cols_only} from {node.out_table}"
                )