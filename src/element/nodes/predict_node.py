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