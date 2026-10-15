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