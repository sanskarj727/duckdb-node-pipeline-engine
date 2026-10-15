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

class FilterNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.expr = config.get('userExpression')  # raw filter expression from UI
        self.out_table = make_table_name("filter")  # table holding filtered rows

    def create_observable(self, upstream):
        def do_filter(in_table):
            expr = self.expr
            # "col = value" pattern to auto-quote strings for output syntax
            m = re.match(r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([^\']+)\s*$', expr)
            if m:
                field, value = m.groups()
                try:
                    colinfo = self.dbcon.execute(f"PRAGMA table_info({in_table})").fetchdf()
                    coltype = colinfo.set_index('name').loc[field, 'type']
                    if 'CHAR' in coltype.upper() and not (value.strip().startswith("'") or value.strip().startswith('"')):
                        expr = f"{field} = '{value.strip()}'"  
                except Exception:
                    pass  
            sql = f"create or replace table {self.out_table} as from {in_table} where {expr}"
            self.dbcon.execute(sql)
            return self.out_table
        return upstream.pipe(ops.map(do_filter))

    def get_query(self, input_table):
        expr = self.expr
        m = re.match(r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([^\']+)\s*$', expr)
        if m:
            field, value = m.groups()
            if not (value.strip().startswith("'") or value.strip().startswith('"')):
                expr = f"{field} = '{value.strip()}'"
        return f"create or replace table {self.out_table} as from {input_table} where {expr}"
