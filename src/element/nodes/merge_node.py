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

class MergeNode:
    def __init__(self, config, dbcon):
        self.operation = config.get('operation', 'union')       # union / intersection / difference
        self.keep_duplicates = config.get('keep_duplicates', True)  # union vs union all
        self.dbcon = dbcon
        self.out_table = make_table_name('merge')              # merged result table

    def create_observable(self, left_obs, right_obs):
        def merge_batches(pair):
            l, r = pair  # upstream table names
            if self.operation == "union":
                sql = f"create or replace table {self.out_table} as select * from {l} union{' all' if self.keep_duplicates else ''} select * from {r}"
            elif self.operation == "intersection":
                sql = f"create or replace table {self.out_table} as select * from {l} intersect select * from {r}"
            elif self.operation == "difference":
                sql = f"create or replace table {self.out_table} as select * from {l} except select * from {r}"
            else:
                raise NotImplementedError(f"Unsupported merge operation: {self.operation}")
            self.dbcon.execute(sql)
            return self.out_table
        return rx.zip(left_obs, right_obs).pipe(ops.map(merge_batches))  # merge in batches for large o/p

    def get_query(self, left_table, right_table):
        if self.operation == "union":
            return f"create or replace table {self.out_table} as select * from {left_table} union{' all' if self.keep_duplicates else ''} select * from {right_table}"
        elif self.operation == "intersection":
            return f"create or replace table {self.out_table} as select * from {left_table} intersect select * from {right_table}"
        elif self.operation == "difference":
            return f"create or replace table {self.out_table} as select * from {left_table} except select * from {right_table}"
        else:
            raise NotImplementedError(f"Unsupported merge operation: {self.operation}")
