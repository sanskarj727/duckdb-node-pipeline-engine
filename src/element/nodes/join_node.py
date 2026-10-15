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

class JoinNode:
    def __init__(self, config, dbcon):
        self.left_on = config.get('left_on', [])             # join keys on left table
        self.right_on = config.get('right_on', [])           # join keys on right table
        self.join_type = config.get('join_type', 'inner')    # inner/left/right/full
        self.suffixes = tuple(config.get('suffixes', ['_left', '_right']))
        self.dbcon = dbcon
        self.out_table = make_table_name('join')             # output join table

    def create_observable(self, left_obs, right_obs):
        def join_batches(pair):
            l, r = pair  # table names from upstream
            on_clause = " AND ".join([f"l.{lk} = r.{rk}" for lk, rk in zip(self.left_on, self.right_on)])
            sql = f"create or replace table {self.out_table} as select * from {l} as l {self.join_type} join {r} as r on {on_clause}"
            self.dbcon.execute(sql)
            return self.out_table
        return rx.zip(left_obs, right_obs).pipe(ops.map(join_batches))  # join per pair of batches

    def get_query(self, left_table, right_table):
        on_clause = " AND ".join([f"l.{lk} = r.{rk}" for lk, rk in zip(self.left_on, self.right_on)])
        return f"create or replace table {self.out_table} as select * from {left_table} as l {self.join_type} join {right_table} as r on {on_clause}"
