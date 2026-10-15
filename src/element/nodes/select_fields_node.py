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

class SelectFieldsNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.fields = config.get('fieldSelection', [])  # columns chosen
        self.out_table = make_table_name("select")      # o/p table

    def create_observable(self, upstream):
        def do_select(in_table):
            sql = f"create or replace table {self.out_table} as select {', '.join(self.fields)} from {in_table}"
            self.dbcon.execute(sql)  # select into a new table
            return self.out_table
        return upstream.pipe(ops.map(do_select))

    def get_query(self, input_table):
        return f"create or replace table {self.out_table} as select {', '.join(self.fields)} from {input_table}"
