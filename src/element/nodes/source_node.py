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

class SourceNode:
    def __init__(self, config, dbcon, data_source_path, table_name, file_type='csv', delimiter=','):
        self.dbcon = dbcon
        self.data_source_path = data_source_path      # path to input file
        self.table_name = table_name                  # duckdb table to create
        self.file_type = file_type                    # csv / dsv / parquet
        self.delimiter = delimiter                    # field separator for text files

    def create_observable(self):
        if self.file_type in ("csv", "dsv"):
            sql = (
                f"create or replace table {self.table_name} "
                f"as from read_csv('{self.data_source_path}', header=True, AUTO_DETECT=TRUE, delim='{self.delimiter}')"
            )  # load csv (here) into duckdb
            self.dbcon.execute(sql)
        elif str(self.file_type).lower() == "parquet":
            sql = (
                f"create or replace table {self.table_name} "
                f"as from read_parquet('{self.data_source_path}')"
            )  # load parquet
            self.dbcon.execute(sql)
        else:
            pass  # unsupported file type 
        return rx.just(self.table_name)  

    def get_query(self, input_table=None):
        if self.file_type in ("csv", "dsv"):
            return (
                f"create or replace table {self.table_name} "
                f"as from read_csv('{self.data_source_path}', header=True, AUTO_DETECT=TRUE, delim='{self.delimiter}')"
            ) # string output of syntax for readability
        elif str(self.file_type).lower() == "parquet":
            return (
                f"create or replace table {self.table_name} "
                f"as from read_parquet('{self.data_source_path}')"
            )
        return f"-- no-op for fileType={self.file_type}"  
