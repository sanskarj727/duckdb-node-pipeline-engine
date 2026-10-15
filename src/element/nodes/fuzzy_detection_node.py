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

class FuzzyDetectionNode:
    def __init__(self, config, dbcon):
        self.dbcon = dbcon
        self.column = config.get('column')              
        self.new_col = config.get('output_col', 'similarity_score')
        self.threshold = float(config.get('threshold', 0.8))  
        self.label_col = config.get('label_col', None)  
        self.out_table = make_table_name("fuzzy")

    def create_observable(self, upstream):
        def do_fuzzy(in_table):
            label_expr = f"b.{self.column}" if self.label_col else "NULL"
            similarity_expr = (
                f"(1-cast(levenshtein(a.{self.column}, b.{self.column}) as double) "
                f"/ greatest(length(a.{self.column}), length(b.{self.column})))"
            )
            select_clause = (
                f"a.*, m.sim_score as {self.new_col}"
                + (f", m.sim_label as {self.label_col}" if self.label_col else "")
            )
            where_clause = f"a.{self.column} != b.{self.column}"
            sql = f"""
            create or replace table {self.out_table} as
            select {select_clause}
            from {in_table} a
            left join (
                select a_rowid, sim_score, sim_label from (
                    select
                        a.rowid as a_rowid,
                        {similarity_expr} as sim_score,
                        {label_expr} as sim_label,
                        row_number() over (partition by a.rowid order by (1-cast(levenshtein(a.{self.column}, b.{self.column}) as double)/greatest(length(a.{self.column}), length(b.{self.column}))) desc) as rn
                    from {in_table} a
                    join {in_table} b
                    on {where_clause}
                )
                where rn = 1
            ) m
            on a.rowid = m.a_rowid
            """
            self.dbcon.execute(sql)
            if self.threshold is not None:
                self.dbcon.execute(
                    f"create or replace table {self.out_table} as select * from {self.out_table} where {self.new_col} >= {self.threshold}"
                )
            return self.out_table
        return upstream.pipe(ops.map(do_fuzzy))

    def get_query(self, input_table):
        sim = self.new_col
        lbl = self.label_col if self.label_col else "most_similar"
        return f"create or replace table {self.out_table} -- computes {sim} and {lbl} for each {self.column} by fuzzy self-join"