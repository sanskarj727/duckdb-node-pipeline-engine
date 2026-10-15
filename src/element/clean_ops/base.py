import re
from uuid import uuid4

import pandas as pd
from reactivex import operators as ops

class Operation:
    def apply(self, node):
        raise NotImplementedError