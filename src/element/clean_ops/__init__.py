from .base import Operation

from .replacements_op import ReplacementsOp
from .fill_nulls_op import FillNullsOp
from .drop_nulls_op import DropNullsOp
from .drop_duplicates_op import DropDuplicatesOp
from .constraints_op import ConstraintsOp
from .iqr_op import IQROp
from .fuzzy_detect_op import FuzzyDetectOp
from .trim_op import TrimUpperLowerOp
from .substitute_replace_value_text_op import SubstituteReplaceValueTextOp
from .extract_ops import ExtractOps
from .conditional_ops import ConditionalOps
from .proper_case_op import ProperCaseOp
from .arithmetic_ops_op import ArithmeticOpsOp
from .validation_ops_op import ValidationOpsOp
from .drop_rows_thresh_op import DropNullsThresholdOps
from .drop_cols_thresh_op import DropColsMissingThresholdOps


PIPELINE_OPS = [
    ReplacementsOp(),
    FillNullsOp(),
    DropNullsOp(),
    DropNullsThresholdOps(),
    DropDuplicatesOp(),
    ConstraintsOp(),
    IQROp(),
    FuzzyDetectOp(),
    TrimUpperLowerOp(),
    SubstituteReplaceValueTextOp(),
    ArithmeticOpsOp(),
    ExtractOps(),
    ConditionalOps(),
    ProperCaseOp(),
    ValidationOpsOp(),
    DropColsMissingThresholdOps(),
]


__all__ = [
    "Operation",
    "ReplacementsOp",
    "FillNullsOp",
    "DropNullsOp",
    "DropNullsThresholdOps",
    "DropDuplicatesOp",
    "ConstraintsOp",
    "IQROp",
    "FuzzyDetectOp",
    "TrimUpperLowerOp",
    "SubstituteReplaceValueTextOp",
    "ArithmeticOpsOp",
    "ExtractOps",
    "ConditionalOps",
    "ProperCaseOp",
    "ValidationOpsOp",
    "DropColsMissingThresholdOps",
    "PIPELINE_OPS",
]
