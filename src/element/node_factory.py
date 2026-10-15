from src.element.nodes import (
    SourceNode,
    SelectFieldsNode,
    FilterNode,
    DeriveFieldsNode,
    SummarizeNode,
    SortNode,
    MergeNode,
    JoinNode,
    CleaningNode,
    FuzzyDetectionNode,
    TrainNode,
    PredictNode,
    TextTransformNode,
    LookupNode,
    TransposeNode,
    ArithmeticNode,
    ValidationNode,
    DropRowsMissingThresholdNode,
    DropColsMissingThresholdNode,
)


class NodeFactory:
    def __init__(self, schema_dict, enabled_types=None):
        self.schema_dict = schema_dict
        self.registry = {
            "source": SourceNode,
            "select-fields": SelectFieldsNode,
            "filter": FilterNode,
            "derive-fields": DeriveFieldsNode,
            "summarize": SummarizeNode,
            "sort": SortNode,
            "merge": MergeNode,
            "join": JoinNode,
            "clean": CleaningNode,
            "fuzzy-detect": FuzzyDetectionNode,
            "train": TrainNode,
            "predict": PredictNode,
            "text-transform": TextTransformNode,
            "lookup": LookupNode,
            "transpose": TransposeNode,
            "arithmetic": ArithmeticNode,
            "validation": ValidationNode,
            "drop-rows-missing-threshold": DropRowsMissingThresholdNode,
            "drop-cols-missing-threshold": DropColsMissingThresholdNode,
        }
        if enabled_types is not None:
            self.registry = {k: v for k, v in self.registry.items() if k in enabled_types}

    def create(self, action_type, config, **kwargs):
        if action_type not in self.registry:
            raise ValueError(f"Unknown node type: {action_type}")
        return self.registry[action_type](config, **kwargs)
