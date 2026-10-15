- Overview

- Engine core: in-memory DuckDB, Python.
- Execution model: RxPy observables; each node emits a DuckDB table name.
- Configuration: JSON "flow" file with dataSources and a node graph (dataFlows.flow.flowModel).
- Nodes: ingestion, selection, filtering, joins/merges, cleaning, aggregation, ML training/prediction, text transforms, lookups, transpose, arithmetic, validation, and missing-threshold nodes.

- Installation

1. Create and activate a virtual environment.
2. Install dependencies (example):

Cmd:

pip install duckdb 
pip install reactivex 
pip install numpy 
pip install pandas 
pip install scikit-learn 
pip install joblib


- Project Structure

- src/project/main.py – CLI entrypoint.
- src/project/project.py – builds the graph, runs the pipeline.
- src/element/node_factory.py – maps actionType → node class.
- src/element/nodes/ – node implementations (one class per action type).
- src/element/clean_ops/ – lower-level cleaning operations used by CleaningNode (some are legacy/unused).
- src/element/table_utils.py – helper for generating unique DuckDB table names.

The engine uses a single in-memory DuckDB connection per run.

- Running a Pipeline

By default, main.py loads configs/threshold_ops.json:

Cmd:

python -m src.project.main


- To run a different pipeline:

- Change the path in main.py, or
- Wrap Project in your own small entrypoint that loads your config file and calls Project.run().

- Project.run():

1. Connects to DuckDB (:memory:).
2. Instantiates a NodeFactory filtered to the actionTypes in the flow.
3. Loads dataSources into DuckDB tables via SourceNode.
4. Builds the node graph from flowModel.actions and flowModel.connections.
5. Pushes RxPy observables through the graph, materializing intermediate results as DuckDB tables.
6. Prints previews and final terminal node outputs.

- Pipeline JSON Structure

Top-level keys used by Project:

- dataSources: list of inputs.
  - Each has id, name, and providerInfo.connectionInfo:
    - filePath
    - fileType (e.g. "csv", "parquet")
    - delimiter (for delimited files)
- dataFlows.flow:
  - schemaDictionary: passed into NodeFactory (not heavily used at runtime yet).
  - flowModel:
    - actions: list of nodes.
      - id
      - actionType (e.g. "clean", "summarize", "join", …)
      - config (node-specific; see separate config-spec doc)
      - inputs (ports with id)
      - outputs (ports with id)
    - connections: edges wiring outputs → inputs:
      - source.action, source.output/port
      - target.action, target.input/port

NodeFactory supports these actionType values:

- "source"
- "select-fields"
- "filter"
- "derive-fields"
- "summarize"
- "sort"
- "merge"
- "join"
- "clean"
- "fuzzy-detect"
- "train"
- "predict"
- "text-transform"
- "lookup"
- "transpose"
- "arithmetic"
- "validation"
- "drop-rows-missing-threshold"
- "drop-cols-missing-threshold"

- Execution Model

- Each node exposes:

- create_observable(...) – returns an observable that, when subscribed, performs the node's work and emits a DuckDB table name.
- get_query(...) – returns an approximate SQL string describing what the node will run (used for logging/preview).

The project orchestrator:

- Passes table-name observables along connections.
- Uses publish() + ref_count() to share upstream work among multiple consumers.
- Uses combine_latest and zip to coordinate binary nodes (joins/merges).
- Times each node execution and prints basic previews with SELECT ... LIMIT.

- Important Nodes (Brief)

I have added Separate document (Markdown file) with full node config details; this section is just to readability.

- SourceNode (source)

Reads CSV/DSV/Parquet into DuckDB using read_csv / read_parquet, controlled by fileType, delimiter, and filePath from the data source.

- CleaningNode (clean)

Orchestrates multiple cleaning operations (drop/fill nulls, replacements, IQR outliers, constraints, basic text ops, arithmetic ops) by running a fixed list of cleaning-operation objects (PIPELINE_OPS) over self.out_table. Some legacy clean-op implementations (threshold/fuzzy) exist but are not wired into the node and are effectively unused.

- DropRowsMissingThresholdNode (drop-rows-missing-threshold)

Drops rows that have too many missing values across a subset of columns, based on a fraction threshold.

- DropColsMissingThresholdNode (drop-cols-missing-threshold)

Drops columns that exceed a missing-value fraction threshold, with options to keep "other" columns and force-keep some columns.

- FuzzyDetectionNode (fuzzy-detect)

Performs a fuzzy self-join using Levenshtein distance to add a similarity score (and optional label) column, then optionally filters by a threshold.

- TextTransformNode (text-transform)

Applies one text operation (trim/upper/lower/substitute/replace/value/text/proper) to specified columns, mostly in SQL; "proper" uses a Python path with Pandas.

- DeriveFieldsNode (derive-fields)

Converts UI-style expressions (e.g. Excel-like IF, IFS, LEFT/RIGHT/MID, date functions, simple arithmetic, AND/OR/NOT) into DuckDB SQL expressions and adds them as new columns.

- SummarizeNode (summarize)

Performs grouped or global aggregation with a rich set of aggregate functions, including count, sum, avg, median, mode, quantiles, covariances, and conditional aggregates using FILTER.

- JoinNode (join) / MergeNode (merge)

Join two tables (inner/left/right/full) or perform set operations (UNION/INTERSECT/EXCEPT).

- SelectFieldsNode (select-fields) / SortNode (sort)

Column selection and ordering, with some numeric-sorting logic for specified columns.

- TrainNode (train) / PredictNode (predict)

Train a scikit-learn classification pipeline on DuckDB data and persist a model, then load it later to score new data (with schema enforcement).

- LookupNode (lookup)

Implements spreadsheet-like lookups (HLOOKUP, INDEX, MATCH, INDIRECT), with both scalar and "map" modes.

- TransposeNode (transpose)

Uses DuckDB UNPIVOT + row_number() to pivot a table so that original columns become rows and original rows become columns.

- ValidationNode (validation)

Appends boolean validation columns (isblank, isnumber, etc.) based on rules.

- Limitations and Notes

- Engine uses an in-memory DuckDB database per run; no automatic persistence.
- Some clean-ops under src/element/clean_ops are legacy and not currently invoked by CleaningNode (notably the threshold and legacy fuzzy ones). For missing-threshold and fuzzy features, the node implementations are the source of truth.
- Error handling is mostly fail-fast inside nodes (raising ValueError for unsupported operations or misconfigurations).

- Key Dependencies

- DuckDB: SQL engine and data processing core.
- RxPy: Reactive programming for observable-based node orchestration.
- Scikit-learn: Machine learning (train/predict nodes).
- Pandas: Data manipulation, DataFrame I/O with DuckDB.
- NumPy: Numerical utilities.

- Internship Project

This codebase was developed as part of an internship project to create a flexible, node-based data pipeline engine supporting data cleaning, transformation, aggregation, and basic ML workflows on top of DuckDB.