# attempt at implementing 2 different modes. refer to main.py for actual execution

import os
import json
from src.project.newproject import Project

with open("configs/Scribble.json", "r") as f:
    pipeline_config = json.load(f)

proj_exec = Project(pipeline_config, mode="execute") # execute mode - existing
proj_exec.run()

proj_compile = Project(pipeline_config, mode="compile") # compile mode - new
plan_graph = proj_compile.run()

input_path = "configs/Scribble.json"
base, ext = os.path.splitext(input_path)
output_path = f"{base}_output{ext}"

with open(output_path, "w") as f:
    json.dump(plan_graph, f, indent=2)