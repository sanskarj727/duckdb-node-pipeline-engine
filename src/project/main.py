import json
from src.project.project import Project

def main():
    with open("configs/Scribble.json", "r") as f:
        pipeline_config = json.load(f)
    proj = Project(pipeline_config)
    proj.run()

if __name__ == "__main__":
    main()

# get number of rows before and after
# fix derive - printing less number of rows than fetching main table

# along with documentation, add another doc which defines config for each node and how it is done


# report format - problem -> approach -> abstract design of implementation (architecture) and detailed implementation design 

'''

Handling Missing Data	Drop rows or columns that contain missing values		Customize to drop based on a threshold of missing values
Handling Missing Data	fill_missing_values		Fills missing data with constant values or statistical measures. Supports imputation strategies (mean, median, mode or custom values), allows using other columns's data for imputation
Reshaping	choose columns/delete columns		Select the columns to keep
Reshaping	choose rows/delete rows		Select the rows to keep


'''