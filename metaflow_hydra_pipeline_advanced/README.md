# Metaflow - Hydra integration

Make sure install the requirements before running the code below. 
Please create new python environments and then install the below requirements. 

```sh
pip install -r metaflow_hydra_pipeline_simple/requirement.txt
```

## Default config runs
You can now run (from base of project) - this would read default config file as
provided in the code.


Command to run pipeline:
```sh
python metaflow_hydra_pipeline_advanced/metaflow_hydra_pipeline_simple.py run
```

Command to just show the pipeline:
```sh
python metaflow_hydra_pipeline_advanced/metaflow_hydra_pipeline_simple.py show
```

## Config file overriding

For overriding, provide the name file name on command line.
```sh
 python metaflow_hydra_pipeline_advanced/metaflow_hydra_pipeline_simple.py run --config_name=config_tree_2
```
