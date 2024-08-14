# Media Cloud Elections Dashboard Backend

## Steps to run code automatically:

The goal of this repository is to create a script that runs every week to cluster the data. 

This can be done using the script ``run_code_in_docker.sh`` using the commands: 

```
cd mc_dashboard pipeline
sh ./run_code_in_docker.sh
```

This needs to be run as a cron job which runs everyday at a set time. It collects data from the day before. On every Monday it runs clustering in addition to data collection.

## Script to run experiments on SLURM:

```
cd mc_dashboard pipeline
python run_experiments_on_full_pipeline_slurm.py
```

This creates config files based on parameters in the .py file and runs the pipeline end to end.


## Build and run Docker container
1. To build: 
2. To run with the data folder mounted as expected: `docker run --mount type=bind,source=/absolute_path/data,target=/scripts/mc_dashboard_pipeline/data mc_backend
