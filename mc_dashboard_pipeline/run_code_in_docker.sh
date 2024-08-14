#!/bin/bash

# Get the day of the week (1=Monday, 7=Sunday)
day_of_week=$(date +%u)

# # Check if it's Monday
if [ "$day_of_week" -eq 1 ]; then
    python get_data_docker.py
    python get_data_docker.py -q
    python run_experiments_on_full_pipeline_docker.py
else
    python get_data_docker.py
    python get_data_docker.py -q
fi