#!/bin/bash

# Get the day of the week (1=Monday, 7=Sunday)
day_of_week=$(date +%u)

# # Check if it's Monday
if [ "$day_of_week" -eq 1 ]; then
    # Run script1.py
    python get_data.py
    python get_data.py -q
    python run_experiments_on_full_pipeline.py
else
    # Run script2.py
    python get_data.py
    python get_data.py -q
fi