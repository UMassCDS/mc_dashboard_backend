#!/bin/bash
#SBATCH -c 4  # Number of Cores per Task
#SBATCH --mem=8G  # Requested Memory
#SBATCH -t 24:00:00  # Job time limit
#SBATCH -o /work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/slurm_scratch/data_collection_week_by_week/slurm-%j.out

source /home/cbagchi_umass_edu/.bashrc
conda init bash
conda activate mc_dashboard
cd /work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/code/mc_dashboard_pipeline
python mc_data_collection_day_by_day.py -s $1 -c $2 -o $3