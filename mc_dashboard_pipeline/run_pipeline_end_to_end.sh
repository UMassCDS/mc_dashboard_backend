#!/bin/bash
#SBATCH -c 4  # Number of Cores per Task
#SBATCH --mem=100G  # Requested Memory
#SBATCH -t 72:00:00  # Job time limit
#SBATCH -p gpu-long
#SBATCH --gres=gpu:1 # Number of GPUs
#SBATCH -o /work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/experiments/slurm_scratch/slurm-%j.out  # %j = job ID

source /home/cbagchi_umass_edu/.bashrc
conda init bash
conda activate mc_dashboard
cd /work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/code/mc_dashboard_pipeline
python run_pipeline_end_to_end.py --config $1
