import os
import yaml
import time
import uuid
import pandas as pd
import numpy as np
import datetime as dt
from datetime import date
import subprocess

CONFIG_FILES_DIR = './experiments/2024/config_files'
EXPERIMENTS_DIR = './data/experiments'
# DATA_DIR = './data/mc_data_day_by_day_all_news'
# EXPERIMENTS_DIR = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/experiments'
# DATA_DIR = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day_all_news'

exp_details = {'exp_num': 9,
               'desc': 'Check Docker'}

config_dict = {'meta': {'exp_name': None, 'exp_num': None, 'desc': None},
            'start_date': None,
            'num_days': None,
            'top_k_tf_idf': None,
            'jaccard_threshold': None,
            'drop_top_ne_num': None,
            }

# start_date = '2024-06-29'
start_dates = ['2024-07-08']#, '2024-07-15', '2024-07-22', '2024-07-29']
# yesterday_dt = date.today() - dt.timedelta(days=1)
# start_date_dt = yesterday_dt - dt.timedelta(days=7)
# start_dates = [start_date_dt.strftime('%Y-%m-%d')] 
num_days = 7

top_k_tf_idf_list = [10]
# jaccard_threshold_list = [0.10, 0.15, 0.20, 0.25]
jaccard_threshold_list = [0.15]
# drop_top_ne_num_list = [10, 25]
drop_top_ne_num_list = [25]

for start_date in start_dates:
    start_date_split = start_date.split("-")
    start_date_dt = dt.date(int(start_date_split[0]), int(start_date_split[1]), int(start_date_split[2]))  
    end_date_dt = start_date_dt + dt.timedelta(days=int(num_days))
    start_date = start_date_dt.strftime("%Y-%m-%d")
    end_date = (start_date_dt + dt.timedelta(days=num_days-1)).strftime("%Y-%m-%d")
    folder_name = f'{start_date}_to_{end_date}'

    # for random_subsample_size in random_subsample_sizes:
    for top_k_tf_idf in top_k_tf_idf_list:
        for jaccard_threshold in jaccard_threshold_list:
            for drop_top_ne_num in drop_top_ne_num_list:
                # print("Here")
                experiment = {}

                exp_name = f'min_ne_{top_k_tf_idf}_jacc_{jaccard_threshold:.2f}_drop_ne_{drop_top_ne_num}'
                print(exp_name)
                output_dir = os.path.join(EXPERIMENTS_DIR, f"exp_{exp_details['exp_num']}", exp_name, f'{folder_name}')
                if os.path.exists(output_dir):
                    raise Exception(f'{output_dir} already exists. Change exp_name')
                else:
                    os.makedirs(output_dir)

                ## Config filename and results filename will be the same
                config_file_path =  os.path.join(output_dir, 'config.yml') 

                config_dict['meta']['exp_name'] = exp_name
                config_dict['meta']['exp_num'] = exp_details['exp_num']
                config_dict['meta']['desc'] = exp_details['desc']
                config_dict['start_date'] = start_date
                config_dict['num_days'] = num_days
                config_dict['top_k_tf_idf'] = top_k_tf_idf
                config_dict['jaccard_threshold'] = jaccard_threshold
                config_dict['drop_top_ne_num'] = drop_top_ne_num
                config_dict['data_dir'] = DATA_DIR
                with open(config_file_path, 'w') as yaml_file:
                    yaml.dump(config_dict, yaml_file, default_flow_style=False)
                
                # cmd = f'python run_pipeline_end_to_end_docker.py --config {config_file_path}'
                # print(cmd)
                # os.system(cmd)
                print('*'*100)
                result = subprocess.run(['python', 'run_pipeline_end_to_end_docker.py', '--config', f"{config_file_path}"] , capture_output=True, text=True)
                print(result.stdout)
                print('*'*100)

# all_experiments_df = pd.DataFrame(all_experiments)
# all_experiments_df.to_csv(os.path.join(EXPERIMENTS_DIR, 'meta_csv', f'exp_{exp_num}_{election_year}' f'all_experiments_{exp_name}.csv'))
