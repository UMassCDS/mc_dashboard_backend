import os
import glob
import json
import yaml
from argparse import ArgumentParser
import datetime as dt
import time
import subprocess

EXPERIMENTS_DIR = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/experiments'

start_time = time.time()

class Config(object):  
    def __init__(self, config_path):
        with open(config_path) as cf_file:
            self._data = yaml.safe_load( cf_file.read() )

    def get(self, path=None, default=None):
        # we need to deep-copy self._data to avoid over-writing its data
        sub_dict = dict(self._data)

        if path is None:
            return sub_dict

        path_items = path.split("/")[:-1]
        data_item = path.split("/")[-1]

        try:
            for path_item in path_items:
                sub_dict = sub_dict.get(path_item)

            value = sub_dict.get(data_item, default)

            return value
        except (TypeError, AttributeError):
            return default

'''
Read the following from the config file:

exp_name: Name of the specific experiment
exp_num: Experiment number
start_date: Start date for data collection
num_days: Number of days to collect data after start_date (default: 7)
top_k_tf_idf: Minimum number of NE in an article. And also, the top-k NEs with the highest TF-IDF values to be considered for ne_art_index.py
jaccard_threshold: Jaccard similarity threshold for NEs for two articles
drop_top_ne_num: Number of NE with the highest frequency to drop from our analysis as they do not provide a lot of information (like Trump, Biden, etc.) 
'''

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", dest="config_file",
                                required=True,
                                help="Full path to config file")
    args = parser.parse_args()    

    cfg = Config(args.config_file)
    
    exp_name = cfg.get('meta/exp_name')
    exp_num = 'exp_' + str(cfg.get('meta/exp_num'))
    start_date = cfg.get('start_date')
    # start_date_dt = cfg.get('start_date')
    num_days = cfg.get('num_days')
    top_k_tf_idf = cfg.get('top_k_tf_idf') ## Min NE in an article as well for filtering
    jaccard_threshold = cfg.get('jaccard_threshold')
    drop_top_ne_num = cfg.get('drop_top_ne_num')
    data_dir = cfg.get('data_dir')

    ## Create datetime objects for start_date and end_date
    start_date_split = start_date.split("-")
    start_date_dt = dt.date(int(start_date_split[0]), int(start_date_split[1]), int(start_date_split[2]))
    # end_date_dt = start_date_dt + dt.timedelta(days=int(num_days))
    
    ## Create strings for start_date and end_date
    start_date = start_date_dt.strftime("%Y-%m-%d")
    end_date = (start_date_dt + dt.timedelta(days=num_days-1)).strftime("%Y-%m-%d")
    
    folder_name = f'{start_date}_to_{end_date}'
    
    # cmd1 = f'''
    # python create_index.py -s {start_date} -n {num_days} --data_dir '{data_dir}' --output_dir {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name)} --min_ne_num {top_k_tf_idf}
    # '''
    # print('*'*100)
    # print(cmd1)
    # os.system(cmd1)
    # print('*'*100)

    print('*'*100)
    print('cmd1')
    result_cmd1 = subprocess.run(['python', 'create_index.py', '-d', '-s', f'{start_date}', '-n', f'{num_days}', '--data_dir', f'{data_dir}', '--output_dir', f'{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name)}', '--min_ne_num', f'{top_k_tf_idf}'], capture_output=True, text=True)
    # Print the output
    print(result_cmd1.stdout)
    print('*'*100)

    # cmd2 = f'''
    # python ne_art_index.py -i {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_filtered_by_ne_count.index')} -o {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_filtered_by_ne_tf_idf.index')} --top_k {top_k_tf_idf}
    # '''
    # cmd2 = f'''
    # python ne_art_index.py -i {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_filtered_by_ne_count.index')} -o {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_filtered_by_ne_tf_idf.index')} 
    # '''
    # print('*'*100)
    # print(cmd2)
    # os.system(cmd2)
    # print('*'*100)
    print('*'*100)
    print('cmd2')
    result_cmd2 = subprocess.run(['python', 'ne_art_index.py', '-i', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_filtered_by_ne_count.index')}", '-o', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_filtered_by_ne_tf_idf.index')}"], capture_output=True, text=True)
    # Print the output
    print(result_cmd2.stdout)
    print('*'*100)

    # cmd3 = f'''
    # ./c_extract_pair {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_filtered_by_ne_tf_idf.index')}   {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/extracted_pairs.txt')} {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_sorted_by_count.index')} {drop_top_ne_num}
    # '''
    # print('*'*100)
    # print(cmd3)
    # os.system(cmd3)
    # print('*'*100)

    print('*'*100)
    print('cmd3')
    result_cmd3 = subprocess.run(['./c_extract_pair', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_filtered_by_ne_tf_idf.index')}", f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/extracted_pairs.txt')}", f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/indexes_sorted_by_count.index')}", f"{drop_top_ne_num}"], capture_output=True, text=True)
    print(result_cmd3.stdout)
    print('*'*100)

    # cmd4 = f'''
    # python pair_candidate.py -i {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/extracted_pairs.txt')} -o {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'candidates')} --jaccard_sim_intra_lang {jaccard_threshold} --jaccard_sim_inter_lang 0.1
    # '''
    # print('*'*100)
    # print(cmd4)
    # os.system(cmd4)
    # print('*'*100)
    print('*'*100)
    print('cmd4')
    result_cmd4 = subprocess.run(['python', 'pair_candidate.py', '-i', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'indexes/extracted_pairs.txt')}", '-o', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'candidates')}", '--jaccard_sim_intra_lang', f"{jaccard_threshold}", '--jaccard_sim_inter_lang', '0.1'] , capture_output=True, text=True)
    print(result_cmd4.stdout)
    print('*'*100)

    # cmd5 = f'''python load_model_inference.py -i {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'candidates/final_candidates.jsonl')} -tl 56 -nt positive
    # '''
    # print('*'*100)
    # print(cmd5)
    # os.system(cmd5)
    # print('*'*100)
    print('*'*100)
    print('cmd5')
    result_cmd5 = subprocess.run(['python', 'load_model_inference.py', '-i', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'candidates/final_candidates.jsonl')}", '-tl', '56', '-nt', 'positive'] , capture_output=True, text=True)
    print(result_cmd5.stdout)
    print('*'*100)

    # cmd6 = f'''
    # python create_data_for_oslom.py -i {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'prediction/final_prediction.jsonl')} -o {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'oslom_input/input.dat')}
    # '''
    # print('*'*100)
    # print(cmd6)
    # os.system(cmd6)
    # print('*'*100)
    print('*'*100)
    print('cmd6')
    result_cmd6 = subprocess.run(['python', 'create_data_for_oslom.py', '-i', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'prediction/final_prediction.jsonl')}", '-o', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'oslom_input/input.dat')}"] , capture_output=True, text=True)
    print(result_cmd6.stdout)
    print('*'*100)
    
    # cmd7 = f'''
    # ./OSLOM2/oslom_undir -f {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'oslom_input/input.dat')} -w
    # ''' 
    # print('*'*100)
    # print(cmd7)
    # os.system(cmd7)
    # print('*'*100)
    print('*'*100)
    print('cmd7')
    result_cmd7 = subprocess.run(['./OSLOM2/oslom_undir', '-f', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'oslom_input/input.dat')}", '-w'] , capture_output=True, text=True)
    print(result_cmd7.stdout)
    print('*'*100)

    # # cmd8 = f'''python headlines_for_clusters.py -p -i '{data_dir}/2024*/*/wikilinked/en/*.json' -g '{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'prediction/story_id_to_graph_id.jsonl')}' -c {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'oslom_input/input.dat_oslo_files/partitions_level_0')} -o {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'clustered_headlines')}
    # # '''
    # cmd8 = f'''python format_data_for_frontend.py -p -s {start_date} -n {num_days} -i '{data_dir}' -g '{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'prediction/story_id_to_graph_id.jsonl')}' -c {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'oslom_input/input.dat_oslo_files/partitions_level_0')} -o {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'clustered_headlines')}
    # '''
    # # cmd8 = f'''python headlines_for_clusters.py -i -p '{data_dir}/2024*/*/wikilinked/en/*.json' -g '{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'prediction/story_id_to_graph_id.jsonl')}' -c {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'oslom_input/input.dat_oslo_files/partitions_level_0')} -o {os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'clustered_headlines')}
    # # '''
    # print('*'*100)
    # print(cmd8)
    # os.system(cmd8)
    # print('*'*100)
    print('*'*100)
    print('cmd8')
    result_cmd8 = subprocess.run(['python', 'format_data_for_frontend.py', '-p', '-s', f"{start_date}", '-n', f"{num_days}", '-i', f"'{data_dir}'", '-g', f"'{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'prediction/story_id_to_graph_id.jsonl')}'", '-c', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'oslom_input/input.dat_oslo_files/partitions_level_0')}", '-o', f"{os.path.join(EXPERIMENTS_DIR, exp_num, exp_name, folder_name, 'clustered_headlines')}"] , capture_output=True, text=True)
    print(result_cmd8.stdout)
    print('*'*100)

print("Time taken (mins):", (time.time()-start_time)/60)