import json
import os
import glob
from argparse import ArgumentParser
import datetime as dt
import requests
import trafilatura

# OUTPUT_DIR = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/clustered_headlines'

'''
python headlines_for_clusters.py -p -s 2024-01-01 -n 7 -i '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day' -g '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/experiments/exp_2/tf_idf_10_jacc_0.15_drop_ne_25/2024-01-01_to_2024-01-07/prediction/story_id_to_graph_id.jsonl' -c  /work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/experiments/exp_2/tf_idf_10_jacc_0.15_drop_ne_25/2024-01-01_to_2024-01-07/oslom_input/input.dat_oslo_files/partitions_level_0 -o /work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/experiments/exp_2/tf_idf_10_jacc_0.15_drop_ne_25/2024-01-01_to_2024-01-07/clustered_headlines_json
'''

'''
/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/2024-07-10/center/en/data.json
/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day_all_news/2024-07-14/center/wikilinked/en/data_wikified.json
'''

def create_story_id_to_headline_dict(all_input_files):
    story_id_to_art = {}
    for filename in all_input_files:
        with open(filename, 'r') as f:
            for line in f:
                data = json.loads(line)
                story_id_to_art[data['id']] = {'title': data['title'],  'url': data['url'], 'collection': filename.split("/")[-4]}
                # story_id_to_collection[data['id']] = filename.split("/")[-4]

    return story_id_to_art

def get_graph_id_to_story_id(path_to_file):
    graph_id_to_story_id = {}
    with open(path_to_file, 'r') as f:
        for line in f:
            data = json.loads(line)
            graph_id_to_story_id[list(data.values())[0]] = list(data.keys())[0] 
    
    return graph_id_to_story_id

def read_file_with_cluster_data(cluster_output_file):
    line_count = 0
    cluster_id_to_graph_id_list = {}
    cluster_id_to_size_of_cluster = {}
    num_clusters_list = []
    with open(cluster_output_file, 'r') as f:
        data = f.read().splitlines()
        for line in data:
            if "#" in line:
                cluster_id = line.strip()
            else:
                # if line_count%2!=0:
                line = line.strip()
                graph_id_list = [int(graph_id) for graph_id in line.split(" ")]
                num_clusters_list.append(len(graph_id_list))
                cluster_id_to_graph_id_list[cluster_id] = graph_id_list
                cluster_id_to_size_of_cluster[cluster_id] = len(graph_id_list)
                # print(graph_id_str)
            line_count += 1
    return cluster_id_to_graph_id_list, cluster_id_to_size_of_cluster

def get_top_image(URL):  
    try:
        html = requests.get(URL).content
        doc = trafilatura.metadata.extract_metadata(html)
        top_image_url = doc.image
    except:
        top_image_url = None
    return top_image_url

if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument("-s", "--date", dest="start_date",
						required=True, type=str,
						help="The date for data collection. Format: YYYY-MM-DD")
    parser.add_argument("-n", "--num_days", dest="num_days",
						required=True, type=int,
						help="Number of days ")
    parser.add_argument("-i", "--input_dir", dest="input_dir",
                            required=True, type=str,
                            help="Input parent directory.")
    parser.add_argument("-g", "--graph_id_to_story", dest="graph_id_to_story_id_filename",
                            required = True,
                            help="Graph ID to Story ID filename")
    parser.add_argument("-c", "--cluster_filename", dest="cluster_output_filename",
                            required = True,
                            help="Partition file from OSLOM")
    parser.add_argument("-o", "--out_dir", dest="out_dir",
                        required = True,
                            help="Output directory for clustered headlines")
    parser.add_argument("-p", "--political_news_only", dest="political_news_only",
                        default=False, action='store_true',
                        help="Only keep political news headlines in clusters")
    
    args = parser.parse_args()
    all_clusters = {}
    top_20_clusters = {}
    # input_glob = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/2024*/*/wikilinked/en/*.json'
    # graph_id_to_story_id_filename = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/prediction/prediction-top10-ne-filtered_0_10000/story_id_to_graph_id.jsonl'
    # cluster_output_filename = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/oslom_input/input2.dat_oslo_files/partitions_level_0'
    
    input_dir = args.input_dir
    graph_id_to_story_id_filename = args.graph_id_to_story_id_filename
    cluster_output_filename = args.cluster_output_filename
    output_dir = args.out_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    start_date_split = args.start_date.split("-")
    start_date_dt = dt.date(int(start_date_split[0]), int(start_date_split[1]), int(start_date_split[2]))
    end_date_dt = start_date_dt + dt.timedelta(days=int(args.num_days-1))
    end_date_str = end_date_dt.strftime('%Y-%m-%d')

    all_input_files = []
    for i in range(int(args.num_days)):
        globstring = glob.glob(os.path.join(args.input_dir,  (start_date_dt + dt.timedelta(days=i)).strftime("%Y-%m-%d"), "*", "wikilinked", "en", "*.json"))
        for ele in globstring:
            all_input_files.append(ele) ##Only working with English articles for now.
	# for globstring in globstrings:
	# 	print(glob.glob(globstring))
	# print(globstrings)
    
    cluster_id_to_graph_id_list, cluster_id_to_cluster_size = read_file_with_cluster_data(cluster_output_filename)
    graph_id_to_story_id = get_graph_id_to_story_id(graph_id_to_story_id_filename)
    story_id_to_art =  create_story_id_to_headline_dict(all_input_files)
    # print(len(all_input_files))
    print(len(story_id_to_art.keys()))
    # print(story_id_to_art)
    final_filename = f'{args.start_date}_to_{end_date_str}'
##################################################### All clusters ##################################################
    
    with open(os.path.join(output_dir, f'{final_filename}_clusters_with_headlines.jsonl'), 'w') as f:
        for cluster_id, graph_id_list in cluster_id_to_graph_id_list.items():
            # f.write(f"Cluster {cluster_id}\n")
            single_cluster = {}
            try:
                for graph_id in graph_id_list:
                    # print(story_id_to_art[graph_id_to_story_id[graph_id]])
                    if 'articles' not in single_cluster.keys():
                        single_cluster['id'] = cluster_id
                        single_cluster['name'] = story_id_to_art[graph_id_to_story_id[graph_id]]['title']
                        story_id_to_art[graph_id_to_story_id[graph_id]]['story_id'] = graph_id_to_story_id[graph_id]
                        single_cluster['articles'] = [story_id_to_art[graph_id_to_story_id[graph_id]]]
                    else:
                        story_id_to_art[graph_id_to_story_id[graph_id]]['story_id'] = graph_id_to_story_id[graph_id]
                        single_cluster['articles'].append(story_id_to_art[graph_id_to_story_id[graph_id]])
                json.dump(single_cluster, f)
                f.write('\n')
            except Exception as e:
                print(repr(e))
                continue

##################################################### Top 20 clusters ##################################################
    count = 0
    cluster_id_to_cluster_size_without_repetitions = {}
    for cluster_id, cluster_size in cluster_id_to_cluster_size.items():
        if count!=0 and '#module 0' in cluster_id:
            break
        else:
            cluster_id_to_cluster_size_without_repetitions[cluster_id] = cluster_size
        count += 1
    
    sorted_cluster_id_to_cluster_size_without_repetitions_top_20 = dict(sorted(cluster_id_to_cluster_size_without_repetitions.items(), key=lambda item: item[1], reverse=True)[:20])


    with open(os.path.join(output_dir, f'{final_filename}_clusters_with_headlines_top_20.jsonl'), 'w') as f:
        for cluster_id in sorted_cluster_id_to_cluster_size_without_repetitions_top_20.keys():
            # f.write(f"Cluster {cluster_id}\n")
            single_cluster_top_20 = {}
            for graph_id in cluster_id_to_graph_id_list[cluster_id]:
                if 'articles' not in single_cluster_top_20.keys():
                    single_cluster_top_20['id'] = cluster_id
                    single_cluster_top_20['name'] = story_id_to_art[graph_id_to_story_id[graph_id]]['title']
                    story_id_to_art[graph_id_to_story_id[graph_id]]['story_id'] = graph_id_to_story_id[graph_id]
                    single_cluster_top_20['articles'] = [story_id_to_art[graph_id_to_story_id[graph_id]]]
                else:
                    story_id_to_art[graph_id_to_story_id[graph_id]]['story_id'] = graph_id_to_story_id[graph_id]
                    single_cluster_top_20['articles'].append(story_id_to_art[graph_id_to_story_id[graph_id]])
            json.dump(single_cluster_top_20, f)
            f.write('\n')
##########################################################################################################################

    if args.political_news_only:
        all_political_clusters = {}
        top_20_political_clusters = {}
        folder_name = args.input_dir.split("/")[-1]
        input_filenames_political = [filename.replace('_all_news', '').replace('wikilinked/', '').replace('_wikified', '') for filename in all_input_files]
        # input_filenames_political = [for filename in all_input_files]
        # input_glob_political = input_glob.replace('mc_data_day_by_day_all_news', 'mc_data_day_by_day')
        story_id_to_political_art = create_story_id_to_headline_dict(input_filenames_political)

        total_number_of_articles = len(list(story_id_to_political_art.keys()))
##################################################### All political clusters ##################################################

        with open(os.path.join(output_dir, f'{final_filename}_clusters_with_headlines_only_political.jsonl'), 'w') as f:
            for cluster_id, graph_id_list in cluster_id_to_graph_id_list.items():
                single_cluster_political = {}
                for graph_id in graph_id_list:
                    if graph_id_to_story_id[graph_id] in story_id_to_political_art.keys():
                        if 'article' not in single_cluster_political.keys():
                            single_cluster_political['id'] = cluster_id
                            story_id_to_political_art[graph_id_to_story_id[graph_id]]['story_id'] = graph_id_to_story_id[graph_id]
                            single_cluster_political['name'] = story_id_to_political_art[graph_id_to_story_id[graph_id]]['title']
                            single_cluster_political['articles'] = [story_id_to_political_art[graph_id_to_story_id[graph_id]]]
                        else:
                            story_id_to_political_art[graph_id_to_story_id[graph_id]]['story_id'] = graph_id_to_story_id[graph_id]
                            single_cluster_political['articles'].append(story_id_to_political_art[graph_id_to_story_id[graph_id]])
                json.dump(single_cluster_political, f)
                f.write('\n')
                    
        count = 0
        cluster_id_to_graph_id_list_only_political_without_repetitions = {}
        for cluster_id, graph_id_list in cluster_id_to_graph_id_list.items():
            if count!=0 and '#module 0' in cluster_id:
                break
            else:
                for graph_id in graph_id_list:
                    if graph_id_to_story_id[graph_id] in story_id_to_political_art.keys():
                        if cluster_id in cluster_id_to_graph_id_list_only_political_without_repetitions.keys():
                            cluster_id_to_graph_id_list_only_political_without_repetitions[cluster_id].append(graph_id)
                        else:
                            cluster_id_to_graph_id_list_only_political_without_repetitions[cluster_id] = [graph_id]
            count += 1

        cluster_id_to_cluster_id_only_political_without_repetitions = {}
        for cluster_id, graph_id_list in cluster_id_to_graph_id_list_only_political_without_repetitions.items():
            cluster_id_to_cluster_id_only_political_without_repetitions[cluster_id] = len(graph_id_list)

        cluster_id_to_cluster_id_only_political_without_repetitions_top_20 = dict(sorted(cluster_id_to_cluster_id_only_political_without_repetitions.items(), key=lambda item: item[1], reverse=True)[:20])

##################################################### Top 20 political clusters ##################################################
        all_mostly_left_art = {}
        all_somewhat_left_art = {}
        all_center_art = {}
        all_somewhat_center_art = {}
        all_mostly_right_art = {}
        all_collections = ['mostly_left', 'somewhat_left', 'center', 'somewhat_right', 'mostly_right']
        used_story_ids_in_summary = set() ## Make sure that the summary for the same collection does not contain the same headline twice 
        used_story_ids_in_combined_treemap = set()

        with open(os.path.join(output_dir, f'{final_filename}_clusters_with_headlines_only_political_top_20.jsonl'), 'w') as f:
            for cluster_id in cluster_id_to_cluster_id_only_political_without_repetitions_top_20.keys():
                # f.write(f"Cluster {cluster_id}\n")
                all_art_by_collection = {}
                single_cluster_political_top_20 = {}
                unique_cluster_headline_found = 0
                for graph_id in cluster_id_to_graph_id_list_only_political_without_repetitions[cluster_id]:
                    
                    if story_id_to_art[graph_id_to_story_id[graph_id]]['collection'] not in all_art_by_collection.keys():
                        all_art_by_collection[story_id_to_art[graph_id_to_story_id[graph_id]]['collection']] = [story_id_to_art[graph_id_to_story_id[graph_id]]]
                    else:
                        all_art_by_collection[story_id_to_art[graph_id_to_story_id[graph_id]]['collection']].append(story_id_to_art[graph_id_to_story_id[graph_id]])

                    if 'articles' not in single_cluster_political_top_20.keys():
                        single_cluster_political_top_20['id'] = cluster_id
                        if graph_id_to_story_id[graph_id] not in used_story_ids_in_combined_treemap:
                            # single_cluster_political_top_20['name'] = story_id_to_art[graph_id_to_story_id[graph_id]]['title']
                            cluster_headline = story_id_to_art[graph_id_to_story_id[graph_id]]['title']
                            used_story_ids_in_combined_treemap.add(graph_id_to_story_id[graph_id])
                            unique_cluster_headline_found = 1
                        story_id_to_art[graph_id_to_story_id[graph_id]]['story_id'] = graph_id_to_story_id[graph_id]
                        single_cluster_political_top_20['articles'] = [story_id_to_art[graph_id_to_story_id[graph_id]]]
                    else:
                        if not unique_cluster_headline_found and graph_id_to_story_id[graph_id] not in used_story_ids_in_combined_treemap:
                            cluster_headline = story_id_to_art[graph_id_to_story_id[graph_id]]['title']
                            used_story_ids_in_combined_treemap.add(graph_id_to_story_id[graph_id])
                            unique_cluster_headline_found = 1
                        story_id_to_art[graph_id_to_story_id[graph_id]]['story_id'] = graph_id_to_story_id[graph_id]
                        single_cluster_political_top_20['articles'].append(story_id_to_art[graph_id_to_story_id[graph_id]])
                    single_cluster_political_top_20['name'] = cluster_headline
                # collections_present = set(all_art_by_collection.keys())
                # collections_not_present = all_collections.difference(collections_present)
                # for collection in all_collections:
                #     print(collection)
                #     print(all_art_by_collection[collection][:5])

                for collection in all_collections:
                    prev_art = None
                    if collection in all_art_by_collection.keys():
                        image_found_flag = 0
                        num_art_in_cluster_for_collection = len(all_art_by_collection[collection])
                        for art in all_art_by_collection[collection]:
                            if art['story_id'] not in used_story_ids_in_summary:
                                top_image = get_top_image(art['url'])
                                if top_image is not None:
                                    image_found_flag = 1
                                    single_cluster_political_top_20[f'{collection}_summary'] = {'article': art, 'image_url': top_image, 'num_art_in_cluster_for_this_collection':num_art_in_cluster_for_collection, 'total_num_articles':total_number_of_articles}
                                    summary_story_id = art['story_id']
                                    break
                        if not image_found_flag:
                            if art['story_id'] in used_story_ids_in_summary:
                                single_cluster_political_top_20[f'{collection}_summary'] = {'article': prev_art, 'image_url': None, 'num_art_in_cluster_for_this_collection':num_art_in_cluster_for_collection, 'total_num_articles':total_number_of_articles}
                                summary_story_id = prev_art['story_id']
                            else:
                                single_cluster_political_top_20[f'{collection}_summary'] = {'article': art, 'image_url': None, 'num_art_in_cluster_for_this_collection':num_art_in_cluster_for_collection, 'total_num_articles':total_number_of_articles}
                                summary_story_id = art['story_id']
                        prev_art = art
                        used_story_ids_in_summary.add(summary_story_id)
                    else:
                        single_cluster_political_top_20[f'{collection}_summary'] = {'article': None, 'image_url': None, 'num_art_in_cluster_for_this_collection':0, 'total_num_articles':total_number_of_articles}

                json.dump(single_cluster_political_top_20, f)
                f.write('\n')
                
    # print(cluster_id_to_graph_id_list)