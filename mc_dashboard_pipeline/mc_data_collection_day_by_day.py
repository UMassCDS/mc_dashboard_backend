import os, mediacloud.api
from importlib.metadata import version
import datetime as dt
import time
import json
from tqdm.notebook import tqdm
import pickle as pkl
from argparse import ArgumentParser
import spacy

NER = spacy.load('en_core_web_sm')
# DATA_DIR = '../../data'

# Set your personal API KEY
MC_API_KEY = '57c1834ae9810bbc6de56e7e38c0d886a08008ca'
mc_search = mediacloud.api.SearchApi(MC_API_KEY)
GET_TEXT_FOR_ARTICLES = True
f'Using Media Cloud python client v{version("mediacloud")}'

collection_to_id_dict = {'mostly_left': 231013063,
                         'somewhat_left': 231013089,
                         'center': 231013108,
                         'somewhat_right': 231013109,
                         'mostly_right': 231013110,}
                        #  'us_national': 34412234}

def save_pkl(obj, filename):
    with open(filename, 'wb') as f:
        pkl.dump(obj, f)

def load_pkl(filename):
    with open(filename, 'rb') as f:
        obj = pkl.load(f)
    return obj

def run_ner(article):
    doc = NER(article)
    ners = []
    for ent in doc.ents:
            ners.append({
                "label": str(ent.label_),
                "text": str(ent.text),
                "start_char": ent.start_char,
                "end_char": ent.end_char
            })

    return ners

def get_data_and_write_to_file(query, start_date, collection_name, out_dir):
    end_date= start_date ## As we are collecting data for only one day          
    pagination_token = None
    more_stories = True
    count = 0
   
    # if not os.path.exists(os.path.join(DATA_DIR, 'mc_data_day_by_day_trump_or_biden', f'{start_date}', f'{collection_name}')):
    #     os.makedirs(os.path.join(DATA_DIR, 'mc_data_day_by_day_trump_or_biden', f'{start_date}', f'{collection_name}'))
    if not os.path.exists(os.path.join(out_dir, f'{start_date}', f'{collection_name}')):
        os.makedirs(os.path.join(out_dir, f'{start_date}', f'{collection_name}'))
    while more_stories:
        page, pagination_token = mc_search.story_list(query, 
                                                    start_date= start_date, 
                                                    end_date= end_date, 
                                                    expanded=GET_TEXT_FOR_ARTICLES, 
                                                    collection_ids=[collection_to_id_dict[collection_name]],
                                                    pagination_token=pagination_token)
        
        for single_entry in page:
            if GET_TEXT_FOR_ARTICLES:
                single_entry['spacy'] = run_ner(single_entry['text'])
            lang = single_entry['language']

            if not os.path.exists(os.path.join(out_dir, f'{start_date}', f'{collection_name}', f'{lang}')):
                os.makedirs(os.path.join(out_dir, f'{start_date}', f'{collection_name}', f'{lang}'))
                with open(os.path.join(out_dir, f'{start_date}', f'{collection_name}', f'{lang}', 'data.json'), 'w') as f_write:
                    json.dump(single_entry, f_write, default=str)
                    f_write.write('\n')
                    count += 1
            else:
                with open(os.path.join(out_dir, f'{start_date}', f'{collection_name}', f'{lang}', 'data.json'), 'a') as f_write:
                    json.dump(single_entry, f_write, default=str)
                    f_write.write('\n')
                    count += 1
        more_stories = pagination_token is not None
    print(f'{start_date}-{end_date} for {collection_name}: {count}')


            
if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-s", "--date", dest="start_date",
                            default=0, type=str,
                            help="The date for data collection. Format: YYYY-MM-DD")
    parser.add_argument("-c", "--collection", dest="collection_name",
                            default='us_national', type=str,
                            help="Name of the collection to collect data from. Options: us_national, mostly_left, somewhat_left, center, somewhat_right, mostly_right.")
    parser.add_argument("-o", "--out_dir", dest="out_dir",
                            required=True, type=str,
                            help="Path to the output directory")
    parser.add_argument("-q", "--search_using_query", dest="use_query",
                        required=True, type=bool,
                        help="Get data using the query (get all news otherwise)")

    args = parser.parse_args()
    start_date = args.start_date.split("-")
    if len(start_date)==1 or len(start_date[0])!=4:
        raise Exception("Start date format is not correct. Correct format: YYYY-MM-DD")
    if args.collection_name not in collection_to_id_dict.keys():
        raise Exception(f'Collection name is not correct. Available options: {list(collection_to_id_dict.keys())}')
    start_date_dt = dt.date(int(start_date[0]), int(start_date[1]), int(start_date[2]))
    
    if args.use_query:
        # query = '''
        # (Trump AND Biden) OR 
        # ("democrat democrat"~1000 OR 
        # "democrats democrat"~1000 OR 
        # "democrats democrats"~1000 OR 
        # "republican republican"~1000 OR 
        # "republican republicans"~1000 OR 
        # "republicans republicans"~1000) OR 
        # (democrat* AND republican*)
        # '''
        query = '''
        (Trump OR Biden) OR 
        ("democrat democrat"~1000 OR 
        "democrats democrat"~1000 OR 
        "democrats democrats"~1000 OR 
        "republican republican"~1000 OR 
        "republican republicans"~1000 OR 
        "republicans republicans"~1000) OR 
        (democrat* AND republican*) AND
        language:en
        '''
        output_dir = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day'
    else:
        query = '*  and language:en'
        output_dir = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day_all_news'
        
    get_data_and_write_to_file(query, start_date_dt, args.collection_name, args.out_dir)

    if GET_TEXT_FOR_ARTICLES:
        wikify_cmd = f'''python wikify_for_ner.py -i "{args.out_dir}/{args.start_date}/{args.collection_name}/en/data.json"'''
        print(wikify_cmd)
        os.system(wikify_cmd)

        get_offsets_cmd = f'''python create_offset_files.py -i "{args.out_dir}/{args.start_date}/{args.collection_name}/wikilinked/en/data_wikified.json"
        '''
        print(get_offsets_cmd)
        os.system(get_offsets_cmd)
