'''
python wikify_for_ner.py -i "/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/*/*/en/data.json"
'''
import sys
import os
import io
import json
from string import digits
import gzip
import glob
import numpy as np
import nltk
import spacy
from nltk.corpus import stopwords
from tqdm import tqdm
import pathlib
import datetime
from argparse import ArgumentParser

def checkNone(it):
    return [] if it == None else it

parser = ArgumentParser()
	# parser.add_argument("-o", "--output-name", dest="output_name",
	# 	default="indexes/en", type=str,
	# 	help="Output index name.")
parser.add_argument("-i", "--input-folder", dest="input",
    required='True', type=str,
    help="Path to input folder regex.")
args = parser.parse_args()

nltk.download('stopwords')
from nltk.tokenize import word_tokenize

remove_digits = str.maketrans('', '', digits)
ALLOWED_ENTITY_TYPES = {'LAW', 'PRODUCT', 'LOC', 'LANGUAGE', 'NORP', 'WORK_OF_ART', 'GPE', 'EVENT', 'PERSON', 'ORG',
                        'FAC'}
lang_list = ['en']#['ar', 'de', 'en', 'es', 'fr', 'it', 'pl', 'ru', 'tr', 'zh']
# load traffic counts

# load concepts
print(f"Start Loading Concepts at {datetime.datetime.now():%Y-%m-%d_%H:%M:%S}")
wiki_concepts = {}
for merge_line in io.open("/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/scott_ner/merged_wiki_data.json", mode="r", encoding="utf-8"):
    row = json.loads(merge_line)
    if row["counts"] and len(row["languages"]) > 1:
        for wiki, term in row["languages"].items():
            if not wiki_concepts.get(term):
                wiki_concepts[term] = []
            new_row = {"term_id": row["term_id"], "term": term, "wiki": wiki}
            for countwiki, count_data in row["counts"].items():
                if countwiki == wiki:
                    new_row["views"] = count_data["views"]
            wiki_concepts[term].append(new_row)
print(f"Finish Loading Concepts at {datetime.datetime.now():%Y-%m-%d_%H:%M:%S}")



# key_route = UK_RU/splitted_by_date
# key_route = sys.argv[1]
# key_route = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/2024-01-01/mostly_left'

# for lang in lang_list:
for filename in glob.glob(args.input):
    print(filename)
    print()
    # if os.path.exists(f"{key_route}/{lang}"):
        # print(lang)
    run_counts = []
    match_counts = []
    concept_sizes = []
    output_folder = os.path.join("/".join(filename.split("/")[:-2]), 'wikilinked', filename.split("/")[-2])
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    if 'json' in filename:
        run_count = 0
        match_count = 0
       
        fh = io.open(filename, mode="r", encoding="utf-8")
        outfile = io.open(os.path.join(output_folder, 'data_wikified.json'), mode="w", encoding="utf-8")
        for lineno, article_line in enumerate(fh):
            try:
                article_entities = json.loads(article_line)
            except:
                sys.stderr.write(f"Error processing line {lineno} of {filename}.\n")
                sys.stderr.write(article_line)
                sys.stderr.write("\n")
                continue
            # print(article_entities)
            article_entities["wiki_concepts"] = []
            final_entity_list = []
            for entity in checkNone(article_entities.get('spacy', [])):
                run_count += 1
                if wiki_concepts.get(entity['text']):
                    wiki_concepts.get(entity['text']).sort(key=lambda x: x.get("views", 0))
                    article_entities["wiki_concepts"].append(wiki_concepts.get(entity['text'])[-1])
                    print(entity['text'], wiki_concepts.get(entity['text'])[-1])
                    entity['text'] = wiki_concepts.get(entity['text'])[-1]['term']
                    match_count += 1
                    final_entity_list.append(entity)
            for entity in checkNone(article_entities.get('polyglot', [])):
                run_count += 1
                for each_entity in entity['text']:
                    if wiki_concepts.get(each_entity):
                        wiki_concepts.get(each_entity).sort(key=lambda x: x.get("views", 0))
                        article_entities["wiki_concepts"].append(wiki_concepts.get(each_entity)[-1])
                        match_count += 1
            print(f"processed {run_count} articles")
            concept_size = []
            for concept in checkNone(article_entities.get('wiki_concepts', [])):
                concept_size.append(len(wiki_concepts.get(concept['term'], [])))
            if concept_size:
                concept_sizes.append(np.mean(concept_size))
            else:
                concept_sizes.append(0.0)
            # del article_entities["text"]  # Remove full text of article.
            article_entities['spacy'] = final_entity_list
            gz = outfile.write(json.dumps(article_entities) + "\n")
        outfile.close()
        run_counts.append(run_count)
        match_counts.append(match_count)
