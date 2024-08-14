''''''

# sbatch -o script_output/load_model_inference/load_model_inference_170_180.out load_model_inference_script.sh "../ner_art_sampling/network_pairs/candidates/candidates-top10-ne-filtered_170_180/*" 170 180 overall positive 4 56

"""python load_model_inference.py -i /work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/candidates/candidates-top10-ne-filtered_0_100000/2024-07-03_22:15:19.jsonl -tl 56 -nt positive"""

# languages in a pair are saved as language1 and a2_language by typo...



import torch, gc
from torch.utils.data import DataLoader
from sentence_transformers import SentenceTransformer, LoggingHandler, losses, util, InputExample
from sentence_transformers.evaluation import EmbeddingSimilarityEvaluator
from sentence_transformers import SentenceTransformer, SentencesDataset, LoggingHandler, losses
from sentence_transformers.readers import InputExample
from argparse import ArgumentParser
import math
import logging
from datetime import datetime
import os
import gzip
import csv
import copy
import json
from collections import Counter
import pandas as pd
import re
import jieba
import glob
from utils_inference import TEXT_MAX_LEN, truncatetext, load_article, score_reverse_normalization
import numpy as np
from tqdm import tqdm

if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument("-i", "--input-glob", dest="input_glob",
                            default="../ner_art_sampling/network_pairs/candidates/candidates-top10-ne-filtered_170_180/*", type=str,
                            help="Input globstring.")
    parser.add_argument("-s", "--start-date", dest="start_date",
                            default=0, type=int,
                            help="The start date for this ne-art index.")
    parser.add_argument("-e", "--end-date", dest="end_date",
                            default=180, type=int,
                            help="The end date for this ne-art index.")
    # parser.add_argument("-lo", "--loss", dest="loss",
    #                     default="overall", type=str,
    #                     help="loss type for training. chosen from overall, multi_label1(add 2 aspects: NET and NAR), and multi_label2(add all 6 aspects)")
    parser.add_argument("-nt", "--norm-type", dest="norm_type",
                        default="positive", type=str,
                        help="normalization function type.")
    # parser.add_argument("-bs", "--batch-size", dest="batch_size",
    #                     default=4, type=int,
    #                     help="batch size.")
    parser.add_argument("-tl", "--tail-length", dest="tail_length",
                        default=0, type=int,
                        help="tail length.")
    args = parser.parse_args()


    # model_save_path = f"model/LaBSE/{args.loss}-{args.norm_type}-tail{args.tail_length}-batch{args.batch_size}"
    model_save_path = f"/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/models/multilabel1-positive-tail56-batch4"
    model = SentenceTransformer(model_save_path)
    data_path = args.input_glob

    art_id_to_integer = {}
    art_count = 0
    n_processedpairs = 0
    n_succeedpairs = 0
    n_failedpairs = 0
    start_time = datetime.now()

    files=list(glob.glob(data_path))
    for file in files:
        output_file = file.replace("candidates","prediction")
        output_dir = "/".join(output_file.split("/")[:-1])
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        embed_file = file.replace("candidates", "embedding")
        embed_dir = "/".join(embed_file.split("/")[:-1])
        if not os.path.exists(embed_dir):
            os.makedirs(embed_dir)

        with open(output_file, "w") as outfile:
            with open(embed_file, "w") as embfile:
                with open(file, "r") as fh:
                    for line in tqdm(fh):
                        n_processedpairs += 1
                        if n_processedpairs % 1000 == 0:
                            print(f"Already processed {n_processedpairs} pairs...  ", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), flush=True)

                        '''for test'''
                        # if n_processedpairs > 10000:
                        #     break

                        try:
                            pair = json.loads(line)
                            a1_file = pair["article1"][0]
                            a1_lineno = pair["article1"][1]
                            a1_date = pair["article1"][2]
                            a2_file = pair["article2"][0]
                            a2_lineno = pair["article2"][1]
                            a2_date = pair["article2"][2]

                            art1 = load_article(a1_file, a1_lineno)
                            art2 = load_article(a2_file, a2_lineno)

                            a1_text_input = art1["title"] + " " + truncatetext(art1["text"], art1["language"], TEXT_MAX_LEN, args.tail_length)
                            a2_text_input = art2["title"] + " " + truncatetext(art2["text"], art2["language"], TEXT_MAX_LEN, args.tail_length)

                            a1_emb = model.encode(a1_text_input)
                            a2_emb = model.encode(a2_text_input)

                            a1_emb_list = a1_emb.tolist()
                            a2_emb_list = a2_emb.tolist()

                            cos_sim = util.pytorch_cos_sim(a1_emb, a2_emb)
                            predict_score = score_reverse_normalization(args.norm_type, cos_sim)

                            ## Creating an integer only ID for input to OSLOM
                            if art1['id'] not in art_id_to_integer.keys():
                                art_id_to_integer[art1['id']] = art_count
                                art_count += 1
                            if art2['id'] not in art_id_to_integer.keys():
                                art_id_to_integer[art2['id']] = art_count
                                art_count += 1
                        
                            # take care!! this is language1 and a2_language by typo!!
                            predict_pair = {
                                "similarity": (1/3)*(np.min([predict_score,4])-1), ## Cutting off score at 4 and then performing the linear transformation mentioned in Appendix A of the paper.
                                "date1":a1_date,
                                "stories_id1":art1["id"],
                                "language1":art1["language"],
                                "graph_id1" : art_id_to_integer[art1['id']],
                                # "media_id1":art1["media_id"],
                                "media_name1":art1["media_name"],
                                "media_url1": art1["media_url"],
                                "date2": a2_date,
                                "stories_id2": art2["id"],
                                "a2_language": art2["language"],
                                "graph_id2" : art_id_to_integer[art2['id']],
                                # "media_id2": art2["media_id"],
                                "media_name2": art2["media_name"],
                                "media_url2": art2["media_url"],
                            }

                            outfile.write(json.dumps(predict_pair))
                            outfile.write("\n")



                            emb_pair = {
                                "embedding1": a1_emb_list,
                                "embedding2": a2_emb_list,
                            }

                            embfile.write(json.dumps(emb_pair))
                            embfile.write("\n")

                        except Exception as e:
                            print(repr(e))
                            n_failedpairs += 1
                        n_succeedpairs += 1

    end_time = datetime.now()
    print(f"Finished at {end_time:%Y-%m-%d %H:%M:%S}")
    print(f"Took {end_time - start_time} seconds to processing {n_processedpairs} pairs....")
    print(f"{n_succeedpairs} succeeded pairs and {n_failedpairs} failed pairs....")

    with open(os.path.join(output_dir, 'story_id_to_graph_id.jsonl'), 'w') as f:
        for key, value in art_id_to_integer.items():
            json.dump({key:value}, f)
            f.write('\n')


