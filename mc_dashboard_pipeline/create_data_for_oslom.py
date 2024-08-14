"""python create_data_for_oslom.py -i /work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/prediction/prediction-top10-ne-filtered_0_100000/2024-07-03_22:15:19.jsonl -o /work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/oslom_input/input.dat"""

import json
import os
from argparse import ArgumentParser

if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument("-i", "--input-file", dest="input_file",
                            type=str,
                            help="Input filename.")
    parser.add_argument("-o", "--output-file", dest="output_file",
                            type=str,
                            help="Output filename.")
    args = parser.parse_args()

    output_dir_name = "/".join(args.output_file.split("/")[:-1])

    if not os.path.exists(output_dir_name):
        os.makedirs(output_dir_name)

    with open(args.output_file, 'w') as f_write:
        with open(args.input_file, 'r') as f:
            for line in f:
                data = json.loads(line)
                art1 = data['graph_id1']
                art2 = data['graph_id2']
                sim = data['similarity']
                f_write.write(f'{art1} {art2} {sim}\n')
                # f_write.write(f'{art2} {art1} {sim}\n')
                