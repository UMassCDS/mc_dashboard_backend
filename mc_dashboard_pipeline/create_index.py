'''
python create_index.py -s 2024-01-01 -n 7
'''

## Create an index to easily fetch articles 
#### Index contains filename, story_id, line in file for a specific article, and list of named entities

import json
import sys
import re
import glob
import datetime
import collections
import os
import gzip
import pandas as pd
import datetime as dt
# from cachetools import LRUCache
from argparse import ArgumentParser
import time

# DATA_DIR = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day'

start = time.time()

# version 5 added story_id and title
script_version = 6

letters="abcdefghijklmnopqrstuvwxyz"
assert len(letters)==26
idx=["".join([a,b]) for a in letters for b in letters]

from utils import blockwords,text2tokens, date_diff, MIN_ARTICLE_LENGTH, SHA, START_DATE, DATE_WINDOW,unify_url, read_and_filter_data, sort_indexes_by_count


def create_index(name, globstrings , lang, debug):
	#Keep a list of the 1m most recent urls seen. LRU - least recently used - urls will be removed after hitting this limit
	# recent_url_cache=LRUCache(1000000)

	all_urls = set()
	all_titles = set()
	ne_index = dict()
	total_ent_count = 0

	# let's use "en_short.index" for the two-character NE representation
	# name += "-v"+str(script_version)
	print("Will print to file", name+".index", flush=True)

	# with open(name+".index","w") as out, open(name+"_memes.index","w") as article_meme_file, open(name+".dups","w") as dups:
	with open(name+".index","w") as out:
		# we want in the reverse order, to keep the most recent article
		# we could reverse this ordering
		all_filepaths = []
		for i in range(len(globstrings)):
			file_paths = glob.glob(globstrings[i])
			for file_path in file_paths:
				all_filepaths.append(file_path)
		print(len(all_filepaths))
		sorted_files = sorted(all_filepaths, reverse=True)
		if debug:
			print(sorted_files, flush=True)
		for file in sorted_files:
			print(file,datetime.datetime.now(), flush=True)
			if lang=="auto":
				lang=os.path.dirname(file)[-2:]
				if lang not in [ "ar", "de", "es", "en", "fr", "it", "ko", "pl", "pt", "ru", "tr", "zh" ]:
					print("Last two letters of the directory don't specify a known language code. Please use the -l command line argument to specify the language. Exiting.", flush=True)
					sys.exit()
				if debug: print(lang, flush=True)
			if ".gz" not in file:
				with open(file,"r") as fh:
					for lineno,line in enumerate(fh):
						if debug and lineno>100:
							break

						art=json.loads(line)
						
						if not "id" in art:
							continue
						if not "url" in art:
							continue

						stories_id = art["id"]
						url=art["url"].strip()

						if any([w in art["url"] for w in blockwords]):
							continue

						# skip URLs that we have seen already
						if url not in all_urls:
							all_urls.add(url)
						else:
							continue

						# if recent_url_cache.get(url)!=None:
						# 	continue
						# recent_url_cache[url]=True

						if not "text" in art:
							continue

						words=text2tokens(art["text"], lang)
						if words==None or len(words)<MIN_ARTICLE_LENGTH:
							continue

						# each indexed article needs a title and we don't admit duplicates
						# if the same titles, then log them for inspection
						if "title" in art and art["title"]:
							title = art["title"].strip()
							# title_memeslist[title].append(memes)
							if title not in all_titles:
								all_titles.add(title)
								# title_url[title] = url
							else:
								continue
								# we looked at the resulting dup files, they contained:
								# i) many exact duplicates
								# ii) many articles not relevant socially/politically
						else:
							title = "null"
							# print("Article without title", url)
							continue

						# index of full NEs
						vec=[] #[0]*len(idx) #len(idx)==26**2
						ent_count=0

						if "spacy" in art and art["spacy"] != None:
							for ent in art["spacy"]:
								text=ent["text"]
								if len(text)<2 and lang not in ["zh", "ko"]:
									continue
								if text in ne_index:
									vec.append(ne_index[text])
								else:
									ne_index[text] = total_ent_count
									vec.append(ne_index[text])
									total_ent_count += 1
								ent_count+=1
							if ent_count>0:
								vec = sorted(collections.Counter(vec).items())
								# compute relative date
								# cur_date = file.split("/")[-1].replace(".json", "").replace(".gz", "")
								cur_date = art['publish_date']
								reladate = int(date_diff(cur_date, START_DATE).days)

								out.write(f"{stories_id}\t{file}\t{lineno}\t{reladate}\t{url}\t{vec}\n")
						if "polyglot" in art and art["polyglot"]!=None:
							for ent in art["polyglot"]:
								# the implemention used by xi chen
								for word in ent["text"]:
									if len(word)<2 and lang not in ["zh", "ko"]:
										continue
									if word in ne_index:
										vec.append(ne_index[word])
									else:
										ne_index[word] = total_ent_count
										vec.append(ne_index[word])
										total_ent_count += 1
									ent_count+=1
								# implemention that joins tokenized named entities
								# which seems to correspond to what spacy does
								# however, xi chen's experience is that
								# we get better results without joining
								# text=" ".join(ent["text"])
								# if len(text)<2 and lang not in ["zh", "ko"]:
								# 	continue
								# if text in ne_index:
								# 	vec.append(ne_index[text])
								# else:
								# 	ne_index[text] = total_ent_count
								# 	total_ent_count += 1
								# ent_count+=1
							if ent_count>0:
								vec = sorted(collections.Counter(vec).items())
								# compute relative date
								# cur_date = file.split("/")[-1].replace(".json", "").replace(".gz", "")
								cur_date = art['publish_date']
								reladate = int(date_diff(cur_date, START_DATE).days)
								out.write(f"{stories_id}\t{title}\t{file}\t{lineno}\t{reladate}\t{url}\t{vec}\n")
			else:
				with gzip.open(file,"rt") as fh:
					for lineno, line in enumerate(fh):
						if debug and lineno > 100:
							break

						art = json.loads(line)

						if not "id" in art:
							continue
						if not "url" in art:
							continue

						stories_id = art["id"]
						url = art["url"]

						if any([w in art["url"] for w in blockwords]):
							continue

						#there is no "story_text" attribute in wiki data now

						# if not "story_text" in art:
						# 	continue
						#
						# words = text2tokens(art["story_text"], lang)
						# if words == None or len(words) < MIN_ARTICLE_LENGTH:
						# 	continue

						# skip URLs that we have seen already
						if url not in all_urls:
							all_urls.add(url)
						else:
							continue

						if "title" in art:
							title = art["title"].strip()
							# title_memeslist[title].append(memes)
							if title not in all_titles:
								all_titles.add(title)
								# title_url[title] = url
							else:
								continue
								# we looked at the resulting dup files, they contained:
								# i) many exact duplicates
								# ii) many articles not relevant socially/politically
						else:
							title = "null"
							# print("Article without title", url)
							continue

						# index of full NEs
						vec = []  # [0]*len(idx) #len(idx)==26**2
						ent_count = 0

						if "wiki_concepts" in art and art["wiki_concepts"] != None:
							for ent in art["wiki_concepts"]:
								if len(ent['term']) < 2 and lang not in ["zh", "ko"]:
									continue
								if ent['term'] in ne_index:
									vec.append(int(ent['term_id']))
								else:
									ne_index[ent['term']] = ent['term_id']
									vec.append(int(ent['term_id']))
								ent_count += 1

							if ent_count > 0:
								vec = sorted(collections.Counter(vec).items())
								# compute relative date
								# cur_date = file.split("/")[-1].replace(".json", "").replace(".gz", "")
								cur_date = art['publish_date']
								reladate = int(date_diff(cur_date, START_DATE).days)
								out.write(f"{stories_id}\t{title}\t{file}\t{lineno}\t{reladate}\t{url}\t{vec}\n")

	with open(name+"_ne.index", "w") as out:
		for k,v in ne_index.items():
			key = k.replace('\t', ' ')
			out.write(f"{key}\t{v}\n")

	with open(name+".info","w") as infofile:
		infodict = {
			"sha":SHA,
			"version":script_version,
			"description":"We moved to sorted by key NE counter/list.",
			"input_description":"Initial NE. Large model only for IT, small models for other languages.",
			"unique_named_entities":len(ne_index),
		}
		infofile.write(json.dumps(infodict, indent=4))

	print("created index successfully...", flush=True)


# create temp index from story id to url
def create_storyid_url_index(name, globstring, lang, debug):
	#Keep a list of the 1m most recent urls seen. LRU - least recently used - urls will be removed after hitting this limit
	# recent_url_cache=LRUCache(1000000)

	# globstring = globstring.replace("\\","")

	all_urls = set()
	all_titles = set()
	ne_index = dict()
	total_ent_count = 0

	# let's use "en_short.index" for the two-character NE representation
	name += "-temp-v"+str(script_version)
	print("Will print to file", name+".index", flush=True)

	# with open(name+".index","w") as out, open(name+"_memes.index","w") as article_meme_file, open(name+".dups","w") as dups:
	with open(name+".index","w") as out:
		# we want in the reverse order, to keep the most recent article
		# we could reverse this ordering
		sorted_files = sorted(glob.glob(globstring), reverse=True)
		if debug:
			print(globstring, sorted_files, flush=True)
		for file in sorted_files:
			print(file,datetime.datetime.now(), flush=True)
			if lang=="auto":
				lang=os.path.dirname(file)[-2:]
				if lang not in [ "ar", "de", "es", "en", "fr", "it", "ko", "pl", "pt", "ru", "tr", "zh" ]:
					print("Last two letters of the directory don't specify a known language code. Please use the -l command line argument to specify the language. Exiting.", flush=True)
					sys.exit()
				if debug: print(lang, flush=True)
			if ".gz" not in file:
				with open(file,"r") as fh:
					for lineno,line in enumerate(fh):
						if debug and lineno>100:
							break

						art=json.loads(line)

						if not "id" in art:
							continue
						if not "url" in art:
							continue

						stories_id = art["id"]
						url=art["url"].strip()

						if any([w in art["url"] for w in blockwords]):
							continue

						# skip URLs that we have seen already
						if url not in all_urls:
							all_urls.add(url)
						else:
							continue

						# if recent_url_cache.get(url)!=None:
						# 	continue
						# recent_url_cache[url]=True

						if not "text" in art:
							continue

						words=text2tokens(art["text"], lang)
						if words==None or len(words)<MIN_ARTICLE_LENGTH:
							continue

						# each indexed article needs a title and we don't admit duplicates
						# if the same titles, then log them for inspection
						if "title" in art:
							title = art["title"].strip()
							# title_memeslist[title].append(memes)
							if title not in all_titles:
								all_titles.add(title)
								# title_url[title] = url
							else:
								continue
								# we looked at the resulting dup files, they contained:
								# i) many exact duplicates
								# ii) many articles not relevant socially/politically
						else:
							title = "null"
							# print("Article without title", url)
							continue

						# index of full NEs
						vec=[] #[0]*len(idx) #len(idx)==26**2
						ent_count=0

						if "spacy" in art and art["spacy"] != None:
							for ent in art["spacy"]:
								text=ent["text"]
								if len(text)<2 and lang not in ["zh", "ko"]:
									continue
								if text in ne_index:
									vec.append(ne_index[text])
								else:
									ne_index[text] = total_ent_count
									vec.append(ne_index[text])
									total_ent_count += 1
								ent_count+=1
							if ent_count>0:
								vec = sorted(collections.Counter(vec).items())
								# compute relative date
								# cur_date = file.split("/")[-1].replace(".json", "").replace(".gz", "")
								cur_date = art['publish_date']
								reladate = int(date_diff(cur_date, START_DATE).days)

								out.write(f"{stories_id}\t{file}\t{lineno}\t{url}\n")
						if "polyglot" in art and art["polyglot"]!=None:
							for ent in art["polyglot"]:
								# the implemention used by xi chen
								for word in ent["text"]:
									if len(word)<2 and lang not in ["zh", "ko"]:
										continue
									if word in ne_index:
										vec.append(ne_index[word])
									else:
										ne_index[word] = total_ent_count
										vec.append(ne_index[word])
										total_ent_count += 1
									ent_count+=1
								# implemention that joins tokenized named entities
								# which seems to correspond to what spacy does
								# however, xi chen's experience is that
								# we get better results without joining
								# text=" ".join(ent["text"])
								# if len(text)<2 and lang not in ["zh", "ko"]:
								# 	continue
								# if text in ne_index:
								# 	vec.append(ne_index[text])
								# else:
								# 	ne_index[text] = total_ent_count
								# 	total_ent_count += 1
								# ent_count+=1
							if ent_count>0:
								vec = sorted(collections.Counter(vec).items())
								# compute relative date
								# cur_date = file.split("/")[-1].replace(".json", "").replace(".gz", "")
								cur_date = art['publish_date']
								reladate = int(date_diff(cur_date, START_DATE).days)
								out.write(f"{stories_id}\t{title}\t{file}\t{lineno}\t{url}\n")
			else:
				with gzip.open(file,"rt") as fh:
					for lineno, line in enumerate(fh):
						if debug and lineno > 100:
							break

						art = json.loads(line)

						if not "id" in art:
							continue
						if not "url" in art:
							continue

						stories_id = art["id"]
						url = art["url"]

						if any([w in art["url"] for w in blockwords]):
							continue

						#there is no "story_text" attribute in wiki data now

						# if not "story_text" in art:
						# 	continue
						#
						# words = text2tokens(art["story_text"], lang)
						# if words == None or len(words) < MIN_ARTICLE_LENGTH:
						# 	continue

						# skip URLs that we have seen already
						if url not in all_urls:
							all_urls.add(url)
						else:
							continue

						if "title" in art:
							title = art["title"].strip()
							# title_memeslist[title].append(memes)
							if title not in all_titles:
								all_titles.add(title)
								# title_url[title] = url
							else:
								continue
								# we looked at the resulting dup files, they contained:
								# i) many exact duplicates
								# ii) many articles not relevant socially/politically
						else:
							title = "null"
							# print("Article without title", url)
							continue

						# index of full NEs
						vec = []  # [0]*len(idx) #len(idx)==26**2
						ent_count = 0

						if "wiki_concepts" in art and art["wiki_concepts"] != None:
							for ent in art["wiki_concepts"]:
								if len(ent['term']) < 2 and lang not in ["zh", "ko"]:
									continue
								if ent['term'] in ne_index:
									vec.append(int(ent['term_id']))
								else:
									ne_index[ent['term']] = ent['term_id']
									vec.append(int(ent['term_id']))
								ent_count += 1

							if ent_count > 0:
								vec = sorted(collections.Counter(vec).items())
								# compute relative date
								# cur_date = file.split("/")[-1].replace(".json", "").replace(".gz", "")
								cur_date = art['publish_date']
								reladate = int(date_diff(cur_date, START_DATE).days)
								out.write(f"{stories_id}\t{title}\t{file}\t{lineno}\t{url}\n")

	with open(name+"_ne.index", "w") as out:
		for k,v in ne_index.items():
			key = k.replace('\t', ' ')
			out.write(f"{key}\t{v}\n")

	with open(name+".info","w") as infofile:
		infodict = {
			"sha":SHA,
			"version":script_version,
			"description":"We moved to sorted by key NE counter/list.",
			"input_description":"Initial NE. Large model only for IT, small models for other languages.",
			"unique_named_entities":len(ne_index),
		}
		infofile.write(json.dumps(infodict, indent=4))

	print("created index successfully...", flush=True)


if __name__ == '__main__':
	
	parser = ArgumentParser()
	parser.add_argument("-s", "--date", dest="start_date",
						required=True, type=str,
						help="The date for to start data processing. Format: YYYY-MM-DD")
	parser.add_argument("-n", "--num_days", dest="num_days",
						required=True, type=int,
						help="Number of days after start date to include in clustering")
	parser.add_argument("-d", "--debug", dest="debug",
		default=False, action='store_true',
		help="Debug mode running for only a small chunk of data.")
	parser.add_argument("--data_dir", dest="data_dir",
						required=True,
						help="Path to folder with the data")
	parser.add_argument("--output_dir", dest="output_dir",
						required=True,
						help="Path to folder to store the indexes")
	parser.add_argument("--min_ne_num", dest="min_ne_num",
						required=True, type=int,
						help="Only keep articles with min_ne_num named entities")
	args = parser.parse_args()

	DATA_DIR = args.data_dir
	start_date_split = args.start_date.split("-")
	start_date_dt = dt.date(int(start_date_split[0]), int(start_date_split[1]), int(start_date_split[2]))
	end_date_dt = start_date_dt + dt.timedelta(days=int(args.num_days))

	globstrings = []
	for i in range(int(args.num_days)):
		globstrings.append(os.path.join(DATA_DIR,  (start_date_dt + dt.timedelta(days=i)).strftime("%Y-%m-%d"), "*", "wikilinked", "en", "*.json")) ##Only working with English articles for now.
	print(globstrings)
	output_filename = os.path.join(args.output_dir, 'indexes', 'indexes')
	if not os.path.exists(os.path.join(args.output_dir, 'indexes')):
		os.makedirs(os.path.join(args.output_dir, 'indexes'))
	# if not args.bias:
	create_index(output_filename, globstrings, "en", args.debug)
	print("Total time taken:", time.time()-start)

	
	## Only keep articles that have >= min_art_ne_num NEs
	read_and_filter_data(index_filename=output_filename+".index", min_art_ne_num=args.min_ne_num)
	
	## Find total number of occurences for NEs and store them in a sorted order. 
	## The top NEs in this list will be dropped later in the pipeline
	sort_indexes_by_count(index_filename=output_filename+ ".index", 
					   output_filename = output_filename+ "_sorted_by_count.index")





