import trafilatura.metadata
import requests
import json
# URL = "https://www.bfmtv.com/cote-d-azur/nice-25-personnes-expulsees-lors-d-operations-anti-squat-menees-dans-le-quartier-des-liserons_AN-202312150639.html"
# URL = "https://apnews.com/article/biden-drops-out-2024-election-ddffde72838370032bdcff946cfc2ce6"
# html = requests.get(URL).content
# doc = trafilatura.metadata.extract_metadata(html)

with open('/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day/2024-01-01/center/en/data.json', 'r') as f:
    for line in f:
        data = json.loads(line)
        URL = data['url']
        html = requests.get(URL).content
        doc = trafilatura.metadata.extract_metadata(html)
        try:
            top_image_url = doc.image
            print(top_image_url)
        except:
            print("Image not found")
