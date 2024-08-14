import os
from datetime import date, timedelta
import datetime as dt

collection_names = ['mostly_left',
                    'somewhat_left',
                    'center',
                    'somewhat_right',
                    'mostly_right',]
                    #'us_national']

yesterday = date.today() - timedelta(days=1)
# yesterday = dt.date(2024, 7, 23) - timedelta(days=1)
num_days_to_collect = 7
start_dates = []
curr_date_dt = yesterday
for i in range(num_days_to_collect):
    curr_date_str = curr_date_dt.strftime('%Y-%m-%d')
    start_dates.append(curr_date_str)
    curr_date_dt = curr_date_dt - timedelta(days=1)

print(start_dates)
# start_dates = ['2024-06-29', '2024-06-30', '2024-07-01', '2024-07-02', '2024-07-03', '2024-07-04', '2024-07-05']
# output_dir = '/work/pi_pgrabowicz_umass_edu/cbagchi/media_cloud_election_dashboard/data/mc_data_day_by_day'

## use_query:
## 0 to get all news articles and store it to mc_data_day_by_day_all_news
## 1 to get political news articles based on Emily's query and store it to mc_data_day_by_day (full text is not collected for this data)
use_query = 1 

for start_date in start_dates:
    for collection_name in collection_names:
        if use_query:
            os.system(f'sbatch mc_data_collection_day_by_day.sh {start_date} {collection_name} {use_query}')