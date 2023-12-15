#Upload generated weather_master to GBQ
pip install pandas_gbq

import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import bigquery

client = bigquery.Client()

#Local authentication json file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'weatherlink-404323-9261c60d889c.json'

# Specify GBQ params
project_id = 'weatherlink-404323'
dataset_id = 'weatherlink_master'
table_id = 'weather_master'

# weather_master file in same directory
csv_file_path = 'weather_master.csv'

# Read CSV file into a dataframe
df = pd.read_csv(csv_file_path)

# Upload the dataframe to BigQuery
to_gbq(df, f"{dataset_id}.{table_id}", project_id=project_id, if_exists='replace')
