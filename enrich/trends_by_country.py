from pytrends.request import TrendReq
from google.cloud import bigquery
import pandas as pd
import os
import time

bq_client = bigquery.Client()
trends = TrendReq()

keywords = ["bubble wrap", "packing materials"]
countryList = ['US', 'MX', 'DE', 'IT', 'FI', 'MY', 'ID']

output_file ='output_file.csv'

trends.build_payload(keywords, cat=0, timeframe='2013-01-01 2016-05-31', geo='US')
time.sleep(60)
trendsData = trends.interest_over_time()
trendsData = trendsData.drop(columns=['isPartial'])
trendsData = trendsData.reset_index()
trendsData = trendsData.melt(id_vars=["date"], var_name="keyword", value_name="trend_score")
trendsData["country"] = 'US'

trendsData.to_csv(output_file, mode='a', index=False, header=not pd.io.common.file_exists(output_file))
# table_id = "data-evolution-moa.raw_wwi.googleTrendsData"
# job = bq_client.load_table_from_dataframe(dataBlob, table_id)
# job.result()
print(f"Data har laddats upp till")
