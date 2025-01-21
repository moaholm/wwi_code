from pytrends.request import TrendReq
from google.cloud import bigquery
import pandas as pd
import os
import time

pytrends = TrendReq()
bq_client = bigquery.Client()

keywords = ["bubble wrap", "packing materials"]

pytrends.build_payload(keywords, cat=0, timeframe='2013-01-01 2016-05-31')
time.sleep(60)
trend_data = pytrends.interest_over_time()

trend_data = trend_data.drop(columns=['isPartial'])
trend_data = trend_data.reset_index()
trend_data = trend_data.melt(id_vars=["date"], var_name="keyword", value_name="trend_score")

table_id = "data-evolution-moa.raw_wwi.google_trends_data"

job = bq_client.load_table_from_dataframe(trend_data, table_id)
job.result()
print(f"Data har laddats upp till {table_id}")