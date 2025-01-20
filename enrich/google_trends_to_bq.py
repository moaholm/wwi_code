from pytrends.request import TrendReq
from google.cloud import bigquery
import pandas as pd
import os

# Initiera Pytrends och BigQuery-klient
pytrends = TrendReq(hl='en-US', tz=360)
bq_client = bigquery.Client()

# Ange dina sökord (kan hämtas dynamiskt från din data)
keywords = ["bubblewrap", "packing materials"]

# Bygg payload för Google Trends
pytrends.build_payload(keywords, cat=0, timeframe='2013-01-01 2016-05-31')

# Hämta intresse över tid
trend_data = pytrends.interest_over_time()

# Ta bort kolumnen "isPartial" som inte behövs
trend_data = trend_data.drop(columns=['isPartial'])

# Lägg till sökord som kolumn
trend_data = trend_data.reset_index()
trend_data = trend_data.melt(id_vars=["date"], var_name="keyword", value_name="trend_score")

# Konvertera Pandas DataFrame till BigQuery-format
table_id = "ditt-projekt-id.ditt-dataset-id.google_trends_data"

# Ladda upp data till BigQuery
job = bq_client.load_table_from_dataframe(trend_data, table_id)
job.result()  # Vänta tills laddningen är klar
print(f"Data har laddats upp till {table_id}")
