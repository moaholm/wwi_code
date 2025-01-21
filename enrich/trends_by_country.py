from pytrends.request import TrendReq
from google.cloud import bigquery
import pandas as pd
import os
import time

trends = TrendReq()

keywords = ["bubble wrap", "packing materials"]
countryList = ["US", "MX", "DE", "IT", "FI", "MY", "ID"]

trends.build_payload(keywords, cat=0, timeframe='2013-01-01 2013-01-05', geo='US')
time.sleep(60)
trendsData = trends.interest_over_time()
trendsData = trendsData.drop(columns=['isPartial'])
trendsData = trendsData.reset_index()
trendsData = trendsData.melt(id_vars=["date"], var_name="keyword", value_name="trend_score")
print(trendsData.head())

# moa.lilja@MacBook-Pro-som-tillhor-Moa enrich (main) $ python3 trends_by_country.py 
#         date      keyword  trend_score
# 0 2013-01-01  bubble wrap           81
# 1 2013-01-02  bubble wrap           84
# 2 2013-01-03  bubble wrap          100
# 3 2013-01-04  bubble wrap           99
# 4 2013-01-05  bubble wrap           81
# moa.lilja@MacBook-Pro-som-tillhor-Moa enrich (main) $ 
