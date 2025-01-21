from pytrends.request import TrendReq
import time

pytrends = TrendReq()
keywords = ["bubblewrap", "packing materials"]

pytrends.build_payload(keywords, cat=0, timeframe='2013-01-01 2013-01-05')
time.sleep(60)
trend_data = pytrends.interest_over_time()

print(trend_data.head())
print(f"Antal rader: {trend_data.shape[0]}")
print(f"Antal kolumner: {trend_data.shape[1]}")