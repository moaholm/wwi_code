#import functions_framework
import uuid
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
#import logging
#from google.cloud import logging as cloudLogging

@functions_framework.http

def apiFetchAndUpdateBQ(startDate):
    APIkey = "e131017d39a641dfb99150023252101"
    cities = ["Stockholm", "Los Angeles", "Calpulalpan","Palermo", "Nuremberg", "Oulu", "Hat Yai", "Beluran", "Makassar"]
    endDate = datetime.now()
    currentDate = startDate
    dataFrame = pd.DataFrame()

    for city in cities:
        while currentDate <= endDate:
            url = f"http://api.weatherapi.com/v1/history.json?key={APIkey}&q={city}&dt={currentDate}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                sortedData = [
                    {
                        "weatherID": str(uuid.uuid4()),
                        "location": str(data['location']['name']),
                        "region": str(data['location']['region']),
                        "country": str(data['location']['country']),
                        "forecastday": str(data['forecast']['forecastday'][0]['date']),
                        "maxtemp_c": str(data['forecast']['forecastday'][0]['day']['maxtemp_c']),
                        "maxtemp_f": str(data['forecast']['forecastday'][0]['day']['maxtemp_f']),
                        "mintemp_c": str(data['forecast']['forecastday'][0]['day']['mintemp_c']),
                        "mintemp_f": str(data['forecast']['forecastday'][0]['day']['mintemp_f']),
                        "avgtemp_c": str(data['forecast']['forecastday'][0]['day']['avgtemp_c']),
                        "avgtemp_f": str(data['forecast']['forecastday'][0]['day']['avgtemp_f']),
                        "maxwind_kph": str(data['forecast']['forecastday'][0]['day']['maxwind_kph']),
                        "totalprecip_mm": str(data['forecast']['forecastday'][0]['day']['totalprecip_mm']),
                        "totalsnow_cm": str(data['forecast']['forecastday'][0]['day']['totalsnow_cm']),
                        "avgvis_km": str(data['forecast']['forecastday'][0]['day']['avgvis_km']),
                        "avghumidity": str(data['forecast']['forecastday'][0]['day']['avghumidity']),
                        "daily_will_it_rain": str(data['forecast']['forecastday'][0]['day']['daily_will_it_rain']),
                        "daily_chance_of_rain": str(data['forecast']['forecastday'][0]['day']['daily_chance_of_rain']),
                        "daily_will_it_snow": str(data['forecast']['forecastday'][0]['day']['daily_will_it_snow']),
                        "daily_chance_of_snow": str(data['forecast']['forecastday'][0]['day']['daily_chance_of_snow']),
                        "uv": str(data['forecast']['forecastday'][0]['day']['uv']),
                        "sunrise": str(data['forecast']['forecastday'][0]['astro']['sunrise']),
                        "sunset": str(data['forecast']['forecastday'][0]['astro']['sunset']),
                        "moonrise": str(data['forecast']['forecastday'][0]['astro']['moonrise']),
                        "moonset": str(data['forecast']['forecastday'][0]['astro']['moonset']),
                        "moon_phase": str(data['forecast']['forecastday'][0]['astro']['moon_phase']),
                        "moon_illumination": str(data['forecast']['forecastday'][0]['astro']['moon_illumination'])
                    }
                ]
                sortedDF = pd.DataFrame(sortedData)
                dataFrame = pd.concat([dataFrame, sortedDF])
            else: 

                print(f"Something went wrong {response.status_code}")
                return response.status_code
            currentDate += timedelta(days=1)
        currentDate = startDate
    return dataFrame

def queryLastDate():
    bqClient = bigquery.Client()
    query = """
        SELECT forecastday 
        FROM `data-evolution-moa.raw_wwi.weather` 
        order by cast(forecastday as datetime) desc
        LIMIT 1
    """
    queryJob = bqClient.query(query).result()
    firstRow = next(queryJob)
    dateTime = datetime.strptime(firstRow[0], "%Y-%m-%d")
    return dateTime

def jobConfig():
    jobConf = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("weatherID", "STRING"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("forecastday", "STRING"),
            bigquery.SchemaField("maxtemp_c", "STRING"),
            bigquery.SchemaField("maxtemp_f", "STRING"),
            bigquery.SchemaField("mintemp_c", "STRING"),
            bigquery.SchemaField("mintemp_f", "STRING"),
            bigquery.SchemaField("avgtemp_c", "STRING"),
            bigquery.SchemaField("avgtemp_f", "STRING"),
            bigquery.SchemaField("maxwind_kph", "STRING"),
            bigquery.SchemaField("totalprecip_mm", "STRING"),
            bigquery.SchemaField("totalsnow_cm", "STRING"),
            bigquery.SchemaField("avgvis_km", "STRING"),
            bigquery.SchemaField("avghumidity", "STRING"),
            bigquery.SchemaField("daily_will_it_rain", "STRING"),
            bigquery.SchemaField("daily_chance_of_rain", "STRING"),
            bigquery.SchemaField("daily_will_it_snow", "STRING"),
            bigquery.SchemaField("daily_chance_of_snow", "STRING"),
            bigquery.SchemaField("uv", "STRING"),
            bigquery.SchemaField("sunrise", "STRING"),
            bigquery.SchemaField("sunset", "STRING"),
            bigquery.SchemaField("moonrise", "STRING"),
            bigquery.SchemaField("moonset", "STRING"),
            bigquery.SchemaField("moon_phase", "STRING"),
            bigquery.SchemaField("moon_illumination", "STRING"),
        ],
        write_disposition="WRITE_APPEND"  # Behåll befintliga data
    )
    return jobConf

def main():
    #logClient = cloudLogging.Client()
    #logName = "weatherLogg"
    #logger = logClient.logger(logName)

    lastUpdateDate = queryLastDate()

    if lastUpdateDate.strftime("%Y-%m-%d") < datetime.today().strftime("%Y-%m-%d"):
        fetchDate = lastUpdateDate + timedelta(1)
        newWeatherDF = apiFetchAndUpdateBQ(fetchDate)
        bqClient = bigquery.Client()
        job = bqClient.load_table_from_dataframe(newWeatherDF,'data-evolution-moa.raw_wwi.weather', job_config=jobConfig())
        job.result()
        return "Weather is now up to date\n"
    else:
        return "Weather is already up to date\n"

main()