import uuid
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

def apiFetch(startDate):
    APIkey = "e131017d39a641dfb99150023252101"
    #cities = ["Stockholm", "Los Angeles", "Calpulalpan","Palermo", "Nuremberg", "Oulu", "Hat Yai", "Beluran", "Makassar"]
    cities = ["Oulu", "Hat Yai", "Beluran", "Makassar"]
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

def main(): 
    tableID = 'data-evolution-moa.raw_wwi.weather'
    bqClient = bigquery.Client()
    apiResult = apiFetch(startDate=datetime(2024,2,5))
    job = bqClient.load_table_from_dataframe(apiResult, tableID)
    job.result()

    print(f"Data har laddats upp")

main()