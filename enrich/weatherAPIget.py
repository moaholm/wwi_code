import requests
import pandas as pd
from datetime import datetime, timedelta

APIkey = "e131017d39a641dfb99150023252101"
cities = ["Stockholm", "Los Angeles", "Calpulalpan","Palermo", "Nuremberg", "Oulu", "Hat Yai", "Beluran", "Makassar"]
response_file = 'weatherOutput.csv'


end_date = datetime.now()
start_date = datetime(2024, 2, 1)
current_date = start_date

for city in cities:
    while current_date <= end_date:
        url = f"http://api.weatherapi.com/v1/history.json?key={APIkey}&q={city}&dt={current_date}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            sorted_data = [
                {
                    "location": data['location']['name'],
                    "region": data['location']['region'],
                    "country": data['location']['country'],
                    "forecastday": data['forecast']['forecastday'][0]['date'],
                    "maxtemp_c": data['forecast']['forecastday'][0]['day']['maxtemp_c'],
                    "maxtemp_f": data['forecast']['forecastday'][0]['day']['maxtemp_f'],
                    "mintemp_c": data['forecast']['forecastday'][0]['day']['mintemp_c'],
                    "mintemp_f": data['forecast']['forecastday'][0]['day']['mintemp_f'],
                    "avgtemp_c": data['forecast']['forecastday'][0]['day']['avgtemp_c'],
                    "avgtemp_f": data['forecast']['forecastday'][0]['day']['avgtemp_f'],
                    "maxwind_kph": data['forecast']['forecastday'][0]['day']['maxwind_kph'],
                    "totalprecip_mm": data['forecast']['forecastday'][0]['day']['totalprecip_mm'],
                    "totalsnow_cm": data['forecast']['forecastday'][0]['day']['totalsnow_cm'],
                    "avgvis_km": data['forecast']['forecastday'][0]['day']['avgvis_km'],
                    "avghumidity": data['forecast']['forecastday'][0]['day']['avghumidity'],
                    "daily_will_it_rain": data['forecast']['forecastday'][0]['day']['daily_will_it_rain'],
                    "daily_chance_of_rain": data['forecast']['forecastday'][0]['day']['daily_chance_of_rain'],
                    "daily_will_it_snow": data['forecast']['forecastday'][0]['day']['daily_will_it_snow'],
                    "daily_chance_of_snow": data['forecast']['forecastday'][0]['day']['daily_chance_of_snow'],
                    "uv": data['forecast']['forecastday'][0]['day']['uv'],
                    "sunrise": data['forecast']['forecastday'][0]['astro']['sunrise'],
                    "sunset": data['forecast']['forecastday'][0]['astro']['sunset'],
                    "moonrise": data['forecast']['forecastday'][0]['astro']['moonrise'],
                    "moonset": data['forecast']['forecastday'][0]['astro']['moonset'],
                    "moon_phase": data['forecast']['forecastday'][0]['astro']['moon_phase'],
                    "moon_illumination": data['forecast']['forecastday'][0]['astro']['moon_illumination']
                }
            ]
            dataFrame = pd.DataFrame(sorted_data)
            dataFrame.to_csv(response_file, mode= 'a', sep= ';', index= False, header= False)
        else: 

            print(f"Something went wrong {response.status_code}")
        current_date += timedelta(days=1)
    current_date = start_date
