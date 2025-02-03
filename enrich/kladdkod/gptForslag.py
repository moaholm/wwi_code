import requests
from google.cloud import bigquery
import datetime

# Funktion som hämtar data från API
def fetch_data_from_api(last_fetch_time):
    API_URL = "https://example.com/data"  # Ersätt med ditt API:s URL
    params = {"since": last_fetch_time}  # Skicka senaste hämtningstid som parameter
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()

# Funktion som skriver data till BigQuery
def write_to_bigquery(data, table_id):
    client = bigquery.Client()
    job = client.insert_rows_json(table_id, data)  # Lägg till rader i tabellen
    if job:
        print(f"Fel vid insättning: {job}")
    else:
        print("Data har skrivits till BigQuery.")

# Huvudfunktion
def main(request=None):
    # BigQuery-klient och tabell
    client = bigquery.Client()
    table_id = "your_project.your_dataset.your_table"  # Uppdatera med ditt tabell-ID

    # Kontrollera senaste hämtningstid
    query = f"""
        SELECT MAX(last_update_time) as last_fetch_time
        FROM `{table_id}`
    """
    query_job = client.query(query)
    results = query_job.result()
    last_fetch_time = None
    for row in results:
        last_fetch_time = row.last_fetch_time if row.last_fetch_time else "1970-01-01T00:00:00"

    # Hämta nya data
    new_data = fetch_data_from_api(last_fetch_time)

    # Lägg till tidsstämpel för att undvika dubbletter
    for item in new_data:
        item['last_update_time'] = datetime.datetime.utcnow().isoformat()

    # Skriv till BigQuery
    write_to_bigquery(new_data, table_id)
    return "Klar!"

