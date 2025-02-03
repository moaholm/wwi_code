from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025,1,26),
    'retries': 1,
}

dag = DAG(
    'load_weather',
    default_args = default_args,
    schedule_interval = None
)

load_csv = GCSToBigQueryOperator(
    task_id = 'load_weather',
    bucket = 'raw-wwi-moa',
    source_objects = ['data-evolution-wwi/csv/weatherOutput2.csv'],
    destination_project_dataset_table = 'data-evolution-moa.raw_wwi.weather',
    schema_fields = [
        {"name": "weatherID", "type": "STRING"},
        {"name": "location", "type": "STRING"},
        {"name": "region", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"name": "forecastday", "type": "STRING"},
        {"name": "maxtemp_c", "type": "STRING"},
        {"name": "maxtemp_f", "type": "STRING"},
        {"name": "mintemp_c", "type": "STRING"},
        {"name": "mintemp_f", "type": "STRING"},
        {"name": "avgtemp_c", "type": "STRING"},
        {"name": "avgtemp_f", "type": "STRING"},
        {"name": "maxwind_kph", "type": "STRING"},
        {"name": "totalprecip_mm", "type": "STRING"},
        {"name": "totalsnow_cm", "type": "STRING"},
        {"name": "avgvis_km", "type": "STRING"},
        {"name": "avghumidity", "type": "STRING"},
        {"name": "daily_will_it_rain", "type": "STRING"},
        {"name": "daily_chance_of_rain", "type": "STRING"},
        {"name": "daily_will_it_snow", "type": "STRING"},
        {"name": "daily_chance_of_snow", "type": "STRING"},
        {"name": "uv", "type": "STRING"},
        {"name": "sunrise", "type": "STRING"},
        {"name": "sunset", "type": "STRING"},
        {"name": "moonrise", "type": "STRING"},
        {"name": "moonset", "type": "STRING"},
        {"name": "moon_phase", "type": "STRING"},
        {"name": "moon_illumination", "type": "STRING"}
        ],
    write_disposition = 'WRITE_TRUNCATE',
    dag = dag
)

load_csv