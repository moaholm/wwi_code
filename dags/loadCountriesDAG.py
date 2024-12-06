from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,12,5),
    'retries': 1,
}

dag = DAG(
    'gcs_to_bigquery',
    default_args = default_args,
    schedule_interval = None
)

load_csv = GCSToBigQueryOperator(
    task_id = 'load_countries',
    bucket = 'raw-wwi-moa',
    source_objects = ['data-evolution-wwi/csv/application.countries/Application.Countries.csv'],
    destination_project_dataset_table = 'data-evolution-moa.raw_wwi.countries',
    schema_fields = [
        {'name' : 'countryID', 'type' : 'STRING'},
        {'name' : 'countryName', 'type' : 'STRING'},
        {'name' : 'formalName', 'type' : 'STRING'},
        {'name' : 'isoAlpha3Code', 'type' : 'STRING'},
        {'name' : 'isoNumericCode', 'type' : 'STRING'},
        {'name' : 'countryType', 'type' : 'STRING'},
        {'name' : 'latestRecordedPopulation', 'type' : 'STRING'},
        {'name' : 'continent', 'type' : 'STRING'},
        {'name' : 'region', 'type' : 'STRING'},
        {'name' : 'subregion', 'type' : 'STRING'},
        {'name' : 'lastEditedBy', 'type' : 'STRING'}
    ],
    write_disposition = 'WRITE_TRUNCATE',
    dag = dag
)

load_csv