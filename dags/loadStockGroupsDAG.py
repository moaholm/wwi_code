from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,12,5),
    'retries': 1,
}

dag = DAG(
    'load_stock_groups',
    default_args = default_args,
    schedule_interval = None
)

load_csv = GCSToBigQueryOperator(
    task_id = 'load_stock_groups',
    bucket = 'raw-wwi-moa',
    source_objects = [
        'data-evolution-wwi/csv/warehouse.stockgroups/year=2013/month=99/warehouse.stockgroups_201399.csv'
    ],
    destination_project_dataset_table = 'data-evolution-moa.raw_wwi.stockGroups',
    schema_fields = [
        {'name' : 'stockGroupID', 'type' : 'STRING'},
        {'name' : 'stockGroupName', 'type' : 'STRING'},
        {'name' : 'lastEditedBy', 'type' : 'STRING'},
        {'name' : 'year', 'type' : 'STRING'},
        {'name' : 'month', 'type' : 'STRING'}
    ],
    write_disposition = 'WRITE_TRUNCATE',
    dag = dag
)
load_csv