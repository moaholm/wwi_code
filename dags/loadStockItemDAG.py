from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,12,6),
    'retries': 1,
}

dag = DAG(
    'gcs_to_bigquery',
    default_args = default_args,
    schedule_interval = None
)

load_csv = GCSToBigQueryOperator(
    task_id = 'load_stock_groups',
    bucket = 'raw-wwi-moa',
    source_objects = ['data-evolution-wwi/csv/warehouse.stockgroups/year=2013/month=99/warehouse.stockgroups_201399.csv'],
    destination_project_dataset_table = 'data-evolution-moa.raw_wwi.stockGroups',
    schema_fields = [
        {'name' : 'stockItemID', 'type' : 'STRING'},
        {'name' : 'stockItemName', 'type' : 'STRING'},
        {'name' : 'supplierID', 'type' : 'STRING'},
        {'name' : 'colorID', 'type' : 'STRING'},
        {'name' : 'unitPackageID', 'type' : 'STRING'}
        {'name' : 'outerPackageID', 'type' : 'STRING'}
        {'name' : 'brand', 'type' : 'STRING'}
        {'name' : 'size', 'type' : 'STRING'}
        {'name' : 'leadTimeDays', 'type' : 'STRING'}
        {'name' : 'quantityPerOuter', 'type' : 'STRING'}
        {'name' : 'isChillerStock', 'type' : 'STRING'}
        {'name' : 'barcode', 'type' : 'STRING'}
        {'name' : 'taxRate', 'type' : 'STRING'}
        {'name' : 'unitPrice', 'type' : 'STRING'}
        {'name' : 'unitPrice', 'type' : 'STRING'}
    ],
    write_disposition = 'WRITE_TRUNCATE',
    dag = dag
)

load_csv

    recommendedRetailPrice STRING,
    typicalWeightPerUnit STRING,
    marketingComments STRING,
    internalComments STRING,
    photo STRING,
    customFields STRING,
    tags STRING,
    searchDetails STRING,
    lastEditedBy STRING,
    validFrom STRING,
    validTo STRING,
    year STRING,
    month STRING