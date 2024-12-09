from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,12,6),
    'retries': 1,
}

with DAG(
    dag_id='load_stock_items_to_bg',
    default_args= default_args,
    schedule_interval= None
) as dag:
    
    gcs_file_path = 'data-evolution-wwi/csv/warehouse.stockitem/year=2013/month=99/warehouse.stockitem_201399.csv'

    def extract_year_month(file_path):
        parts = file_path.split('/')
        year = parts[-3].split('=')[1]
        month = parts[-2].split('=')[1]
        return year, month

    def extract_year_month_task(**context):
        year, month = extract_year_month(gcs_file_path)
        context['ti'].xcom_push(key='year', value=year)
        context['ti'].xcom_push(key='month', value = month)

    extract_year_month_operator = PythonOperator(
        task_id = 'extract_year_month',
        python_callable= extract_year_month_task,
        provide_context = True
    )

    load_to_temp_table_stockitems = GCSToBigQueryOperator(
        task_id = 'load_to_temp_table_stockitems',
        bucket = 'raw-wwi-moa',
        source_objects = [gcs_file_path],
        destination_project_dataset_table = 'data-evolution-moa.raw_wwi.temp_warehouse_stockitems',
        schema_fields = [
            {'name' : 'stockItemID', 'type' : 'STRING'},
            {'name' : 'stockItemName', 'type' : 'STRING'},
            {'name' : 'supplierID', 'type' : 'STRING'},
            {'name' : 'colorID', 'type' : 'STRING'},
            {'name' : 'unitPackageID', 'type' : 'STRING'},
            {'name' : 'outerPackageID', 'type' : 'STRING'},
            {'name' : 'brand', 'type' : 'STRING'},
            {'name' : 'size', 'type' : 'STRING'},
            {'name' : 'leadTimeDays', 'type' : 'STRING'},
            {'name' : 'quantityPerOuter', 'type' : 'STRING'},
            {'name' : 'isChillerStock', 'type' : 'STRING'},
            {'name' : 'barcode', 'type' : 'STRING'},
            {'name' : 'taxRate', 'type' : 'STRING'},
            {'name' : 'unitPrice', 'type' : 'STRING'},
            {'name' : 'recommendedRetailPrice', 'type' : 'STRING'},
            {'name' : 'typicalWeightPerUnit', 'type' : 'STRING'},
            {'name' : 'marketingComments', 'type' : 'STRING'},
            {'name' : 'internalComments', 'type' : 'STRING'},
            {'name' : 'photo', 'type' : 'STRING'},
            {'name' : 'customFields', 'type' : 'STRING'},
            {'name' : 'tags', 'type' : 'STRING'},
            {'name' : 'searchDetails', 'type' : 'STRING'},
            {'name' : 'lastEditedBy', 'type' : 'STRING'},
            {'name' : 'validFrom', 'type' : 'STRING'},
            {'name' : 'validTo', 'type' : 'STRING'}],
        source_format = 'CSV',
        skip_leading_rows = 1,
        write_disposition = 'WRITE_TRUNCATE',
        autodetect = False
    )

    year_placeholder = '{{ ti.xcom_pull(task_ids="extract_year_month", key="year")}}'
    month_placeholder = '{{ ti.xcom_pull(task_ids="extract_year_month", key="month")}}'

    merge_stockitems_to_final_table = BigQueryInsertJobOperator(
        task_id = 'merge_stockitems_to_final_table',
        configuration = {
            "query": {
                "query": f"""
                    INSERT INTO `data-evolution-moa.raw_wwi.stockItem` (
                        stockItemID ,stockItemName, supplierID, colorID, unitPackageID, outerPackageID, brand, size, leadTimeDays, quantityPerOuter, isChillerStock, barcode, taxRate, unitPrice, recommendedRetailPrice, typicalWeightPerUnit, marketingComments, internalComments, photo, customFields, tags, searchDetails, lastEditedBy, validFrom, validTo, year, month
                    )
                    SELECT
                        stockItemID ,stockItemName, supplierID, colorID, unitPackageID, outerPackageID, brand, size, leadTimeDays, quantityPerOuter, isChillerStock, barcode, taxRate, unitPrice, recommendedRetailPrice, typicalWeightPerUnit, marketingComments, internalComments, photo, customFields, tags, searchDetails, lastEditedBy, validFrom, validTo,
                        '{year_placeholder}' AS year,
                        '{month_placeholder}' AS month
                    FROM `data-evolution-moa.raw_wwi.temp_warehouse_stockitems`
                """,
                "useLegacySql": False,
            }
        },
    )

    extract_year_month_operator >> load_to_temp_table_stockitems >> merge_stockitems_to_final_table