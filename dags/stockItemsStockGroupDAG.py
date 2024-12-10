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
    dag_id='load_stock_items_stock_group_to_bg',
    default_args= default_args,
    schedule_interval= None
) as dag:
    
    gcs_file_path = 'data-evolution-wwi/csv/warehouse.stockitemstockgroups/year=2013/month=99/warehouse.stockitemstockgroups_201399.csv'

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

    load_to_temp_table_stockitems_stock_group = GCSToBigQueryOperator(
        task_id = 'load_to_temp_table_stockitems_stock_group',
        bucket = 'raw-wwi-moa',
        source_objects = [gcs_file_path],
        destination_project_dataset_table = 'data-evolution-moa.raw_wwi.temp_warehouse_stockitems_stock_group',
        schema_fields = [
            {'name' : 'stockItemID', 'type' : 'STRING'},
            {'name' : 'stockItemStockGroupID', 'type' : 'STRING'},
            {'name' : 'stockGroupID', 'type' : 'STRING'},
            {'name' : 'lastEditedBy', 'type' : 'STRING'},
            {'name' : 'lastEditedWhen', 'type' : 'STRING'}],
        source_format = 'CSV',
        skip_leading_rows = 1,
        write_disposition = 'WRITE_TRUNCATE',
        autodetect = False
    )

    year_placeholder = '{{ ti.xcom_pull(task_ids="extract_year_month", key="year")}}'
    month_placeholder = '{{ ti.xcom_pull(task_ids="extract_year_month", key="month")}}'

    merge_stockitems_to_final_table = BigQueryInsertJobOperator(
        task_id = 'merge_stockitems_stock_group_to_final_table',
        configuration = {
            "query": {
                "query": f"""
                    INSERT INTO `data-evolution-moa.raw_wwi.stockItemStockGroup` (
                        stockItemID, stockItemStockGroupID, stockGroupID, lastEditedBy, lastEditedWhen, year, month
                    )
                    SELECT
                        stockItemID, stockItemStockGroupID, stockGroupID, lastEditedBy, lastEditedWhen,
                        '{year_placeholder}' AS year,
                        '{month_placeholder}' AS month
                    FROM `data-evolution-moa.raw_wwi.temp_warehouse_stockitems_stock_group`
                """,
                "useLegacySql": False,
            }
        },
    )

    extract_year_month_operator >> load_to_temp_table_stockitems_stock_group >> merge_stockitems_to_final_table