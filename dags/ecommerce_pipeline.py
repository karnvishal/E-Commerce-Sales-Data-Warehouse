from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_data(**kwargs):
    from data_generator import generate_daily_data
    execution_date = kwargs['execution_date']
    generate_daily_data(execution_date.date())

def upload_to_gcs(**kwargs):
    from upload_to_gcs import upload_daily_data
    execution_date = kwargs['execution_date']
    upload_daily_data(execution_date.date())

with DAG(
    'ecommerce_pipeline',
    default_args=default_args,
    description='E-commerce ELT Pipeline',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    generate_data_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
        provide_context=True,
    )
    
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True,
        on_failure_callback=lambda context: print("Upload failed!"),
    )
    
    load_orders_task = GCSToBigQueryOperator(
        task_id='load_orders_to_bq',
        bucket='ecommerce-dw-raw',
        source_objects=['orders/{{ ds }}/orders.csv'],
        destination_project_dataset_table='ecommerce-e2e-458713.ecommerce_raw.orders',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        autodetect=True,
        skip_leading_rows=1,
        time_partitioning={'type': 'DAY', 'field': 'order_date'},
    )
    
    load_order_items_task = GCSToBigQueryOperator(
        task_id='load_order_items_to_bq',
        bucket='ecommerce-dw-raw',
        source_objects=['order_items/{{ ds }}/order_items.csv'],
        destination_project_dataset_table='ecommerce-e2e-458713.ecommerce_raw.order_items',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        autodetect=True,
        skip_leading_rows=1,
    )
    
    load_inventory_task = GCSToBigQueryOperator(
        task_id='load_inventory_to_bq',
        bucket='ecommerce-dw-raw',
        source_objects=['inventory/{{ ds }}/inventory_movements.csv'],
        destination_project_dataset_table='ecommerce-e2e-458713.ecommerce_raw.inventory_movements',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        autodetect=True,
        skip_leading_rows=1,
    )

    load_customers_task = GCSToBigQueryOperator(
    task_id='load_customers_to_bq',
    bucket='ecommerce-dw-raw',
    source_objects=['reference/customers.csv'],
    destination_project_dataset_table='ecommerce-e2e-458713.ecommerce_raw.customers',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',  
    autodetect=True,
    skip_leading_rows=1,
    )

    load_products_task = GCSToBigQueryOperator(
    task_id='load_products_to_bq',
    bucket='ecommerce-dw-raw',
    source_objects=['reference/products.csv'],
    destination_project_dataset_table='ecommerce-e2e-458713.ecommerce_raw.products',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',  
    autodetect=True,
    skip_leading_rows=1,
    )

    
    dbt_run_task = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/ecommerce_dbt && /home/airflow/.local/bin/dbt run',
    env={
        'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/ecommerce-e2e-458713-0ee65e15ab5c.json', 
        'DBT_PROFILES_DIR': '/opt/airflow/ecommerce_dbt',
        'PATH': '/home/airflow/.local/bin:/opt/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin'
    }
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/ecommerce_dbt && /home/airflow/.local/bin/dbt test',
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/ecommerce-e2e-458713-0ee65e15ab5c.json', 
            'DBT_PROFILES_DIR': '/opt/airflow/ecommerce_dbt',
            'PATH': '/home/airflow/.local/bin:/opt/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin'
        }
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> generate_data_task >> upload_to_gcs_task
    upload_to_gcs_task >> [load_orders_task, load_order_items_task, load_inventory_task]
    [load_orders_task, load_order_items_task, load_inventory_task] >>  dbt_run_task
    dbt_run_task >> dbt_test_task >> end