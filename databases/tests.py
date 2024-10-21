# from django.test import TestCase

# # Create your tests here.


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.transfers.wasb_to_local import WasbToLocalOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'adls_to_databricks_pipeline',
    default_args=default_args,
    description='Fetch data from ADLS and ingest into Databricks',
    schedule_interval='@daily',
    catchup=False,
    start_date=days_ago(1),
)

fetch_data_from_adls = WasbToLocalOperator(
    task_id='fetch_data_from_adls',
    container_name='my-container',
    blob_name='path',
    file_path='file path',
    dag=dag,
)

def check_pipeline_exists(**kwargs):
    http_hook = HttpHook(http_conn_id='databricks_default', method='GET')
    pipeline_id = 'your-pipeline-id'
    endpoint = f"/api/2.0/pipelines/{pipeline_id}"
    response = http_hook.run(endpoint)
    if response.status_code == 200:
        return True
    return False

check_pipeline = PythonOperator(
    task_id='check_pipeline',
    python_callable=check_pipeline_exists,
    dag=dag,
)

def create_databricks_pipeline(**kwargs):
    if not kwargs['ti'].xcom_pull(task_ids='check_pipeline'):
        http_hook = HttpHook(http_conn_id='databricks_default', method='POST')
        endpoint = "/api/2.0/pipelines"
        payload = {
            "name": "my-pipeline",
            "libraries": [{"jar": "dbfs:/my/jar/path.jar"}],
            "clusters": [{"num_workers": 2, "spark_version": "6.4.x-scala2.11"}],
            "configuration": {},
            "continuous": True
        }
        response = http_hook.run(endpoint, json=payload)
        if response.status_code == 200:
            return True
    return False

create_pipeline = PythonOperator(
    task_id='create_pipeline',
    python_callable=create_databricks_pipeline,
    provide_context=True,
    dag=dag,
)

def trigger_pipeline(**kwargs):
    http_hook = HttpHook(http_conn_id='databricks_default', method='POST')
    pipeline_id = 'your-pipeline-id'
    endpoint = f"/api/2.0/pipelines/{pipeline_id}/trigger"
    response = http_hook.run(endpoint)
    if response.status_code == 200:
        return True
    return False

trigger_pipeline = PythonOperator(
    task_id='trigger_pipeline',
    python_callable=trigger_pipeline,
    provide_context=True,
    dag=dag,
)

def ingest_data_into_databricks(**kwargs):
    http_hook = HttpHook(http_conn_id='databricks_default', method='POST')
    pipeline_id = 'your-pipeline-id'
    endpoint = f"/api/2.0/pipelines/{pipeline_id}/runs"
    response = http_hook.run(endpoint)
    if response.status_code == 200:
        return True
    return False

ingest_data = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data_into_databricks,
    provide_context=True,
    dag=dag,
)

fetch_data_from_adls >> check_pipeline
check_pipeline >> create_pipeline >> trigger_pipeline
check_pipeline >> trigger_pipeline
trigger_pipeline >> ingest_data
