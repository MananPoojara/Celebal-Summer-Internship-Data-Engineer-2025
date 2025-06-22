from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
import os
import shutil
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


# Credentials 
load_dotenv()

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_ACCOUNT_KEY")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT")
RDS_DB = os.getenv("RDS_DB")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")


def create_spark_session():
    return SparkSession.builder \
        .appName("RDS_to_DataLake") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4,org.apache.spark:spark-avro_2.12:3.4.1") \
        .getOrCreate()

def extract(**kwargs):
    spark = create_spark_session()
    selected_tables = ["orders"]  # Selected tables

    tmp_base_dir = os.path.join(os.getcwd(), "data", "tmp_extract")
    os.makedirs(tmp_base_dir, exist_ok=True)

    for table in selected_tables:
        df = spark.read.jdbc(
            url=f"jdbc:postgresql://{RDS_HOST}:{RDS_PORT}/{RDS_DB}",
            table=table,
            properties={
                "user": RDS_USER,
                "password": RDS_PASSWORD,
                "driver": "org.postgresql.Driver"
            }
        )
        df.write.parquet(os.path.join(tmp_base_dir, f"{table}.parquet"), mode="overwrite")
        kwargs['ti'].xcom_push(key=f'{table}_extract_path', value=os.path.join(tmp_base_dir, f"{table}.parquet"))

def transform(**kwargs):
    spark = create_spark_session()
    selected_tables = ["orders"]

    output_base = os.path.join(os.getcwd(), "data", "transformed")
    os.makedirs(output_base, exist_ok=True)

    for table in selected_tables:
        extract_path = kwargs['ti'].xcom_pull(key=f'{table}_extract_path', task_ids='extract')
        df = spark.read.parquet(extract_path)
        df_filtered = df.filter("quantity > 5")

        table_dir = os.path.join(output_base, table)
        df_filtered.write.csv(os.path.join(table_dir, "csv"), header=True, mode="overwrite")
        df_filtered.write.parquet(os.path.join(table_dir, "parquet"), mode="overwrite")
        df_filtered.write.format("avro").save(os.path.join(table_dir, "avro"), mode="overwrite")

        kwargs['ti'].xcom_push(key=f'{table}_transformed_path', value=table_dir)

def load(**kwargs):
    selected_tables = ["orders"]
    connect_str = f"DefaultEndpointsProtocol=https;AccountName={AZURE_STORAGE_ACCOUNT_NAME};AccountKey={AZURE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    for table in selected_tables:
        transformed_path = kwargs['ti'].xcom_pull(key=f'{table}_transformed_path', task_ids='transform')
        for root, _, files in os.walk(transformed_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                rel_path = os.path.relpath(local_file_path, transformed_path)
                blob_path = f"{table}/{rel_path.replace(os.sep, '/')}"
                
                blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=blob_path)
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print(f"Uploaded {local_file_path} to {blob_path}")

def cleanup(**kwargs):
    base_dir = os.path.join(os.getcwd(), "data")
    for folder in ["tmp_extract", "transformed"]:
        dir_path = os.path.join(base_dir, folder)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            print(f"Deleted {dir_path}")

default_args = {
    'owner': 'dexter',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='rds_to_datalake_full',
    default_args=default_args,
    description='Extract RDS → Transform → Load to Azure Data Lake → Cleanup',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load_to_datalake',
        python_callable=load,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_local',
        python_callable=cleanup,
    )

    extract_task >> transform_task >> load_task >> cleanup_task
