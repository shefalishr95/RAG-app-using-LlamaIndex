from airflow import DAG
from airflow.decorators import task, dag
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import os
import sys
import openai

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils import Config

from utils import (
    check_pgvector_extension,
    create_tables_if_not_exist,
    list_new_files,
    download_files,
    generate_embeddings_and_store,
    update_processed_files_table,
)

config = Config()
print("Loading config var from Config class now")

try:
    openai.api_key = os.environ["OPENAI_API_KEY"].strip()
except:
    print("Error loading OpenAI API key")


processed_files_bucket_name = config["gcs"]["processed_files_upload_bucket"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="generate_and_load_embeddings",
    default_args=default_args,
    description="Generate embeddings from README files and load into PostgreSQL",
    schedule_interval=timedelta(days=30),
    start_date=datetime(2024, 8, 3),
    catchup=False,
)
def generate_and_load_embeddings():
    @task(task_id="check_if_vector_ext_installed")
    def check_and_setup_database():
        if not check_pgvector_extension():
            raise AirflowException(
                "pgvector extension is not installed in the database"
            )
        create_tables_if_not_exist()

    @task(task_id="list_new_files_to_download")
    def list_new_files_task():
        return list_new_files(processed_files_bucket_name=processed_files_bucket_name)

    @task(task_id="download_new_files")
    def download_files_task(new_files):
        downloaded_files = download_files(
            processed_files_bucket_name=processed_files_bucket_name,
            new_files=new_files,
        )
        return {"downloaded_files": downloaded_files}

    @task(task_id="generate_embeddings_amd_store")
    def generate_embeddings_and_store_task(download_result):
        downloaded_files = download_result["downloaded_files"]
        success = generate_embeddings_and_store(downloaded_files)
        if success:
            print("Vector index successfully created!")
        else:
            print("Failed to create vector index.")
        return success

    @task(task_id="process_embedded_files")
    def process_embedded_files_task():
        print("Updating processed_files table")
        update_processed_files_table()

    check_db = check_and_setup_database()
    new_files = list_new_files_task()
    download_result = download_files_task(new_files)
    embedding_success = generate_embeddings_and_store_task(download_result)
    process_task = process_embedded_files_task()

    check_db >> new_files >> download_result >> embedding_success >> process_task


generate_embeddings_dag = generate_and_load_embeddings()
