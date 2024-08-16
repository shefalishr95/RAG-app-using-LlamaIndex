from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from typing import List
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils import Config, execute

print("Loaded Config, execute from utils")

config = Config()
print("Loading config var from Config class now")

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="github_data_scraping_etl",
    default_args=default_args,
    description="DAG to scrape and process GitHub data",
    start_date=datetime(2024, 8, 3),
    schedule_interval="@weekly",
    catchup=False,
)
def github_data_scraping_etl():

    @task(task_id="get_topics")
    def get_topics() -> List[str]:
        return config["git-scraping"]["topics"]

    @task(task_id="scrape_topic", multiple_outputs=True)
    def scrape_topic(topic):
        try:
            n = config["git-scraping"]["n"]
            permissive_licenses = config["git-scraping"]["permissive_licenses"]
            execute_kwargs = {
                "topic": topic,
                "n": n,
                "permissive_licenses": permissive_licenses,
            }
            execute(**execute_kwargs)  # Call execute synchronously
            return {"status": "success", "topic": topic}
        except Exception as e:
            raise AirflowException("Error scraping topic") from e

    @task(task_id="validate_data")
    def validate_data(scrape_results: List[dict]):
        for result in scrape_results:
            if result["status"] != "success":
                raise AirflowException(f"Scraping failed for topic: {result['topic']}")
        return "Data validation successful"

    topics = get_topics()
    scrape_results = scrape_topic.expand(topic=topics)
    validate_data(scrape_results)


github_etl_dag = github_data_scraping_etl()
