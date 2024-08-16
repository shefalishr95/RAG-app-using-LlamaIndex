import json
from typing import Any, Dict
import os
import requests
from requests.exceptions import HTTPError

from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from .process import preprocess_data


class Config:
    """
    Loads all configurations from `config.json` for the project.
    """

    def __new__(cls) -> Dict[str, Any]:
        with open("./config/config.json", "r") as f:
            config = json.load(f)
        return config


config = Config()
print("Loaded config in extract.py file")

github_api_key = os.environ["GITHUB_API_KEY"].strip()


def fetch_license_info(session, repo_url, headers):
    """Fetches license information from a repository."""
    license_url = f"{repo_url}/license"
    try:
        response = session.get(license_url, headers=headers)
        response.raise_for_status()
        license_info = response.json()
        return license_info.get("license", {}).get("spdx_id", "NO LICENSE")
    except HTTPError as e:
        print(f"Error fetching license: {e}")
        return "NO LICENSE"
    except Exception as e:
        print(f"Unexpected error fetching license: {e}")
        return "NO LICENSE"


def fetch_readme_from_repo(**kwargs):
    """Download README files from GitHub repositories."""
    readme_url = f'{kwargs["repo_url"]}/readme'
    try:
        response = kwargs["session"].get(readme_url, headers=kwargs["headers"])
        response.raise_for_status()
        readme_content = response.json()
        download_url = readme_content.get("download_url", "")
    except HTTPError as e:
        print(f"Error downloading README URL: {e}")
        return

    repo_id = kwargs["repo_url"].split("/")[-1]  # repo name!
    file_name = f'{kwargs["topic"]}_{repo_id}_README.md'

    try:
        response = kwargs["session"].get(download_url, headers=kwargs["headers"])
        response.raise_for_status()
        file_content = response.text
        try:
            gcshook = GCSHook(gcp_conn_id=config["gcs"]["gcs_connection_id"])
        except Exception as e:
            raise AirflowFailException("Error connecting to GCS hook") from e

        # Upload raw README file to GCS
        try:
            if not gcshook.exists(
                bucket_name=config["gcs"]["raw_files_upload_bucket"],
                object_name=file_name,
            ):
                gcshook.upload(
                    bucket_name=config["gcs"]["raw_files_upload_bucket"],
                    object_name=file_name,
                    data=file_content,
                    encoding="utf-8",
                )
            else:
                print("Raw file not uploaded; already exists!")
        except Exception as e:
            raise AirflowFailException("Error uploading raw files to GCS") from e

        # Preprocess the README content
        preprocessed_content = preprocess_data(file_content)

        # Upload preprocessed README file to a different GCS bucket
        try:
            if not gcshook.exists(
                bucket_name=config["gcs"]["processed_files_upload_bucket"],
                object_name=file_name,
            ):
                gcshook.upload(
                    bucket_name=config["gcs"]["processed_files_upload_bucket"],
                    object_name=file_name,
                    data=preprocessed_content,
                    encoding="utf-8",
                )
            else:
                print("Processed file not uploaded; already exists!")
        except Exception as e:
            print("Error uploading processed files to GCS bucket")
    except Exception as e:
        print(f"Encountered an error in downloading URL: {e}")


def process_repo(**kwargs):
    """Fetches README.md files for repos with acceptable licenses"""
    session = kwargs["session"]
    repo = kwargs["repo"]
    headers = kwargs["headers"]
    topic = kwargs["topic"]
    permissive_licenses = kwargs["permissive_licenses"]

    full_name = repo["full_name"]
    repo_url = repo["url"]
    license_id = fetch_license_info(session, repo_url, headers)

    if license_id in permissive_licenses:
        fetch_readme_from_repo(
            session=session, repo_url=repo_url, headers=headers, topic=topic
        )
    elif license_id == "NO LICENSE":
        print(f"Warning: {full_name} has no license specified. Proceeding anyway")
        fetch_readme_from_repo(
            session=session, repo_url=repo_url, headers=headers, topic=topic
        )
    else:
        print(f"Skipping {full_name} due to incompatible license!")


def execute(**kwargs):
    """Execute all functions"""
    topic = kwargs["topic"]
    n = kwargs["n"]
    permissive_licenses = kwargs["permissive_licenses"]

    headers = {"Authorization": f"token {github_api_key}"}

    with requests.Session() as session:
        url = f"https://api.github.com/search/repositories?q=stars:10..1500+topic:{topic}&sort=stars"
        try:
            response = session.get(url, headers=headers)
            response.raise_for_status()
            api_json = response.json()
            repos = api_json.get("items", [])[:n]
            # no pagination

            for repo in repos:
                process_repo(
                    session=session,
                    repo=repo,
                    headers=headers,
                    topic=topic,
                    permissive_licenses=permissive_licenses,
                )
                # time.sleep(1)  # Delay to avoid rate limit

        except HTTPError as e:
            print(f"Error fetching repo for topic {topic}: {e}")
        except Exception as e:
            print(f"Unexpected error in execute: {e}")
