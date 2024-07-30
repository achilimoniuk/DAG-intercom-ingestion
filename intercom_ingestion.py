import pendulum
import pandas as pd
import requests
import time
import logging

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from airflow.decorators import dag, task, task_group
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from config import env_name, default_last_update, api_url, dataset_name, api_key, data_dict, schema, bucket_name

@dag(
    schedule_interval='@hourly',  # Define the frequency of the DAG execution
    catchup=False,
    tags=["intercom", "data_ingestion", "v.1.0.0"],
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    max_active_runs=1,
)
def intercom_data_ingestion_pipeline():

    @task
    def init_task() -> None:
        logging.info("Initialization task started.")
        pass

    def generate_headers(api_key: str) -> dict:
        return {
            "Authorization": f"Bearer {api_key}",
            "Intercom-Version": "2.10",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    @task
    def retrieve_last_update(table_name: str) -> str:
        query = f"""
        SELECT MAX(updated_at) as last_update
        FROM `{env_name}.{dataset_name}.{table_name}`
        WHERE updated_at IS NOT NULL
        """
        
        bq_client = bigquery.Client()
        logging.info("Running query to get the last update timestamp.")
        
        last_update = pendulum.parse(default_last_update)
        
        try:
            result = bq_client.query(query).result()
            if not result.total_rows:
                logging.info(f"No records found. Assuming last update: {default_last_update}")
            else:
                last_update = list(result)[0]["last_update"]
                logging.info(f"Last update timestamp retrieved: {last_update}")
        except NotFound:
            logging.warning(f"Table not found. Assuming default last update: {default_last_update}")
        
        return last_update.strftime("%Y-%m-%dT%H:%M:%SZ")

    def execute_request(method: str, url: str, **kwargs) -> dict:
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()

            rate_limit_remaining = response.headers.get("x-ratelimit-remaining")
            rate_limit_reset = response.headers.get("x-ratelimit-reset")
            logging.info(f"Rate limit remaining: {rate_limit_remaining}")
            
            if int(rate_limit_remaining) < 15:
                sleep_duration = max(0, int(rate_limit_reset) - int(pendulum.now("UTC").timestamp()))
                logging.info(f"Rate limit threshold reached. Sleeping for {sleep_duration} seconds.")
                time.sleep(sleep_duration)
            
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Request error: {e}")
            return {}

    def fetch_data(url: str, headers: dict, payload: dict, key: str) -> list:
        collected_data = []

        while True:
            response_data = execute_request("POST", url, headers=headers, json=payload)
            if not response_data:
                break
            logging.info(f"Data received: {response_data.get('pages', {})}")

            items = response_data.get(key, [])
            if not items:
                break
            collected_data.extend(items)

            next_start_after = response_data.get("pages", {}).get("next", {}).get("starting_after")
            if not next_start_after:
                break
            payload["pagination"]["starting_after"] = next_start_after

        return collected_data

    @task(task_id="fetch_all_records")
    def fetch_all_records(data_key: str, table_name: str, last_update: str, headers: dict) -> list:
        search_url = f"{api_url}/{table_name.split('_')[0]}/search"
        last_update_epoch = int(pendulum.parse(last_update).timestamp())
        payload = {
            "query": {"field": "updated_at", "operator": ">", "value": last_update_epoch},
            "pagination": {"per_page": 150},
        }

        return fetch_data(search_url, headers, payload, data_key)

    @task
    def retrieve_details(data: list, data_key: str, headers: dict) -> list:
        detailed_records = []
        current_time = pendulum.now()

        for record in data:
            record_id = record["id"]
            detail_url = f"{api_url}/{data_key}/{record_id}"
            record_details = execute_request("GET", detail_url, headers=headers))
            detailed_records.append(record_details)

        return detailed_records

    @task(task_id="upload_data_to_gcs")
    def upload_data_to_gcs(data: list, table_name: str) -> str:
        logging.info("Converting data to DataFrame.")
        df = pd.DataFrame(data)

        timestamp = int(pendulum.now('UTC').timestamp())
        file_name = f"{table_name}_{timestamp}.jsonl"
        gcs_path = f"gs://{bucket_name}/{table_name}/{file_name}"
        logging.info(f"Uploading data to: {gcs_path}")

        local_file_path = f"/tmp/{file_name}"
        df.to_json(local_file_path, orient="records", lines=True)

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{table_name}/{file_name}")

        blob.upload_from_filename(local_file_path)
        logging.info(f"Data successfully uploaded to GCS: {gcs_path}")

        return f"{table_name}/{file_name}"

    @task.branch(task_id="validate_data_availability", trigger_rule=TriggerRule.NONE_FAILED)
    def validate_data_availability(data: list, table_name: str) -> str:
        return f"{table_name}_workflow.upload_data_to_gcs" if data else None

    start_task = init_task.override(task_id="start_pipeline")()
    end_task = init_task.override(task_id="end_pipeline")()

    ingestion_workflows = []
    for table_name, data_key in data_dict.items():
        @task_group(group_id=f"{table_name}_workflow")
        def process_data():
            headers = generate_headers(api_key)
            last_update_time = retrieve_last_update(table_name)
            raw_data = fetch_all_records(data_key, table_name, last_update_time, headers)
            detailed_data = retrieve_details(raw_data, data_key, headers)
            validate_task = validate_data_availability(detailed_data, table_name)
            gcs_file_path = upload_data_to_gcs(detailed_data, table_name)
            
            gcs_to_bq_task = GCSToBigQueryOperator(
                task_id="load_to_bq",
                bucket=bucket_name,
                source_objects=[gcs_file_path],
                destination_project_dataset_table=f"{env_name}.{dataset_name}.{table_name}",
                schema_fields=schema,
                source_format="NEWLINE_DELIMITED_JSON",
                write_disposition="WRITE_APPEND",
                autodetect=False
            )
            
            last_update_time >> raw_data >> detailed_data >> validate_task >> gcs_file_path >> gcs_to_bq_task

        ingestion_workflows.append(process_data())

    start_task >> [workflow for workflow in ingestion_workflows] >> end_task

intercom_data_ingestion_pipeline()
