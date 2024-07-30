
# Intercom Data Ingestion Pipeline
## Overview
The Intercom Data Ingestion Pipeline is an Apache Airflow Directed Acyclic Graph (DAG) designed to automate the extraction, transformation, and loading (ETL) of data from the Intercom API to Google BigQuery. This pipeline performs several key operations:

1. Retrieves the last update timestamp from BigQuery.
2. Fetches new or updated records from the Intercom API.
3. Retrieves detailed information for each record.
4. Saves the data to Google Cloud Storage (GCS).
5. Loads the data from GCS into BigQuery.
## Features
- Scheduled Execution: Configured to run hourly.
- Data Fetching: Handles pagination and rate limits when fetching data from the Intercom API.
- Data Transformation: Converts data to JSON lines format for compatibility with BigQuery.
- Data Loading: Automatically loads data into BigQuery, appending to existing tables.
## Prerequisites
Before you begin, ensure you have:
1. Google Cloud Account: Access to Google Cloud Storage and BigQuery.
2. Intercom API Access: An API key for accessing Intercom data.
3. Apache Airflow: Properly set up and configured.
