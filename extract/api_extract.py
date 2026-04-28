from loguru import logger
import yaml
from dotenv import load_dotenv 
import os
from google.cloud import storage
import datetime
from google.cloud import bigquery
import uuid
import requests
import json
import sys

logger.add("logs/pipeline.log",
           level="INFO",
           rotation="10 MB",
           retention="7 days")

with open('pipeline_config/config.yml', 'r') as file:
    config = yaml.safe_load(file)
if not config:
    logger.error("Config file is empty or not properly formatted.")
    raise ValueError("Config file is empty or not properly formatted.")

load_dotenv()
GITHUB_PAT_TOKEN = os.getenv('PAT_GITHUB_API_AUTH_TOKEN')
if not GITHUB_PAT_TOKEN:
    logger.error("GitHub API token not found in environment variables.")
    raise ValueError("GitHub API token not found in environment variables.")
headers = {
    'Authorization': f'Bearer {GITHUB_PAT_TOKEN}',
    'Accept': 'application/vnd.github.v3.star+json'
}


def fetch_github_data(repo, endpoint, page_size, max_pages, since_ts):
    timeout = config['runtime']['request_timeout']
    base_url = f"https://api.github.com/repos/{repo}/{endpoint}"
    all_data = []
    page = 1
    while page <= max_pages:    
        params = {
            'per_page': page_size,
            'page': page,
            'since': since_ts.isoformat() + 'Z'
        }
        response = requests.get(base_url, headers=headers, params=params, timeout=timeout)
        if response.status_code != 200:
            logger.error(f"Error fetching data from {base_url}: {response.status_code} - {response.text}")
            raise Exception(f"Error fetching data from {base_url}")
        data = response.json()
        if not data:
            break
        all_data.extend(data)
        page += 1
    return all_data


def run_pipeline(mode):
    now = datetime.datetime.utcnow()
    date_str = now.strftime("%Y-%m-%d")
    run_ts = now.strftime("%Y-%m-%dT%H-%M-%SZ")
    lookback_days = config['runtime']['lookback_days']
    storage_client = storage.Client()
    bucket_name = config['storage']['raw_bucket_name']
    bucket = storage_client.bucket(bucket_name)
    bq_client = bigquery.Client()

    select_query = f"""SELECT last_run_ts
                   FROM `instant-medium-491107-t6.metadata.pipeline_runs`
                   WHERE pipeline_name = 'saas-analytics-platform'
                   AND status = 'SUCCESS'
                   AND last_run_ts IS NOT NULL 
                   ORDER BY last_run_ts DESC
                   LIMIT 1"""
    logger.info(f"[{mode.upper()}] Running pipeline in {mode} mode. Fetching last successful pipeline run timestamp from BigQuery...")
    result = bq_client.query(select_query).to_dataframe()

    if result.empty:
       logger.warning("No successful pipeline runs found in the database. Defaulting to lookback mode with configured lookback_days.")
       raise ValueError("No successful pipeline runs found in the database.")
    else:
        last_run_ts = result['last_run_ts'][0]
        logger.info(f"[{mode.upper()}] Last successful pipeline run timestamp: {last_run_ts}")
    
    if mode == 'incremental':
        since_ts = last_run_ts - datetime.timedelta(minutes=5)
    elif mode == 'lookback':
        since_ts = now - datetime.timedelta(days=lookback_days)
    else:
        logger.error("Invalid mode provided. Use 'incremental' or 'lookback'.")
        raise ValueError("Invalid mode. Use 'incremental' or 'lookback'.")

    uuid_str = str(uuid.uuid4())
    run_id = f"{uuid_str}_{run_ts}"
    bq_client.query(f"""INSERT INTO `instant-medium-491107-t6.metadata.pipeline_runs`
                                (
                                    run_id,
                                    pipeline_name,
                                    status,
                                    start_time,
                                    mode,
                                    stage
                                )
                        VALUES 
                                (
                                    '{run_id}',
                                    'saas-analytics-platform',
                                    'RUNNING',
                                    TIMESTAMP('{now.isoformat()}'),
                                    '{mode}',
                                    'EXTRACT'
                                )
                    """).result()
    logger.info(f"[{mode.upper()}] Pipeline run {run_id} started with mode: {mode}. Fetching data since: {since_ts}...")
    
    status = 'FAILED'
    error_message = None
    total_rows = 0

    try:
        runtime = config['runtime']
        for source in config['sources']:
            repo = source['repo']
            endpoint = source['endpoint']
            page_size = runtime['page_size']
            max_pages = runtime['max_pages']
            
            logger.info(f"[{mode.upper()}] Fetching data for {repo} - {endpoint} since {since_ts}...")
            data = fetch_github_data(repo, endpoint, page_size, max_pages, since_ts)
            if not data:
                logger.warning(f"No data found for {repo} - {endpoint}")
                continue

            json_data = json.dumps(data, default=str)

            gcs_path = f"raw/github/{repo}/{endpoint}/mode={mode}/dt={date_str}/run_ts={run_ts}/data.json"

            blob = bucket.blob(gcs_path)
            blob.upload_from_string(json_data,
            content_type='application/json')
            logger.info(f"Data for {repo} - {endpoint} uploaded to GCS at {gcs_path}")

            total_rows += len(data)
            logger.info(f"[{mode.upper()}] Rows fetched this endpoint: {len(data)}")    
        logger.info(f"[{mode.upper()}] Final total rows : {total_rows}")

        status = 'SUCCESS'

    except Exception as e:
        logger.exception(f"Error during pipeline run {run_id}- {e}")
        error_message = str(e).replace("'", "").replace('"', ' ')

    finally:
        if status == 'SUCCESS':
            if mode == 'incremental':
                update_query = f"""UPDATE `instant-medium-491107-t6.metadata.pipeline_runs`
                                    SET status = 'SUCCESS',
                                        end_time = CURRENT_TIMESTAMP(),
                                        rows_processed = {total_rows},
                                        last_run_ts = TIMESTAMP('{now.isoformat()}')
                                    WHERE run_id = '{run_id}'
                                    AND stage = 'EXTRACT'"""
                bq_client.query(update_query).result()
                logger.info(f"Pipeline run {run_id} marked as SUCCESS with {total_rows} rows processed.")
            else:
                update_query = f"""UPDATE `instant-medium-491107-t6.metadata.pipeline_runs`
                                SET status = 'SUCCESS',
                                    end_time = CURRENT_TIMESTAMP(),
                                    rows_processed = {total_rows},
                                    last_run_ts = TIMESTAMP('{now.isoformat()}')
                                WHERE run_id = '{run_id}'
                                AND stage = 'EXTRACT'"""
                bq_client.query(update_query).result()
                logger.info(f"Pipeline run {run_id} marked as SUCCESS with {total_rows} rows processed.")
        else:
            failed_query = f"""UPDATE `instant-medium-491107-t6.metadata.pipeline_runs`
                                SET status = 'FAILED',
                                    end_time = CURRENT_TIMESTAMP(),
                                    error_message = '{error_message}'
                                WHERE run_id = '{run_id}'
                                AND stage = 'EXTRACT'"""
            bq_client.query(failed_query).result()
            logger.error(f"Pipeline run {run_id} marked as FAILED due to error: {error_message or 'Unknown error'}")
            raise Exception(f"Pipeline run {run_id} failed with error: {error_message or 'Unknown error'}") 
        return run_id, date_str

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else 'incremental'
    if mode not in ['incremental', 'lookback']:
        logger.error("Invalid mode provided. Use 'incremental' or 'lookback'.") 
        raise ValueError("Invalid mode. Use 'incremental' or 'lookback'.")

    run_id, date_str = run_pipeline(mode)
    logger.info(f"Pipeline run completed with run_id: {run_id}")
    print(f"RUN_ID={run_id}")
    print(f"MODE={mode}")
    print(f"DATE_STR={date_str}")