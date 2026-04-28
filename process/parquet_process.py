from google.cloud import bigquery
from google.cloud import storage
import json
import yaml
from loguru import logger
import datetime
import sys
import pandas as pd
import uuid

with open('pipeline_config/config.yml', 'r') as file:
    config = yaml.safe_load(file)
if not config:
    logger.error("Config file is empty or not properly formatted.")

def process_parquet_files():

    storage_client = storage.Client()

    raw_bucket = config['storage']['raw_bucket_name']
    raw_bucket_name = storage_client.bucket(raw_bucket)

    processed_bucket = config['storage']['processed_bucket_name']
    processed_bucket_name = storage_client.bucket(processed_bucket)

    now = datetime.datetime.utcnow()

    run_id = sys.argv[1]
    mode = sys.argv[2]
    dt = sys.argv[3]
    if len(sys.argv) < 4:
        logger.error("Date argument is missing.")
        raise ValueError("Date argument is missing.")

    bq_client = bigquery.Client()
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
                                'PROCESS'
                            )
                    """).result()
    
    status = 'FAILED'
    error_message = None
    total_records = 0

    try:
        for source in config['sources']:
            repo = source['repo']
            endpoint = source['endpoint']

            prefix = f"raw/github/{repo}/{endpoint}/mode={mode}/dt={dt}/"
            logger.info(f"Processing files with prefix: {prefix}")

            blobs = list(raw_bucket_name.list_blobs(prefix=prefix))
            if not blobs:
                logger.warning(f"No files found with prefix: {prefix}")
                continue

            for blob in blobs:
                data = blob.download_as_text()
                records = json.loads(data)
                if not records:
                    logger.warning(f"No records found in blob: {blob.name}")
                    continue

                df = pd.DataFrame(records)
                if df.empty:
                    logger.warning(f"DataFrame is empty for blob- {blob.name}")
                    continue

                file_name = f"PART-{uuid.uuid4().hex}_{blob.name.split('/')[-1].replace('.json', '.parquet')}"
                
                processed_path = f"gs://{processed_bucket}/processed/github/{repo}/{endpoint}/mode={mode}/dt={dt}/{file_name}"
                df.to_parquet(processed_path, engine='pyarrow', index=False)
                logger.info(f"Processed blob {blob.name} and saved to {processed_path}")
                
                total_records += len(df)
                logger.info(f"Records processed for {repo}/{endpoint}: {len(df)}")
            
        status = 'SUCCESS'

    except Exception as e:
        logger.exception(f"Error processing parquet files for run_id {run_id}: {e}")
        error_message = str(e).replace("'", "").replace('"', ' ')
    
    finally:
        if status == 'SUCCESS':
            update_query = f"""UPDATE `instant-medium-491107-t6.metadata.pipeline_runs`
                                    SET status = 'SUCCESS',
                                    end_time = CURRENT_TIMESTAMP(),
                                    rows_processed = {total_records}
                               WHERE run_id = '{run_id}'
                               AND stage = 'PROCESS'"""
            bq_client.query(update_query).result()
            logger.info(f"Pipeline run {run_id} completed successfully with total records processed: {total_records}")
        
        else:
            failed_query = f"""UPDATE `instant-medium-491107-t6.metadata.pipeline_runs`
                                    SET status = 'FAILED',
                                    end_time = CURRENT_TIMESTAMP(),
                                    error_message = '{error_message}'
                               WHERE run_id = '{run_id}'
                               AND stage = 'PROCESS'"""
            bq_client.query(failed_query).result()
            raise Exception(f"Pipeline run {run_id} failed with error: {error_message}")

if __name__ == "__main__":
    process_parquet_files()