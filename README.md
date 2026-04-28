# Building a SaaS Analytics Platform — Production-Style Ingestion Pipeline

## Part 1 Update: Incremental + Lookback Ingestion (Fully Operational)

I’m building a SaaS analytics platform with a complete data pipeline, starting from external data ingestion.

For this phase, I’m using the GitHub API as the source, ingesting data from repositories like Apache Airflow and Kubernetes across multiple endpoints (branches, issues, pull requests, commits, stargazers).

---

## What’s New

The ingestion pipeline is now running end-to-end with both incremental and lookback strategies:

- Incremental extraction across multiple sources and endpoints  
- Raw data stored in GCS with partitioning ("mode / dt / run_ts")  
- Pipeline runs tracked in BigQuery (status, timestamps, rows processed, execution mode)  
- ~12K+ records processed per run  
- Dual execution modes implemented:
  - Incremental → frequent, lightweight ingestion  
  - Lookback → periodic reprocessing for data completeness  

---

## Key Design Decisions

### Watermark-Based Incremental Ingestion

Uses "last_run_ts" as a high-watermark (timestamp cursor) to fetch new data.

### Lookback Window for Late-Arriving Data

A 2-day reprocessing window ensures recently updated or delayed records are not missed.

### Mode-Based Execution

- Incremental → uses "last_run_ts" as the starting point (watermark) and advances it after a successful run  
- Lookback → ignores "last_run_ts" and instead uses a rolling window ("current_time - lookback_days")  

This ensures:
- Incremental mode processes only new data  
- Lookback mode reprocesses recent data to handle late-arriving updates  

### Handling API Limitations

GitHub API does not provide strict cursor guarantees.  
The pipeline intentionally allows overlap and ensures correctness downstream.

### Failure-Safe Design

Watermark updates only on successful incremental runs, enabling safe retries and recovery.

### Config-Driven Architecture

All sources, endpoints, and runtime parameters are defined in "config.yml".

### Secure Authentication

- GitHub API access via PAT (managed through environment variables)  
- GCP access via Workload Identity Federation (no service account keys)  

---

## Architecture

GitHub API  
→ Python ingestion layer  
→ GCS (raw, partitioned by mode/dt/run_ts)  
→ BigQuery (metadata + upcoming tables)  
→ dbt (transformations – upcoming)  
→ BI / dashboards (upcoming)

---

## Orchestration Strategy

Orchestration is currently handled using GitHub Actions with separate scheduled workflows:

- Hourly workflow → incremental ingestion  
- Daily workflow → lookback reprocessing  

### Why GitHub Actions instead of Airflow (for now)?

- Cost efficiency → avoids running a managed Airflow environment during early-stage development  
- Simplicity → faster setup with minimal operational overhead  
- CI/CD integration → tightly coupled with the codebase and version control  

Airflow will be introduced later as the pipeline complexity grows and requires advanced scheduling, dependencies, and monitoring.

---

## Key Learning

API-based ingestion is not perfectly incremental.

- Overlapping data is expected (>= semantics, pagination limits)  
- Data is skewed toward recent activity  
- Pipelines must ensure correctness rather than rely on source guarantees  

This pipeline follows a production-grade pattern:

Watermark + Lookback + Deduplication (downstream)

---

## Current Status

- Incremental + lookback ingestion fully operational  
- Observability layer implemented (pipeline_runs metadata)  
- Safe checkpointing and retry logic in place  
- CI/CD orchestration via GitHub Actions (separate workflows per mode)  

---

## Next Steps

- JSON → Parquet optimization  
- Load raw data into BigQuery  
- dbt transformations (staging → marts with deduplication)  
- Data quality checks and validation layer  

---