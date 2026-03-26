# Task 1
## Overview
This pipeline reads the provided fb_ads_mock.json, normalizes nested actions and action_values arrays into flat columns, and outputs one row per unique combination of (ad_id, date_start, age, gender). The result is prepared for loading into BigQuery (stubbed loader). The code is organised into logical modules: extraction, transformation, loading, and configuration.

## Key Design Choices & Assumptions
1. Consistent event naming - For each desired event (purchase, add_to_cart, initiate_checkout, add_payment_info), we consistently use the simplest action type:
   - "purchase"
   - "add_to_cart"
   - "initiate_checkout"
   - "add_payment_info"

This avoids confusion from duplicate namespaces. If a record does not contain the chosen action type, the value defaults to 0.
2. Handling missing data - All fields are accessed safely using dict.get() with defaults. Numeric fields are cast to appropriate types; non‑numeric values are logged and defaulted.
3. Output schema - The output includes:
   - All original flat metrics (impressions, clicks, spend, cpm, etc.)
   - Extracted funnel event counts and revenues (from action_values)
   - Derived roas = purchase revenue / spend (0 if spend = 0)
4. One row per input record - The input already provides unique combinations of (ad_id, date_start, age, gender), so no explicit grouping is required.
5. No hard‑coded logic - All mappings (e.g., which action types to extract) and output column names are stored in config.py, allowing easy modification.

## Deployment on GCP
The pipeline is suitable for a serverless execution model:
- Cloud Run Job – Package the code as a container, run on a schedule with Cloud Scheduler.
- Cloud Composer (Airflow) – Orchestrate as a DAG for more complex workflows.
- Dataflow – If scaling is needed, rewrite using Apache Beam for streaming/batch.
- Cloud Function – For simple, event‑driven ingestion.

In production we would:
- Store raw JSON in Cloud Storage and trigger the pipeline.
- Use BigQuery client directly to load the final DataFrame.
- Add schema validation and incremental handling.