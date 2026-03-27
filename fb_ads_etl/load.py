import csv
import json
import logging

def write_csv(rows, output_path):
    if not rows:
        logging.warning("No rows to write.")
        return
    keys = rows[0].keys()
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)

def write_json(rows, output_path):
    with open(output_path, 'w') as f:
        json.dump(rows, f, indent=2)

def load_to_bigquery_stub(rows, dataset, table, schema):
    """Stubbed BigQuery loader. In production, use google.cloud.bigquery.Client."""
    logging.info(f"Would load {len(rows)} rows to {dataset}.{table}")