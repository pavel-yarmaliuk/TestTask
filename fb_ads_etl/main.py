import argparse
import logging
from extract import load_data
from transform import transform_record
from load import write_csv, write_json, load_to_bigquery_stub

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to fb_ads_mock.json")
    parser.add_argument("--output", required=True, help="Output file path")
    parser.add_argument("--format", choices=["csv", "json"], default="csv")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info(f"Loading data from {args.input}")
    records = load_data(args.input)
    logging.info(f"Loaded {len(records)} records")

    transformed_rows = []
    for rec in records:
        try:
            row = transform_record(rec)
            transformed_rows.append(row)
        except Exception as e:
            logging.error(f"Error transforming record {rec.get('ad_id')}: {e}")

    logging.info(f"Transformed {len(transformed_rows)} rows")

    if args.format == "csv":
        write_csv(transformed_rows, args.output)
    else:
        write_json(transformed_rows, args.output)

    # Stubbed BigQuery load
    # load_to_bigquery_stub(transformed_rows, "my_dataset", "fb_ads_insights", schema)

if __name__ == "__main__":
    main()