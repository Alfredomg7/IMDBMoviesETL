import logging
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

logger = logging.getLogger(__name__)

def main():
    logger.info("Starting ETL process...")
    try:
        # Extract
        logger.info("Starting data extraction...")
        raw_data = extract_data()
        logger.info("Data extraction completed.")

        # Transform
        logger.info("Starting data transformation...")
        transformed_data = transform_data(raw_data)
        logger.info("Data transformation completed.")

        # Load
        logger.info("Starting data load to BigQuery...")
        load_data(transformed_data)
        logger.info("Data load to BigQuery completed.")

        logger.info("ETL process completed successfully.")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")

if __name__ == "__main__":
    main()