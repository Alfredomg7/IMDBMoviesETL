import logging
import polars as pl
from google.cloud import bigquery
    
def load_df_to_bigquery(df: pl.DataFrame, table_id: str, client: bigquery.Client, write_disposition: str = "WRITE_APPEND") -> None:
    """
    Loads a Polars DataFrame directly into a BigQuery table.

    Args:
        df (pl.DataFrame): Polars DataFrame to load into BigQuery.
        table_id (str): The ID of the BigQuery table to load data into.
        client (bigquery.Client): An authenticated BigQuery client instance.
        write_disposition (str): BigQuery write disposition (e.g., 'WRITE_APPEND', 'WRITE_TRUNCATE').

    Returns:
        None
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
    )
    
    # Convert Polars DataFrame to pandas for BigQuery compatibility
    pandas_df = df.to_pandas()
    
    job = client.load_table_from_dataframe(pandas_df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

def create_dataset(client: bigquery.Client, dataset_id: str) -> None:
    """
    Creates a BigQuery dataset if it does not already exist.

    Args:
        client (bigquery.Client): An authenticated BigQuery client instance.
        dataset_id (str): The ID of the dataset to create.
     
    Returns:
        None
    """
    datasets = list(client.list_datasets())
    if any(dataset.dataset_id == dataset_id for dataset in datasets):
        return
    
    try:
        dataset_ref = client.dataset(dataset_id)
        client.create_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} created.")
    except Exception as e:
        logging.error(f"Error creating dataset {dataset_id}: {e}")