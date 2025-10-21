from google.oauth2 import service_account
from google.cloud import bigquery

def get_bigquery_client(credentials_dict: dict, project_id: str) -> bigquery.Client:
    """
    Creates and returns a BigQuery client using the provided service account credentials as a dict.

    Args:
        credentials_dict (dict): Dictionary containing service account credentials.
        project_id (str): Google Cloud project ID to associate with the BigQuery client.

    Returns:
        bigquery.Client: An authenticated BigQuery client instance.
    """
    try:
        if not credentials_dict:
            raise ValueError("Failed to load Google Cloud credentials: credentials dictionary is empty.")
        if not project_id:
            raise ValueError("Failed to load Google Cloud project ID: project_id is empty.")
        
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        return client
    except Exception as e:
        raise RuntimeError(f"Failed to create BigQuery client: {e}")