import logging
import aiohttp
import polars as pl

async def download_to_dataframe(session: aiohttp.ClientSession, url: str) -> pl.DataFrame:
    """
    Downloads a .tsv.gz file from a URL asynchronously and reads it directly into a Polars DataFrame.
    
    Args:
        session (aiohttp.ClientSession): Shared aiohttp session for connection pooling
        url (str): URL of the .tsv.gz file to download
    
    Returns:
        pl.DataFrame: DataFrame containing the downloaded data
    """
    try:        
        async with session.get(url) as response:
            response.raise_for_status()
            content = await response.read()
        
        # Read directly from response content
        df = pl.read_csv(
            content,
            separator='\t',
            null_values='\\N',
            quote_char=None,
        )
        
        return df
    except aiohttp.ClientError as e:
        logging.error(f"Failed to download {url}: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to parse data from {url}: {e}")
        raise