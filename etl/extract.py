import os
import asyncio
import polars as pl
import aiohttp
from config import IMDB_BASE_URL, RAW_FILE_NAMES
from utils.data_downloader import download_to_dataframe

async def extract_movies(session: aiohttp.ClientSession) -> pl.DataFrame:
    """ Extracts movie data from IMDb dataset by downloading directly from the web."""
    url = IMDB_BASE_URL + RAW_FILE_NAMES["MOVIES"]
    return await download_to_dataframe(session, url)

async def extract_ratings(session: aiohttp.ClientSession) -> pl.DataFrame:
    """ Extracts ratings data from IMDb dataset by downloading directly from the web."""
    url = IMDB_BASE_URL + RAW_FILE_NAMES["RATINGS"]
    return await download_to_dataframe(session, url)

async def extract_data_async() -> dict[str, pl.DataFrame]:
    """
    Extracts all required data from IMDb datasets asynchronously.
    Downloads movies and ratings data concurrently using a shared session.
    
    Returns:
        dict[str, pl.DataFrame]: Dictionary containing movies and ratings DataFrames
    """    
    # Create a shared session for both downloads
    async with aiohttp.ClientSession() as session:
        # Execute both downloads concurrently
        movies_df, ratings_df = await asyncio.gather(
            extract_movies(session),
            extract_ratings(session)
        )
    
    return {
        "movies": movies_df,
        "ratings": ratings_df
    }

def extract_data() -> dict[str, pl.DataFrame]:
    """
    Synchronous wrapper for extract_data_async.
    
    Returns:
        dict[str, pl.DataFrame]: Dictionary containing movies and ratings DataFrames
    """
    return asyncio.run(extract_data_async())

if __name__ == "__main__":
    import logging
    from config import RAW_FILEPATHS
    
    logging.info("Starting data extraction...")
    try:
        data = extract_data()
        
        os.makedirs(os.path.dirname(RAW_FILEPATHS["MOVIES"]), exist_ok=True)
        data["movies"].write_csv(RAW_FILEPATHS["MOVIES"], separator='\t')
        data["ratings"].write_csv(RAW_FILEPATHS["RATINGS"], separator='\t')
        
        for key, df in data.items():
            logging.info(f"Extracted {len(df)} rows for {key}")
            logging.debug(f"DataFrame {key} schema:\n{df.schema}")

        logging.info("Data extraction completed successfully.")
    except Exception as e:
        logging.error(f"Data extraction failed: {str(e)}")