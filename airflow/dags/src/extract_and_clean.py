import pandas as pd
import logging
import ast
from .data_config import DataConfig as Data

logger = logging.getLogger(__name__)



def get_country_code(x):
    """ Extracts the country code from a dictionary. """
    if isinstance(x, dict):
        return x.get('iso_3166_1')
    else:
        return None

def extract_and_clean(download_path, pqt_path):
    """ Extracts data from CSV files, cleans it, and saves it as parquet files. """
    for file in Data.FILE_DATA:
        full_path = download_path + file["name"]
        try:
            # Read the CSV file
            df = pd.read_csv(full_path)
            logger.info(f"File {full_path} read successfully.")
        except Exception as e:
            logger.error(f"File {full_path} could not be read. Error: {e}", exc_info=True)
            continue  # Skip to the next file if reading fails

        try:
            # Get only columns we are interested in
            df = df[file["columns"]]
            logger.info("Start cleaning dataset")

            # Clean dataset
            df.drop_duplicates(inplace=True)
            df.dropna(inplace=True)

            # The following columns should contain no zero values
            if "no_zero_columns" in file:
                try:
                    no_zero_columns = file["no_zero_columns"]
                    logger.info(f"Dropping zero values in {no_zero_columns}")
                    df = df[(df[no_zero_columns] != 0).all(axis=1)]
                    logger.info("Zero values in no_zero_columns dropped successfully.")
                except Exception as e:
                    logger.warning(f"Error during removing zero values in no_zero_columns. Error: {e}", exc_info=True)

            for col in df.columns:
                # Convert date strings to numeric values
                if 'release_date' in col:
                    logger.info("Parsing datetime")
                    df[col] = pd.to_datetime(df[col]).dt.year
                    df.rename(columns={'release_date': 'release_year'}, inplace=True)
                if 'production_countries' in col:
                    # Convert to list with dict, extract only country code
                    logger.info("Start parsing production countries")
                    try:
                        df['production_countries'] = (
                            df['production_countries']
                            .apply(ast.literal_eval)        # Convert to list of dict
                            .explode('production_countries')# Expand rows (since single dict - get rid of list)
                            .apply(get_country_code)        # Get only iso_3166_1 country code
                        )
                        df['production_countries'].fillna('UNKNOWN', inplace=True)
                        logger.info("Production countries parsed successfully.")
                    except Exception as e:
                        logger.warning(f"Parsing production countries failed. Error: {e}", exc_info=True)

            df.reset_index(drop=True, inplace=True)

            # Map data types
            logger.info("Mapping datatypes")
            df = df.astype(Data.DATATYPE[file["name"]])

            # Provide short summary
            zeros_count = (df == 0).sum()
            null_count = df.isnull().sum()
            result = pd.DataFrame({'Zeros': zeros_count, 'Nulls': null_count})
            logger.info(f"Results after cleanup: \n{result}")

            # Save as parquet file
            pqt_filename = pqt_path + file["name"].replace('.csv', '.parquet')
            logger.info(f"Saving parquet file to {pqt_filename}")
            df.to_parquet(pqt_filename)
            logger.info(f"Extracting and cleaning of {file['name']} completed.")

        except Exception as e:
            logger.error(f"An error occurred while processing {file['name']}. Error: {e}", exc_info=True)

