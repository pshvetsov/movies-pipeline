
import logging
import pyarrow.fs as pfs
from .data_config import DataConfig as Data

logger = logging.getLogger(__name__)

def upload_to_gcs(storage_path):
    """ Uploads files from a local storage path to Google Cloud Storage (GCS). """
    gcs_path = Data.GCS_PATH
    try:
        # Initialize GCS file system
        gcs = pfs.GcsFileSystem()
        dir_info = gcs.get_file_info(gcs_path)
        
        # Check if the directory exists and delete if it does
        if dir_info.type != pfs.FileType.NotFound:
            gcs.delete_dir(gcs_path)

        # Create a new directory on GCS
        gcs.create_dir(gcs_path)
        logger.info(f"Created directory at {gcs_path}")
        
        logger.info(f"Start copying parquet files from {storage_path} to {gcs_path} on GCS")
        pfs.copy_files(
            source=storage_path,
            destination=gcs_path,
            destination_filesystem=gcs
        )
    except Exception as e:
        # Log any exception that occurs during the process
        logger.error(f"An error occurred while uploading files to GCS: {e}", exc_info=True)
        raise
    
    else:
        logger.info('Uploaded parquet files to gsc')