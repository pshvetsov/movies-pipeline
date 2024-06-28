import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DataConfig():
    _movies_meta_name = "movies_metadata.csv"
    _ratings_name = "ratings_small.csv"

    FILE_DATA = [
        {
            "name": _movies_meta_name,
            "columns": ["id", "budget", "genres", "original_language", "title", 
                        "production_countries", "release_date", "revenue", "status", 
                        "vote_average", "vote_count"],
            "no_zero_columns": ["vote_average", "vote_count"]
        },
        {
            "name": _ratings_name,
            "columns": ["userId", "movieId", "rating"]
        }
    ]
    DATATYPE = {
        _movies_meta_name: {
            'id': pd.Int64Dtype(),
            'budget': pd.Float64Dtype(),
            'genres': pd.StringDtype(),
            'original_language': pd.StringDtype(),
            'title': pd.StringDtype(),
            'production_countries': pd.StringDtype(),
            'release_year': pd.Int64Dtype(),
            'revenue': pd.Float64Dtype(),
            'status': pd.StringDtype(),
            'vote_average': pd.Float64Dtype(),
            'vote_count': pd.Int64Dtype()
        },
        _ratings_name: {
            'userId': pd.Int64Dtype(),
            'movieId': pd.Int64Dtype(),
            'rating': pd.Float64Dtype()
        }
    }
    
    GCS_BUCKET = os.environ.get('GCP_BUCKET')
    try:
        if GCS_BUCKET is None:
            raise ValueError
        if (not len(GCS_BUCKET)):
            raise ValueError
    except ValueError:
        logger.error("GCS_BUCKET environment variable is not set!")
    
    if (not GCS_BUCKET.endswith('/')):
        GCS_BUCKET += '/'
    
    GCS_PATH = GCS_BUCKET+"parquet"
