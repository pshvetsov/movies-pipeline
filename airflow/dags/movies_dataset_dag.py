import datetime
import logging
import os
import sys

from src import DataConfig, extract_and_clean, upload_to_gcs

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup

logging.basicConfig(
    level=logging.INFO,
    filename="webscraper.log",
    filemode="w",
    format="%(levelname)s:%(name)s:%(asctime)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("").addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger(__name__)

# TODO bind terraform variables and .env variables together to
# avoid decentralisation of variables definition.
AIRFLOW_PATH = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow/")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_RAW_DATASET = os.environ.get("GCP_RAW_DATASET")
GCP_ANALYTICS_DATASET = os.environ.get("GCP_ANALYTICS_DATASET")
DBT_WORKDIR = os.environ.get("DBT_WORKDIR")

if not AIRFLOW_PATH.endswith("/"):
    AIRFLOW_PATH += "/"

download_path = AIRFLOW_PATH + "data/"
if not os.path.exists(download_path):
    os.makedirs(download_path)

pqt_path = download_path + "pqt/"
if not os.path.exists(pqt_path):
    os.makedirs(pqt_path)

table_names = tuple(file["name"].split(".")[0] for file in DataConfig.FILE_DATA)

default_args = {
    "owner": "shv",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=2),
}

with DAG(
    "movie_database_dag",
    start_date=datetime.datetime(2024, 1, 1),
    default_args=default_args,
    description="DAG for movie database ETL",
    schedule_interval="@once",
    user_defined_macros={
        "GCP_RAW_DATASET": GCP_RAW_DATASET,
        "GCP_ANALYTICS_DATASET": GCP_ANALYTICS_DATASET,
        "MOVIES_METADATA_TABLE": table_names[0],
        "RATINGS_TABLE": table_names[1],
    },
) as dag:
    pull_dataset_task = BashOperator(
        task_id="pull_dataset",
        bash_command=(
            f"kaggle datasets download -d rounakbanik/the-movies-dataset "
            f"--path {download_path} --unzip"
        ),
    )

    extract_and_clean_task = PythonOperator(
        task_id="extract_and_clean",
        python_callable=extract_and_clean,
        op_kwargs={"download_path": download_path, "pqt_path": pqt_path},
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={"storage_path": pqt_path},
    )

    with TaskGroup("create-external-tables") as create_external_tables_task:
        for table in table_names:
            BigQueryCreateExternalTableOperator(
                task_id=f"BQ_external_table_{table}_task",
                table_resource={
                    "tableReference": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": GCP_RAW_DATASET,
                        "tableId": table,
                    },
                    "externalDataConfiguration": {
                        "autodetect": True,
                        "sourceFormat": "PARQUET",
                        "sourceUris": [f"gs://{DataConfig.GCS_PATH}/{table}.parquet"],
                    },
                },
            )

    create_table_partitions_task = BigQueryInsertJobOperator(
        task_id="create_table_partitions_task",
        configuration={
            "query": {
                "query": "{% include 'sql/load-dwh.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    clean_up_task = BashOperator(
        task_id="clean_up_task", bash_command=f"rm -rf {download_path}"
    )

    install_dbt_task = BashOperator(
        task_id="install_dbt", bash_command="pip install dbt-bigquery"
    )

    run_dbt_task = BashOperator(
        task_id="run_dbt", bash_command="dbt run -t dev", cwd=DBT_WORKDIR
    )

    (
        pull_dataset_task
        >> extract_and_clean_task
        >> upload_to_gcs_task
        >> create_external_tables_task
        >> create_table_partitions_task
        >> clean_up_task
        >> install_dbt_task
        >> run_dbt_task
    )
