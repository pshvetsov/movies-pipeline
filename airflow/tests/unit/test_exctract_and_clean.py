import filecmp
import logging
import os
import shutil
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from dags.src.data_config import DataConfig as Data
from dags.src.extract_and_clean import extract_and_clean

logging.basicConfig(
    level=logging.INFO,
    filename="testing.log",
    filemode="w",
    format="%(levelname)s:%(name)s:%(asctime)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("").addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger(__name__)


class TestExtractAndClean(unittest.TestCase):

    def setUp(self):
        self.path = "/opt/airflow/tests/fixtures/"
        self.results_path = "/opt/airflow/tests/fixtures/res/"

        self.tearDown()

        os.makedirs(self.results_path)
        logger.info(f"Fresh directory {self.results_path} was created.")

    def tearDown(self):
        if os.path.exists(self.results_path):
            shutil.rmtree(self.results_path)
            logger.info(f"Existing folder {self.results_path} was deleted.")

    def test_extract_and_clean(self):
        extract_and_clean(self.path, self.results_path)

        for file in Data.FILE_DATA:
            pqt_filename = file["name"].replace(".csv", ".parquet")
            reference_file = os.path.join(self.path, pqt_filename)
            tested_file = os.path.join(self.results_path, pqt_filename)

            # Check if parquet file exists
            self.assertTrue(
                os.path.exists(tested_file), f"File {tested_file} does not exist."
            )

            # Check if parquet files match
            self.assertTrue(
                filecmp.cmp(tested_file, reference_file, shallow=False),
                f"Files {pqt_filename} do not match",
            )


if __name__ == "__main__":
    unittest.main()
