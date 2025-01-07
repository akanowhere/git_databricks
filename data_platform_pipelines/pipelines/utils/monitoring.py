import logging
import pytz
from databricks.sdk.runtime import *
from datetime import datetime
from time import sleep


def configure_logging():
    logger = spark._jvm.org.apache.log4j
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s - %(asctime)s - %(message)s")


class FileTracking:
    def __init__(self, table: str, env: str, path: str = "") -> None:
        self.path = "s3a://" + path
        self.table = table
        self.env = env
        self.now = (datetime.now(pytz.timezone("America/Sao_Paulo"))
                    .strftime("%Y-%m-%dT%H:%M"))

    def append_saved_path(self, max_no_tries) -> None:
        query = f"""
            INSERT INTO
                monitoring.file_tracking.sqs_processing_status_v2
            VALUES(
                '{self.table}',
                '{self.path}',
                'FALSE',
                '{self.env}',
                '{self.now}'
            )
        """

        logging.info(f"Query: {query}")
        tries = 0
        while tries < max_no_tries:  
            try:
                spark.sql(query)
                break
            except Exception as e:
                print(f'Retry no. "{tries}" to update monitoring table saved status and path. Error: {e}')
                sleep(10)
                tries +=1

    def get_paths_to_read(self, max_no_tries) -> str:
        query = f"""
            SELECT DISTINCT
                path
                , readed
            FROM
                monitoring.file_tracking.sqs_processing_status_v2
            WHERE
                queue = '{self.table}'
                AND env = '{self.env}'
        """
        logging.info(f"Query: {query}")
        tries = 0
        while tries < max_no_tries:  
            try:
                result = spark.sql(query)
                break
            except Exception as e:
                print(f'Retry no. "{tries}" to update monitoring table readed status. Error: {e}')
                sleep(10)
                tries +=1
                
        return result
         

    def append_readed_path(self, max_no_tries) -> None:
        query = f"""
            INSERT INTO
                monitoring.file_tracking.sqs_processing_status_v2
            VALUES(
                '{self.table}',
                '{self.path}',
                'TRUE',
                '{self.env}',
                '{self.now}'
            )          
        """
        logging.info(f"Query: {query}")
        tries = 0
        while tries < max_no_tries:  
            try:
                spark.sql(query)
                break
            except Exception as e:
                print(f'Retry no. "{tries}" to update monitoring table readed status. Error: {e}')
                sleep(10)
                tries +=1
