import io
import zipfile
from datetime import datetime, timedelta
from typing import List, Union

import paramiko
from databricks.sdk.runtime import *  # type: ignore
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StructType


class InfobipCollector:
    def __init__(self, **credentials) -> None:
        self.host = credentials["host"]
        self.port = credentials["port"]
        self.username = credentials["username"]
        self.password = credentials["password"]
        self.ssh_client = None
        self.ftp = None

    def _open_connection(self):
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(
            hostname=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            timeout=10,
        )
        print("connection established successfully")
        self.ftp = self.ssh_client.open_sftp()

    def _close_connection(self):
        if self.ftp:
            self.ftp.close()
        if self.ssh_client:
            self.ssh_client.close()
        print("Connection closed successfully.")

    def get_zip_files(
        self,
        n_days: Union[int, None],
        extension: str = ".zip",
        incremental: bool = True,
    ) -> List[str]:
        if incremental and n_days is not None:
            target_date = (datetime.now() - timedelta(days=n_days)).strftime(
                "%d-%m-%Y"
            )
            current_date = datetime.now().strftime("%d-%m-%Y")
            remote_files = [
                file
                for file in self.ftp.listdir()
                if file.endswith(extension)
                and target_date <= file.split("_")[0] <= current_date
            ]
        else:
            remote_files = [
                file for file in self.ftp.listdir() if file.endswith(extension)
            ]

        return remote_files

    def extract_csv_from_zip(
        self,
        remote_file: str,
        prefix: str,
        keyword: str,
        extension: str = ".csv",
    ) -> List[io.StringIO]:
        with self.ftp.file(
            remote_file, mode="rb"
        ) as remote_zip_file:  # 'rb' for reading binary
            zip_file_bytes = (
                remote_zip_file.read()
            )  # read the entire zip file into memory

        # use zipfile to handle the zip file from memory
        with zipfile.ZipFile(io.BytesIO(zip_file_bytes)) as zip_file:
            # get all files in the zip according to the rules specified
            files = [
                file
                for file in zip_file.namelist()
                if file.startswith(prefix)
                and file.endswith(extension)
                and (
                    (keyword in file.lower() and keyword in prefix)
                    or (keyword not in file.lower() and keyword not in prefix)
                )
            ]

            # transform the CSV files into a Spark DataFrame
            csv_files = []
            for file in files:
                with zip_file.open(file) as f:
                    # read the CSV file into a bytes
                    csv_bytes = f.read()

                    # Convert bytes to StringIO for later processing
                    csv_io = io.StringIO(csv_bytes.decode("utf-8"))
                    csv_files.append(csv_io)

            return csv_files

    def transform_csv_to_df(self, csv_file: io.StringIO) -> DataFrame:
        # split the CSV content into rows and extract the header
        data = csv_file.read().splitlines()
        data = [
            row.split(",") for row in data
        ]  # adjust delimiter if necessary

        # extract the header and rows from the CSV file
        header = [column_name.replace(" ", "_") for column_name in data[0]]
        rows = [row for row in data[1:] if len(row) == len(header)]

        # create the schema for the DataFrame
        schema = StructType([
            StructField(col, StringType(), True) for col in header
        ])

        # create the DataFrame
        df = spark.createDataFrame(rows, schema)  # type: ignore

        return df

    def start(
        self,
        prefix: str,
        keyword: str,
        n_days: Union[int, None],
        incremental: bool = True,
    ) -> Union[DataFrame, None]:
        try:
            # Open connection to SFTP server
            self._open_connection()

            # Get the relevant ZIP files from the last N days
            remote_files = self.get_zip_files(n_days, incremental=incremental)

            # Extract the CSV files from the ZIP files and transform them into Spark DataFrames
            dataframes = []
            for remote_file in remote_files:
                file_generation_date = datetime.strptime(
                    remote_file.split("_")[0], "%d-%m-%Y"
                )
                csv_files = self.extract_csv_from_zip(
                    remote_file, prefix=prefix, keyword=keyword
                )
                for csv_file in csv_files:
                    df = self.transform_csv_to_df(csv_file)
                    df = df.withColumn(
                        "_file_generation_date", lit(file_generation_date)
                    )
                    dataframes.append(df)

            # Combine the dataframes into a single DataFrame
            if dataframes:
                combined_df = dataframes[0]
                for df in dataframes[1:]:
                    combined_df = combined_df.union(df)
                return combined_df
            else:
                print("No CSV files found.")
                return None
        finally:
            # Close connection to SFTP server
            self._close_connection()
