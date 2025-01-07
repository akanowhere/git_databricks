import io
from datetime import datetime, timedelta
from typing import List, Union

import chardet, paramiko
from databricks.sdk.runtime import *  # type: ignore
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StructType


class YTPremiumCollector:
    def __init__(self, **credentials) -> None:
        self.host = credentials["host"]
        self.port = credentials["port"]
        self.username = credentials["username"]
        self.password = credentials["password"]
        self.ssh_client = None
        self.ftp = None

    def _open_connection(self, specific_path: str = None):
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
        if specific_path:
            self.ftp.chdir(specific_path)
            print("Mudou para:", self.ftp.getcwd())

    def _close_connection(self):
        if self.ftp:
            self.ftp.close()
        if self.ssh_client:
            self.ssh_client.close()
        print("Connection closed successfully.")

    def get_files(
        self,
        prefix: str,
        n_days: Union[int, None] = None,
        extension: str = ".csv",
        incremental: bool = True,
    ) -> List[str]:
        if incremental and n_days is not None:
            target_date = (datetime.now() - timedelta(days=n_days)).strftime("%Y%m%d")
            current_date = datetime.now().strftime("%Y%m%d")
            remote_files = [
                file
                for file in self.ftp.listdir()
                if prefix in file
                and file.endswith(extension)
                and target_date <= file.split(prefix)[1].replace(extension, "") <= current_date
            ]
        else:
            remote_files = [
                file
                for file in self.ftp.listdir()
                if prefix in file and file.endswith(extension)
            ]

        return remote_files

    def transform_csv_to_df(self, csv_file: io.StringIO) -> DataFrame:
        # split the CSV content into rows and extract the header
        data = csv_file.read().splitlines()
        data = [row.split(",") for row in data]  # adjust delimiter if necessary

        # extract the header and rows from the CSV file
        header = data[0]
        rows = [row for row in data[1:] if len(row) == len(header)]

        # create the schema for the DataFrame
        schema = StructType([StructField(col, StringType(), True) for col in header])

        # create the DataFrame
        df = spark.createDataFrame(rows, schema)  # type: ignore

        return df

    def start(
        self,
        prefix: str,
        specific_path: str,
        n_days: Union[int, None],
        extension: str = ".csv",
        incremental: bool = True,
    ) -> Union[DataFrame, None]:
        try:
            # Open connection to SFTP server
            self._open_connection(specific_path=specific_path)

            # Get the CSV files
            remote_files = self.get_files(prefix, n_days, incremental=incremental)
            print(remote_files)

            # Transform CSV file into Spark DataFrames
            dataframes = []
            for file in remote_files:
                with self.ftp.open(file, "r") as f:
                    file_generation_date = datetime.strptime(file.split(prefix)[1].replace(extension, ""), "%Y%m%d").date()

                    # Detect the encoding of the file
                    file_sample = f.read(10000)
                    encoding = chardet.detect(file_sample)["encoding"]
                    f.seek(0)  # Reset the cursor to the beginning of the file

                    # Decode and convert the CSV content into a StringIO object
                    csv_content = io.StringIO(f.read().decode(encoding))

                    # Transform the CSV file into a Spark DataFrame
                    df = self.transform_csv_to_df(csv_content)
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
