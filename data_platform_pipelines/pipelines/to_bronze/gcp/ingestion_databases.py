# Databricks notebook source
from data_platform_pipelines.pipelines.utils.datalake import DataLake
import logging

# Configuração dos logs
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

if __name__ == "__main__":

    # Obtendo widgets do Databricks
    widgets = DataLake.get_bronze_widgets_from_databricks()

    # Iterando sobre os esquemas e tabelas para processamento
    for schema, tables in widgets["schemas"].items():

        for table in tables:

            aux = table.popitem()
            mode = aux[1]
            table = aux[0]

            target_table = widgets["outputs"]["s3"].format(
                    origin=widgets["db_name"],
                    schema=schema,
                    table=table
                )

            source = widgets["input_path"].format(
                    origin=widgets["db_name"],
                    schema=schema,
                    table=table
                )

            catalog = widgets["outputs"]["unity_catalog"]["catalog"]

            # Copia dados para a tabela delta
            copy = DataLake.copy_data_to_delta_table(
                catalog=catalog,
                schema=schema,
                table=table,
                target_table=target_table,
                source=source,
                mode=mode
                )

            logging.info(f"Caminho da tabela na camada bronze no s3: {target_table}")
            logging.info(f"A tabela {catalog}.{schema}.{table} sofreu {mode}")
