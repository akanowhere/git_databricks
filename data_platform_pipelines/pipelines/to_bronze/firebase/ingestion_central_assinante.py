# Databricks notebook source

import logging

from data_platform_pipelines.pipelines.utils.datalake import DataLake

# Configuração dos logs
logger = spark._jvm.org.apache.log4j  # type: ignore
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def main():
    # Obtendo widgets do Databricks
    widgets = DataLake.get_bronze_widgets_from_databricks_v2()

    for schema_name, tables_info in widgets["schemas"].items():
        for table_name, about_ingestion in tables_info.items():
            target_table = widgets["outputs"]["s3"].format(
                source_type=widgets["source_type"],
                data_source=widgets["data_source"],
                schema=schema_name,
                table=table_name,
            )

            source = widgets["input_path"].format(
                source_type=widgets["source_type"],
                data_source=widgets["data_source"],
                schema=schema_name,
                table=table_name,
            )

            catalog = widgets["outputs"]["unity_catalog"]["catalog"]

            # Copia dados para a tabela delta
            DataLake.copy_data_to_delta_table(
                catalog=catalog,
                schema=schema_name,
                table=table_name,
                target_table=target_table,
                source=source,
                file_format="csv",
                mode=["append"],
            )

            logging.info(
                f"Caminho da tabela na camada bronze no s3: {target_table}"
            )
            logging.info(
                f"A tabela {catalog}.{schema_name}.{table_name} sofreu atualização"
            )


if __name__ == "__main__":
    main()
