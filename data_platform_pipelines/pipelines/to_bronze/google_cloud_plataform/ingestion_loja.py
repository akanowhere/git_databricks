# Databricks notebook source
import logging
from data_platform_pipelines.pipelines.utils.datalake import DataLake
from pyspark.sql import DataFrame
from io import StringIO

# Configuração dos logs
logger = spark._jvm.org.apache.log4j  # type: ignore
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# Função para limpar os nomes das colunas
def clean_column_names(df: DataFrame) -> DataFrame:
    invalid_chars = [' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '=']
    for col_name in df.columns:
        clean_name = col_name
        for char in invalid_chars:
            clean_name = clean_name.replace(char, '_')  # Substitui por underscore
        # Remover underscores duplicados e possíveis underscores no início/fim
        clean_name = '_'.join(filter(None, clean_name.split('_')))
        df = df.withColumnRenamed(col_name, clean_name)
    return df

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

            # Carregar os dados do DataFrame Spark com encoding UTF-8
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("encoding", "utf-8") \
                .load(source)

            # Limpa os nomes das colunas antes de salvar os dados
            df_cleaned = clean_column_names(df)

            # Log das colunas após limpeza
            logging.info(f"Colunas após limpeza para {schema_name}.{table_name}: {df_cleaned.columns}")

            # Verificação extra: certifique-se de que não há caracteres inválidos nas colunas
            invalid_chars = set(' ,;{}()\n\t=')
            for col in df_cleaned.columns:
                if any(char in col for char in invalid_chars):
                    raise ValueError(f"Coluna '{col}' contém caracteres inválidos.")

            # Salva o DataFrame limpo em um caminho temporário no S3
            temp_path = f"s3://plataforma-dados-alloha-bronze-dev-637423315513/google_cloud_sotarage/pre_processing/"
            df_cleaned.write.format("json").mode("overwrite").save(temp_path)

            # Chama a função para copiar os dados para a tabela Delta
            DataLake.copy_data_to_delta_table(
                catalog=catalog,
                schema=schema_name,
                table=table_name,
                target_table=target_table,
                source=temp_path,  # Usa o caminho temporário
                file_format="delta",
                mode="overwrite"  # Utilize "append" ou "overwrite" conforme necessário
            )

            logging.info(f"Caminho da tabela na camada bronze no s3: {target_table}")
            logging.info(f"A tabela {catalog}.{schema_name}.{table_name} sofreu atualização")

if __name__ == "__main__":
    main()
