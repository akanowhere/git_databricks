# Databricks notebook source
from data_platform_pipelines.pipelines.utils.datalake import DataLake
import logging

# Configuração dos logs
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

if __name__ == "__main__":

    # Obtendo widgets do Databricks
    widgets = DataLake.get_cleanup_widgets_from_databricks()

    retention_hours = widgets.get("retention_hours")
    
    for catalog in widgets["catalogs"]:
        # Obtendo a lista de tabelas por schema
        catalog_query = f"""
        SELECT 
            table_catalog, table_schema, table_name 
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog}'
        and table_schema not in ('information_schema', 'hive_metastore')
        """
        
        result = spark.sql(catalog_query)
        rows = result.collect()
        table_path_list = []
        for row in rows:
            table_path_list.append(f"{row['table_catalog']}.{row['table_schema']}.{row['table_name']}")
            
        # Iterando sobre as tabelas para processamento
        for table_path in table_path_list:
            try:
                # Executa Optimize na tabela delta
                num_files_added, num_files_removed, total_size = DataLake.optimize_table(
                    table_path=table_path
                )
                print(f'{table_path} - Optimized {num_files_removed} files into {num_files_added} files. Size of files that became obsolete: {total_size} bytes.')
                
                # Executa Vacuum na tabela delta
                removed_files = DataLake.vacuum_table(
                    table_path=table_path,
                    retention_hours=retention_hours
                )

                logging.info(f"A tabela {catalog}.{table_path} sofreu VACUUM de {retention_hours}h e removeu {removed_files} arquivos.")
            except Exception as e:
                logging.error(f"SKIPPING - Tabela {catalog}.{table_path} falhou com erro: {e}")