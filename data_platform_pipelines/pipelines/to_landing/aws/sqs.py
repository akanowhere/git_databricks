# Databricks notebook source
import logging
from data_platform_pipelines.pipelines.utils.conn.aws import AwsSession
from data_platform_pipelines.pipelines.utils.sqs.to_landing import SqsProcessor
from data_platform_pipelines.pipelines.utils.monitoring import configure_logging
from databricks.sdk.runtime import *


def main():
    """
    Função principal que configura e executa o processamento.
    """
    configure_logging()

    # TODO: como usar POO pra isso ser dinâmico para todos os processamentos?
    queues = eval(dbutils.widgets.get("queues"))
    output = dbutils.widgets.get("output")

    aws_manager = AwsSession()
    sqs = aws_manager.get_client_to(service="sqs", max_pool_connections=1000)
    s3 = aws_manager.get_client_to(service="s3")

    for queue in queues:
        table = queue.split("-")[-1]

        bucket_name = output.split("/")[2]
        file_name = "/".join(output.split("/")[3:]).format(origin="sqs",
                                                           schema="sydle",
                                                           table=table)
        logging.info(f"Lendo a fila {table}")

        processor = SqsProcessor(
            sqs,
            s3,
            queue,
            bucket_name,
            file_name,
            table
        )
        processor.process_messages()
        
        logging.info(f'Fila {table}. Processo finalizado pelo motivo: {processor.breaking_condition}')
        logging.info(f"Fila {table}. Mensagens recebidas: {processor.messages_received_counter}, "
                    f"Mensagens salvas: {processor.s3_saver.messages_saved_counter}, "
                    f"Mensagens deletadas: {processor.messages_deleted_counter}")


if __name__ == "__main__":
    main()
