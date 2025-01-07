import json
import logging
import pandas as pd
import pytz
import time
import threading
from datetime import datetime, timedelta
from functools import partial
from data_platform_pipelines.pipelines.utils.monitoring import FileTracking


class S3Saver:
    """
    Responsável por salvar dados no S3.
    """

    def __init__(self, s3_client, bucket_name, folder_name, file_name_prefix):
        """
        Inicializa a classe S3Saver.

        Args:
            s3_client (boto3.client): Cliente S3.
            bucket_name (str): Nome do bucket S3.
            folder_name (str): Nome da pasta no S3.
            file_name_prefix (str): Prefixo do nome do arquivo.
        """
        self.s3 = s3_client
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.file_name_prefix = file_name_prefix
        self.messages_saved_counter = 0

    def save_parquet_to_s3(self, df):
        """
        Salva o DataFrame em formato Parquet no S3.

        Args:
            df (pandas.DataFrame): DataFrame a ser salvo.
        """
        file_now = datetime.now(
            pytz.timezone("America/Sao_Paulo")
            ).strftime("%Y-%m-%dT%H:%M")
        logging.info("Início do save: "
                     f"{datetime.now(pytz.timezone('America/Sao_Paulo'))}")

        parquet_object = df.to_parquet(index=False)

        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=f"{self.file_name_prefix}{self.folder_name}/{file_now}.parquet",
            Body=parquet_object
        )

        self.messages_saved_counter += len(df)
        logging.info("Fim do save: "
                     f"{datetime.now(pytz.timezone('America/Sao_Paulo'))}")


class SqsProcessor:
    """
    Responsável por processar mensagens do SQS.
    """
    def __init__(
        self,
        sqs_client,
        s3_client,
        queue_url,
        bucket_name,
        file_name,
        table,
        max_len_df=100_000
    ) -> None:
        """
        Inicializa a classe SqsProcessor.

        Args:
            sqs_client (boto3.client): Cliente SQS.
            s3_client (boto3.client): Cliente S3.
            queue_url (str): URL da fila SQS.
            bucket_name (str): Nome do bucket S3.
            file_name (str): Nome do arquivo no S3.
            table (str): Nome da tabela.
            max_len_df (int): Tamanho máximo do DataFrame antes de salvar no S3.
                Default é 100_000.
        """
        self.sqs = sqs_client
        self.queue_url = queue_url
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.table = table
        self.max_len_df = max_len_df
        self.df = pd.DataFrame()
        self.messages_received_counter = 0
        self.messages_deleted_counter = 0
        self.breaking_condition = ''
        self.messages_written = False
        self.saved_parquet_flag = False
        self.folder_now = (datetime.now(pytz.timezone("America/Sao_Paulo"))
                           .strftime("%Y-%m-%dT%H:%M"))
        self.s3_saver = S3Saver(s3_client,
                                bucket_name,
                                self.folder_now,
                                file_name)

    @staticmethod
    def chunkify(lst, n):
        """
        Divide uma lista em n partes.

        Args:
            lst (list): Lista a ser dividida.
            n (int): Número de partes.

        Returns:
            list: Lista de listas divididas.
        """
        return [lst[i::n] for i in range(n)]

    def delete_messages_from_sqs(self, list_receipt_handle):
        """
        Deleta mensagens do SQS.

        Args:
            list_receipt_handle (list): Lista de ReceiptHandles das mensagens a
            serem deletadas.
        """
        for receipthandle in list_receipt_handle:
            self.sqs.delete_message(QueueUrl=self.queue_url,
                                    ReceiptHandle=receipthandle)
            self.messages_deleted_counter += 1
        logging.info(f"{len(list_receipt_handle)} mensagens deletadas do SQS!")

    def process_messages(self):
        """
        Processa mensagens do SQS e salva no S3.
        """
        logging.info(f"Solicitando a mensagem da {self.table}")
        while True:
            message_counter = 0
            start = datetime.now()
            self.saved_parquet_flag = False
            while True:
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=10,
                    VisibilityTimeout=20*60,
                    WaitTimeSeconds=20,
                )

                if "Messages" not in response:
                    self.breaking_condition='zero_messages'
                    self._process_remaining_messages()
                    if not self._has_more_messages():
                        break
                    else:
                        continue
                
                new_messages_read_number = self._process_received_response(response)
                message_counter += new_messages_read_number
                end = datetime.now()
                if end - start >= timedelta(seconds=60):
                    break
                
            if message_counter < 1000 and self.saved_parquet_flag == False:
                self._process_remaining_messages()
                if self.messages_written:
                    FileTracking(
                        path=f"{self.bucket_name}/{self.file_name}{self.folder_now}",
                        table=self.table,
                        env="dev" if "-dev-" in self.bucket_name else "prd"
                    ).append_saved_path(max_no_tries = 15)
                break
        
        if self.breaking_condition == '':
            self.breaking_condition = 'low_throughput'

    def _process_remaining_messages(self):
        """
        Processa mensagens restantes no DataFrame.
        """
        if len(self.df) > 0:
            self.s3_saver.save_parquet_to_s3(self.df)
            self._delete_messages_in_chunks(self.df["ReceiptHandle"])
            self.df = pd.DataFrame()
            self.messages_written = True
            self.saved_parquet_flag = True

    def _has_more_messages(self):
        """
        Verifica se há mais mensagens na fila SQS.

        Returns:
            bool: True se há mais mensagens, False caso contrário.
        """
        attributes = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=['ApproximateNumberOfMessages',
                            'ApproximateNumberOfMessagesNotVisible']
        )
        approximate_number_of_messages = int(
            attributes["Attributes"]["ApproximateNumberOfMessages"]
            )
        approximate_number_of_messages_not_visible = int(
            attributes["Attributes"]["ApproximateNumberOfMessagesNotVisible"]
            )


        if approximate_number_of_messages > 0:
            logging.warning(f"Ainda existem {approximate_number_of_messages} "
                            f"mensagens disponíveis na fila de {self.table}")
            time.sleep(30)
            return True
        
        if approximate_number_of_messages_not_visible > 0:
            logging.warning(
                f"Ainda existem {approximate_number_of_messages_not_visible} "
                f"mensagens em trânsito na fila {self.table}"
                )
            time.sleep(30)
            return True
        else:
            logging.info(f"Não houveram mensagens na última solicitação da "
                         f"fila {self.table}")
            return False

    def _process_received_response(self, response):
        """
        Processa mensagens recebidas do SQS.

        Args:
            response (dict): Resposta da SQS contendo as mensagens.
        """
        i_invalid_messages = []
        messages = response["Messages"]

        for i in range(len(messages)):
            try:
                messages[i]["Body"] = json.loads(messages[i]["Body"])
            except Exception as e:
                i_invalid_messages.append(i)
                logging.warning(f"A mensagem a seguir voltará para a fila:"
                                f"\n{messages[i]['MessageId']}\n{e}")

            messages[i]["Body"] = json.dumps(messages[i]["Body"])
            self.messages_received_counter += 1

        for n in i_invalid_messages:
            response["Messages"].pop(n)

        response["ResponseMetadata"]["HTTPHeaders"] = json.dumps(
            response["ResponseMetadata"]["HTTPHeaders"]
        )

        df_messages = pd.DataFrame(response["Messages"])
        df_response_metadata = pd.DataFrame([response["ResponseMetadata"]],
                                            index=[0])
        df_combined = df_messages.merge(df_response_metadata, how="cross")

        logging.info(f"Início do concat: {len(self.df)} mensagens no df final")
        self.df = pd.concat([self.df, df_combined])
        logging.info(f"Fim do concat: {len(self.df)} mensagens no df final")

        if len(self.df) >= self.max_len_df:
            self.s3_saver.save_parquet_to_s3(self.df)
            self._delete_messages_in_chunks(self.df["ReceiptHandle"])
            self.df = pd.DataFrame()
            self.saved_parquet_flag = True
        
        new_messages_read_number = len(df_combined)
        return new_messages_read_number

    def _delete_messages_in_chunks(self, receipt_handles):
        """
        Deleta mensagens em chunks do SQS.

        Args:
            receipt_handles (list): Lista de ReceiptHandles das mensagens a
            serem deletadas.
        """
        chunks = self.chunkify(receipt_handles, 30)
        processing_threads = []

        for chunk in chunks:
            t = threading.Thread(target=partial(self.delete_messages_from_sqs,
                                                chunk))
            t.start()
            processing_threads.append(t)

        for t in processing_threads:
            t.join()
