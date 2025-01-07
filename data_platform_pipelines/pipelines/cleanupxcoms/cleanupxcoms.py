# Databricks notebook source
import logging
from datetime import datetime, timedelta
from airflow import settings
from airflow.models import XCom
from airflow.utils.db import provide_session

def cleanup_xcoms(**kwargs):
    """Remove entradas do XCom mais antigas que 20 dias."""
    max_age = 20  # dias
    cutoff_date = datetime.now() - timedelta(days=max_age)

    with provide_session() as session:
        try:
            deleted_count = session.query(XCom).filter(XCom.execution_date < cutoff_date).delete(synchronize_session=False)
            session.commit()
            logging.info(f"Removidos {deleted_count} XComs mais antigos que {cutoff_date}.")
        except Exception as e:
            logging.error(f"Erro ao deletar XComs: {e}")
            session.rollback()
