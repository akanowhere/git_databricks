# Databricks notebook source
from data_platform_pipelines.pipelines.utils.sqs.to_bronze import SqsProcesser
from databricks.sdk.runtime import *


def main():
    tables = eval(dbutils.widgets.get("tables"))
    input_path = dbutils.widgets.get("input")
    outputs = eval(dbutils.widgets.get("outputs"))

    pipeline = SqsProcesser(tables, input_path, outputs)
    pipeline.process_tables()


if __name__ == "__main__":
    main()
