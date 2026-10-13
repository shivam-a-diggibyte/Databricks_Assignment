# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///

from pyspark.sql import DataFrame

def read_csv_file(file_path: str) -> DataFrame:
    """
    Read a CSV file with a header into a Spark DataFrame.
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

def write_csv_file(df: DataFrame, output_path: str) -> None:
    """
    Write a Spark DataFrame as CSV with a header.
    """
    (
        df.write
        .format("csv")
        .option("header", True)
        .mode("overwrite")
        .save(output_path)
    )