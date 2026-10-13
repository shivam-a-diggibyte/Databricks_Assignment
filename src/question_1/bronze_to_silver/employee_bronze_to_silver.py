# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ../source_to_bronze/utils

# COMMAND ----------


bronze_path = "/Volumes/workspace/assignment/assignment_files/source_to_bronze"

employee_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{bronze_path}/employee")
)

department_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{bronze_path}/department_df")
)

country_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{bronze_path}/country_df")
)

print("EMPLOYEE COLUMNS")
employee_raw.printSchema()
display(employee_raw)

print("DEPARTMENT COLUMNS")
department_raw.printSchema()
display(department_raw)

print("COUNTRY COLUMNS")
country_raw.printSchema()
display(country_raw)


# COMMAND ----------


from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

employee_schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Salary", DoubleType(), True),
    StructField("Age", IntegerType(), True)
])

department_schema = StructType([
    StructField("DepartmentID", IntegerType(), True),
    StructField("DepartmentName", StringType(), True)
])

country_schema = StructType([
    StructField("CountryCode", StringType(), True),
    StructField("CountryName", StringType(), True)
])


# COMMAND ----------


bronze_path = (
    "/Volumes/workspace/assignment/assignment_files/source_to_bronze"
)

# Method 1: Read employee data using a custom schema
employee_df = (
    spark.read
    .schema(employee_schema)
    .option("header", True)
    .csv(f"{bronze_path}/employee")
)

# Method 2: Read department data using a custom schema
department_df = (
    spark.read
    .format("csv")
    .option("header", True)
    .schema(department_schema)
    .load(f"{bronze_path}/department_df")
)

# Method 3: Read country data using a custom schema
country_df = (
    spark.read
    .option("header", True)
    .schema(country_schema)
    .csv(f"{bronze_path}/country_df")
)

employee_df.printSchema()
display(employee_df)

department_df.printSchema()
display(department_df)

country_df.printSchema()
display(country_df)


# COMMAND ----------


import re

def camel_to_snake(name):
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([A-Z])([A-Z][a-z])", r"\1_\2", name)
    return name.lower()


def rename_columns_to_snake_case(df):
    for old_name in df.columns:
        new_name = camel_to_snake(old_name)
        df = df.withColumnRenamed(old_name, new_name)
    return df


employee_df = rename_columns_to_snake_case(employee_df)
department_df = rename_columns_to_snake_case(department_df)
country_df = rename_columns_to_snake_case(country_df)

employee_df.printSchema()


# COMMAND ----------


from pyspark.sql.functions import current_date

employee_df = employee_df.withColumn(
    "load_date", current_date()
)

department_df = department_df.withColumn(
    "load_date", current_date()
)

country_df = country_df.withColumn(
    "load_date", current_date()
)

display(employee_df)


# COMMAND ----------

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS workspace.employee_info
""")

# COMMAND ----------


(
    employee_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.employee_info.dim_employee")
)

print("Silver employee dimension written successfully.")


# COMMAND ----------

silver_df = spark.table("workspace.employee_info.dim_employee")

silver_df.printSchema()

display(silver_df)

print("Total rows:", silver_df.count())