# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./utils

# COMMAND ----------

base_path = "/Volumes/workspace/assignment/assignment_files"

employee = read_csv_file(f"{base_path}/Employee-Q1.csv")
department_df = read_csv_file(f"{base_path}/Department-Q1.csv")
country_df = read_csv_file(f"{base_path}/Country-Q1.csv")

print("Employee Data")
display(employee)

print("Department Data")
display(department_df)

print("Country Data")
display(country_df)

# COMMAND ----------


bronze_path = "/Volumes/workspace/assignment/assignment_files/source_to_bronze"

write_csv_file(employee, f"{bronze_path}/employee")
write_csv_file(department_df, f"{bronze_path}/department_df")
write_csv_file(country_df, f"{bronze_path}/country_df")

print("All three datasets written to Bronze storage.")


# COMMAND ----------

display(dbutils.fs.ls(
    "/Volumes/workspace/assignment/assignment_files/source_to_bronze/"
))


# COMMAND ----------

