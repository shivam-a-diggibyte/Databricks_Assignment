# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ../source_to_bronze/utils

# COMMAND ----------

silver_df = spark.table("workspace.employee_info.dim_employee")

display(silver_df)
silver_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import sum, current_date

department_salary_df = (
    silver_df
    .groupBy("department")
    .agg(sum("salary").alias("total_salary"))
    .orderBy("total_salary", ascending=False)
)

department_salary_df = department_salary_df.withColumn(
    "at_load_date",
    current_date()
)

display(department_salary_df)

# COMMAND ----------

from pyspark.sql.functions import count, current_date

employee_count_df = (
    silver_df
    .groupBy("department", "country")
    .agg(count("employee_id").alias("employee_count"))
)

employee_count_df = employee_count_df.withColumn(
    "at_load_date",
    current_date()
)

display(employee_count_df)

# COMMAND ----------

from pyspark.sql.functions import current_date

# Read the Department and Country lookup data
department_df = spark.read.option("header", True).csv(
    "/Volumes/workspace/assignment/assignment_files/source_to_bronze/department_df"
)

country_df = spark.read.option("header", True).csv(
    "/Volumes/workspace/assignment/assignment_files/source_to_bronze/country_df"
)

# Join employee data with department names
department_country_df = (
    silver_df
    .join(
        department_df,
        silver_df.department == department_df.DepartmentID,
        "left"
    )
    .join(
        country_df,
        silver_df.country == country_df.CountryCode,
        "left"
    )
    .select(
        department_df.DepartmentName.alias("department_name"),
        country_df.CountryName.alias("country_name")
    )
    .distinct()
    .withColumn("at_load_date", current_date())
)

display(department_country_df)

# COMMAND ----------

from pyspark.sql.functions import avg, current_date

average_age_df = (
    silver_df
    .groupBy("department")
    .agg(avg("age").alias("average_age"))
)

average_age_df = average_age_df.withColumn(
    "at_load_date",
    current_date()
)

display(average_age_df)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.employee_gold")

# COMMAND ----------

(
    department_salary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.employee_gold.department_salary")
)

(
    employee_count_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.employee_gold.employee_count")
)

(
    department_country_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.employee_gold.department_country")
)

(
    average_age_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.employee_gold.average_age")
)

print("All four Gold tables written successfully.")

# COMMAND ----------

display(spark.table("workspace.employee_gold.department_salary"))
display(spark.table("workspace.employee_gold.employee_count"))
display(spark.table("workspace.employee_gold.department_country"))
display(spark.table("workspace.employee_gold.average_age"))

# COMMAND ----------

gold_tables = [
    "department_salary",
    "employee_count",
    "department_country",
    "average_age"
]

for table_name in gold_tables:
    full_table_name = f"workspace.employee_gold.{table_name}"

    print(f"Checking table: {full_table_name}")

    df = spark.table(full_table_name)
    print("Rows:", df.count())
    df.printSchema()
    display(df)

# COMMAND ----------

from datetime import date

today = date.today().isoformat()

(
    department_salary_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"at_load_date = '{today}'")
    .saveAsTable("workspace.employee_gold.department_salary")
)

# COMMAND ----------

from pyspark.sql.functions import current_date

fact_employee_df = silver_df.withColumn(
    "at_load_date",
    current_date()
)

(
    fact_employee_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.employee_gold.fact_employee")
)

print("fact_employee table created successfully.")

# COMMAND ----------

display(spark.table("workspace.employee_gold.fact_employee"))