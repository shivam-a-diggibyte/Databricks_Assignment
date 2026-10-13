# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import requests

url = "https://reqres.in/api/users"
response = requests.get(
    url,
    params={"page": 2},
    timeout=30
)
print("Status code:", response.status_code)
print(response.text)

# COMMAND ----------

import json

response_json = response.json()

print(json.dumps(response_json, indent=4))

# COMMAND ----------

import requests

url = "https://reqres.in/api/users"

all_users = []
page = 1

while True:
    response = requests.get(
        url,
        params={"page": page}
    )

    response.raise_for_status()
    response_json = response.json()

    if not response_json["data"]:
        break

    all_users.extend(response_json["data"])
    page += 1

print("Total users fetched:", len(all_users))

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

user_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

users_df = spark.createDataFrame(
    all_users,
    schema=user_schema
)

users_df.printSchema()
display(users_df)

# COMMAND ----------

print(users_df.columns)
users_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit, current_date

users_df = (
    users_df
    .withColumn("site_address", lit("reqres.in"))
    .withColumn("load_date", current_date())
)

display(users_df)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.site_info")

(
    users_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.site_info.person_info")
)

# COMMAND ----------

person_df = spark.table("workspace.site_info.person_info")

person_df.printSchema()
display(person_df)

print("Total rows:", person_df.count())