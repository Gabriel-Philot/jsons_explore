# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Schema and Table Creation Script

# COMMAND ----------
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.logger import logger

logger.info("Starting Question 01")

question01 = """Below table temp_data has 
one column called raw contains JSON data that records temperature 
for every four hours in the day for the city of Chicago, 
you are asked to calculate the maximum temperature that was ever 
recorded for 12:00 PM hour across all the days. 
Parse the JSON data and use the necessary array function to calculate the max temp.

Table: temp_date
Column: raw
Datatype: string

"""
logger.info(question01)

json_data = """
                {
                    "chicago": [
                                    {
                                        "date": "01-01-2021",
                                        "temp": [25, 28, 45, 56, 39, 25]
                                    },
                                    {
                                        "date": "01-02-2021",
                                        "temp": [25, 28, 49, 54, 38, 25]
                                    },
                                    {
                                        "date": "01-03-2021",
                                        "temp": [25, 28, 49, 58, 38, 25]
                                    }
                               ]
                }
            """

# COMMAND ----------
data = [(json_data,)]

col = StructType([StructField('raw', StringType(), True)])

df_raw = spark.createDataFrame(data, col)

df_raw.createOrReplaceTempView("temp_data")

sql_query_0 = "SELECT raw:chicago[*] FROM temp_data"

result_df = spark.sql(sql_query_0)
display(result_df)
# COMMAND ----------
sql_query_1 = "SELECT raw:chicago[*].temp[3] FROM temp_data"

result_df_1 = spark.sql(sql_query_1)
display(result_df_1)

# COMMAND ----------
sql_query_2 = "SELECT from_json(raw:chicago[*].temp[3], 'array<int>') FROM temp_data"

result_df_2 = spark.sql(sql_query_2)
display(result_df_2)

# COMMAND ----------
sql_query_3 = """
SELECT array_max(from_json(raw:chicago[*].temp[3], 'array<int>')) 
FROM temp_data
"""
result_df_3 = spark.sql(sql_query_3)
display(result_df_3)



# COMMAND ----------
