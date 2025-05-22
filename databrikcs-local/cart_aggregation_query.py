# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Cart Items Aggregation
# MAGIC 
# MAGIC This notebook demonstrates how to aggregate cart items for each `cartId` 
# MAGIC to produce a unique list of items that were ever added to the cart.

# COMMAND ----------
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.logger import logger

logger.info("Starting Cart Items Aggregation Script")

question_description = """
Below sample input data contains two columns, one cartId also known as session id, 
and the second column is called items, every time a customer makes a change 
to the cart this is stored as an array in the table, the Marketing team asked 
you to create a unique list of item’s that were ever added to the cart by 
each customer.

Schema: cartId INT, items Array<INT>

Sample Input:
cartId | items
-------|-----------------
1      | [1,100,200,300]
1      | [1,250,300]

Expected Result:
cartId | items
-------|---------------------
1      | [1,100,200,300,250]
"""
logger.info(question_description)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Create Sample DataFrame

# COMMAND ----------
# Sample Data
data = [
    (1, [1, 100, 200, 300]),
    (1, [1, 250, 300])
]

# Schema
schema = StructType([
    StructField("cartId", IntegerType(), True),
    StructField("items", ArrayType(IntegerType()), True)
])

# Create DataFrame
# Ensure SparkSession 'spark' is available in your Databricks environment
df_carts = spark.createDataFrame(data, schema)

# Create a temporary view
df_carts.createOrReplaceTempView("carts")

logger.info("Created temporary view 'carts' with the following data:")
display(df_carts)


# COMMAND ----------
sql_query1 = """
SELECT 
  cartId, 
  collect_set(items) as items
FROM carts 
GROUP BY cartId
"""

logger.info("Executing SQL Query 1:")
logger.info(sql_query1)

result_df1 = spark.sql(sql_query1)

logger.info("Query Result 1:")
display(result_df1)

# COMMAND ----------
sql_query_flatten = """
SELECT 
  cartId, 
  flatten(collect_set(items)) as items
FROM carts 
GROUP BY cartId
"""

logger.info("Executing SQL Query Flatten:")
logger.info(sql_query_flatten)

result_df_flatten = spark.sql(sql_query_flatten)

logger.info("Query Result Flatten:")
display(result_df_flatten)


# COMMAND ----------
sql_query_distinct = """
SELECT 
  cartId, 
  array_distinct(flatten(collect_list(items))) as items
FROM carts
GROUP BY cartId
"""

logger.info("Executing SQL Query Distinct:")
logger.info(sql_query_distinct)

result_df_distinct = spark.sql(sql_query_distinct)

logger.info("Query Result Distinct:")
display(result_df_distinct)

# COMMAND ----------
sql_query_union = """
SELECT
  cartId,
  array_union(collect_set(items), array()) as unique_items_using_union
FROM carts
GROUP BY cartId
"""

logger.info("Executing SQL Query Union:")
logger.info(sql_query_union)

result_df_union = spark.sql(sql_query_union)

logger.info("Query Result Union:")
display(result_df_union)


# COMMAND ----------
sql_query_union = """
SELECT
  cartId,
  array_union(flatten(collect_set(items)), array()) as unique_items_using_union
FROM carts
GROUP BY cartId
"""

logger.info("Executing SQL Query Union:")
logger.info(sql_query_union)

result_df_union = spark.sql(sql_query_union)

logger.info("Query Result Union:")
display(result_df_union)



# COMMAND ----------
data = [(["b", "a", "c"], ["c", "d", "a", "f"]), (["x", "y"], ["z"]), (["a"], None)]
columns = ["array1", "array2"]
df_teste = spark.createDataFrame(data, columns)


display(df_teste.withColumn(
  "union_result", array_union(
    col("array1"), col("array2"))))
# COMMAND ----------
logger.info("Cart Items Aggregation Script Finished")
