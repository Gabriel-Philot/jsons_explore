# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Cart Items Unique List
# MAGIC 
# MAGIC This notebook demonstrates how to create a unique list of items that were added to the cart by each customer.

# COMMAND ----------
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.logger import logger

logger.info("Starting Cart Items Unique List Script")

question_description = """
You are currently looking at a table that contains data 
from an e-commerce platform, each row contains a list 
of items(Item number) that were present in the cart, 
when the customer makes a change to the cart the entire 
information is saved as a separate list and appended to an 
existing list for the duration of the customer session, 
to identify all the items customer bought you have to make a 
unique list of items, you were asked to create a unique 
item's list that was added to the cart by the user.

Schema: cartId INT, items Array<Array<INT>>

Sample Data:
cartId | items
-------|-----------------
1      | [[1,100,200,300], [1,250,300]]
2      | [[10,150,200,300], [1,210,300],[350]]

Expected Result:
cartId | items
-------|---------------------
1      | [1,100,200,300,250]
2      | [10,150,200,300,210,350]
"""
logger.info(question_description)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Create Sample DataFrame

# COMMAND ----------
# Sample Data
data = [
    (1, [[1, 100, 200, 300], [1, 250, 300]]),
    (2, [[10, 150, 200, 300], [1, 210, 300], [350]])
]

# Schema
schema = StructType([
    StructField("cartId", IntegerType(), True),
    StructField("items", ArrayType(ArrayType(IntegerType())), True)
])

# Create DataFrame
# Ensure SparkSession 'spark' is available in your Databricks environment
df_carts = spark.createDataFrame(data, schema)

# Create a temporary view
df_carts.createOrReplaceTempView("carts")

logger.info("Created temporary view 'carts' with the following data:")
display(df_carts)

# COMMAND ----------
# The solution query
sql_query_flatten = """
SELECT cartId, 
    flatten(items)
FROM carts
"""

logger.info("Executing SQL Query Flatten:")
logger.info(sql_query_flatten)

result_df_flatten = spark.sql(sql_query_flatten)

logger.info("Query Result Flatten:")
display(result_df_flatten)



# COMMAND ----------
# The solution query
sql_query_distinct = """
SELECT cartId, 
    array_distinct(flatten(items))
FROM carts
"""

logger.info("Executing SQL Query Distinct:")
logger.info(sql_query_distinct)

result_df_distinct = spark.sql(sql_query_distinct)

logger.info("Query Result Distinct:")
display(result_df_distinct)


# COMMAND ----------
sql_query_collect_set = """
SELECT cartId, 
    collect_set(items)
FROM carts
GROUP BY cartId
"""

logger.info("Executing SQL Query Collect Set:")
logger.info(sql_query_collect_set)

result_df_collect_set = spark.sql(sql_query_collect_set)

logger.info("Query Result Collect Set:")
display(result_df_collect_set)


# COMMAND ----------
sql_query_collect_list = """
SELECT cartId, 
    collect_list(items)
FROM carts
GROUP BY cartId
"""

logger.info("Executing SQL Query Collect List:")
logger.info(sql_query_collect_list)

result_df_collect_list = spark.sql(sql_query_collect_list)

logger.info("Query Result Collect List:")
display(result_df_collect_list)



# COMMAND ----------
logger.info("Cart Items Unique List Script Finished")
