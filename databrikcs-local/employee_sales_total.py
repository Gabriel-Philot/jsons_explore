# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Employee Sales Calculation Script

# COMMAND ----------
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.logger import logger

logger.info("Starting Employee Sales Question")

question = """You are trying to calculate total sales made by all the employees by parsing a complex struct data type that stores employee and sales data, how would you approach this in SQL

Table definition,

batchId INT, performance ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>, insertDate TIMESTAMP

Sample data of performance column

Calculate total sales made by all the employees?
"""
logger.info(question)

# COMMAND ----------
# Create the sample data as provided in the question
spark.sql("""
create or replace table sales as 
select 1 as batchId,
    from_json('[{ "employeeId":1234,"sales" : 10000 },{ "employeeId":3232,"sales" : 30000 }]',
         'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
  current_timestamp() as insertDate
union all 
select 2 as batchId,
  from_json('[{ "employeeId":1235,"sales" : 10500 },{ "employeeId":3233,"sales" : 32000 }]',
                'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
                current_timestamp() as insertDate
""")

# COMMAND ----------
# Display the sample data
sql_query_0 = "SELECT * FROM sales"
result_df_0 = spark.sql(sql_query_0)
display(result_df_0)

# COMMAND ----------
# First approach: Explode the array to get individual sales records
sql_query_1 = """
SELECT batchId, explode(performance) as employee_sales, insertDate
FROM sales
"""
result_df_1 = spark.sql(sql_query_1)
display(result_df_1)

# COMMAND ----------
# Extract employee ID and sales from the struct
sql_query_2 = """
SELECT 
  batchId, 
  employee_sales.employeeId,
  employee_sales.sales,
  insertDate
FROM (
  SELECT batchId, explode(performance) as employee_sales, insertDate
  FROM sales
)
"""
result_df_2 = spark.sql(sql_query_2)
display(result_df_2)

# COMMAND ----------
# Calculate the total sales across all employees
sql_query_3 = """
SELECT SUM(employee_sales.sales) as total_sales
FROM (
  SELECT batchId, explode(performance) as employee_sales, insertDate
  FROM sales
)
"""
result_df_3 = spark.sql(sql_query_3)
display(result_df_3)

# COMMAND ----------
# Alternative approach using a more direct method with array functions
sql_query_4 = """
SELECT SUM(sales_per_batch) as total_sales
FROM (
  SELECT 
    batchId,
    AGGREGATE(performance, 0, (acc, x) -> acc + x.sales) as sales_per_batch
  FROM sales
)
"""
result_df_4 = spark.sql(sql_query_4)
display(result_df_4)

# COMMAND ----------
sql_query_5 = """
SELECT 
    collect_list(performance.sales)
FROM sales
"""
result_df_5 = spark.sql(sql_query_5)
display(result_df_5)


# COMMAND ----------
sql_query_6 = """
SELECT 
    collect_set(performance.sales)
FROM sales
"""
result_df_6 = spark.sql(sql_query_6)
display(result_df_6)


# COMMAND ----------
sql_query_7 = """
SELECT 
    flatten(collect_list(performance.sales))
FROM sales
"""
result_df_7 = spark.sql(sql_query_7)
display(result_df_7)


# COMMAND ----------
sql_query_8 = """
SELECT 
    aggregate(flatten(collect_list(performance.sales)), 0, (acc, x) -> acc + x) as total_sales
FROM sales
"""
result_df_8 = spark.sql(sql_query_8)
display(result_df_8)


# COMMAND ----------
sql_query_9 = """
SELECT 
    explode(flatten(collect_list(performance.sales))) as sales
FROM sales
"""
result_df_9 = spark.sql(sql_query_9)
display(result_df_9)


# COMMAND ----------
sql_query_10 = """
WITH BASE AS (
SELECT 
    explode(flatten(collect_list(performance.sales))) as sales
FROM sales
)
SELECT 
    sum(sales) as total_sales
FROM BASE
"""
result_df_10 = spark.sql(sql_query_10)
display(result_df_10)


# COMMAND ----------
logger.info("Employee Sales Total Script Finished")
