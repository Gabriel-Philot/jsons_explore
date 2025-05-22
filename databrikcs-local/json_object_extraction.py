# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # GET_JSON_OBJECT com PySpark
# MAGIC 
# MAGIC Este notebook demonstra como extrair valores de strings JSON usando a função `GET_JSON_OBJECT`, que é especialmente útil para extrair valores específicos de uma coluna contendo strings JSON.

# COMMAND ----------
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.logger import logger

logger.info("Iniciando exemplos de extração de JSON usando GET_JSON_OBJECT")

question_description = """
Explicação da função GET_JSON_OBJECT:

GET_JSON_OBJECT é uma função do Spark SQL projetada especificamente para extrair valores 
de strings JSON com base em uma chave especificada. Usa a sintaxe JSONPath para navegar
pela estrutura JSON e extrair valores.

Sintaxe: GET_JSON_OBJECT(coluna_json, '$.caminho.para.chave')

Onde:
- coluna_json: A coluna contendo a string JSON
- '$.caminho.para.chave': O caminho JSONPath para a chave desejada
"""
logger.info(question_description)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Configuração: Criando DataFrame de exemplo com dados de usuários em formato JSON

# COMMAND ----------
# Dados de exemplo - Usuários com detalhes em formato JSON
data = [
    (1, '{"userId": 101, "name": "Ana Silva", "email": "ana@example.com", "address": {"city": "São Paulo", "state": "SP"}}'),
    (2, '{"userId": 102, "name": "Carlos Santos", "email": "carlos@example.com", "address": {"city": "Rio de Janeiro", "state": "RJ"}}'),
    (3, '{"userId": 103, "name": "Mariana Costa", "email": "mariana@example.com", "address": {"city": "Belo Horizonte", "state": "MG"}}'),
    (4, '{"userId": 104, "name": "Paulo Oliveira", "email": "paulo@example.com", "address": {"city": "Brasília", "state": "DF"}}'),
]

# Definindo o schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("details", StringType(), True)
])

# Criando DataFrame
df_users = spark.createDataFrame(data, schema)

# Criando view temporária
df_users.createOrReplaceTempView("users")

logger.info("Criada view temporária 'users' com os seguintes dados:")
display(df_users)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Extraindo valores de JSON usando GET_JSON_OBJECT

# COMMAND ----------
# Exemplo 1: Extraindo o ID do usuário
sql_query_extract1 = """
SELECT 
    id,
    details,
    GET_JSON_OBJECT(details, '$.userId') as user_id
FROM users
"""

logger.info("Exemplo 1 - Extraindo o ID do usuário")
logger.info(sql_query_extract1)

result_df_extract1 = spark.sql(sql_query_extract1)
display(result_df_extract1)

# COMMAND ----------
# Exemplo 2: Extraindo nome e email
sql_query_extract2 = """
SELECT 
    id,
    GET_JSON_OBJECT(details, '$.name') as name,
    GET_JSON_OBJECT(details, '$.email') as email
FROM users
"""

logger.info("Exemplo 2 - Extraindo nome e email")
logger.info(sql_query_extract2)

result_df_extract2 = spark.sql(sql_query_extract2)
display(result_df_extract2)

# COMMAND ----------
# Exemplo 3: Extraindo valores aninhados - cidade e estado
sql_query_extract3 = """
SELECT 
    id,
    GET_JSON_OBJECT(details, '$.name') as name,
    GET_JSON_OBJECT(details, '$.address.city') as city,
    GET_JSON_OBJECT(details, '$.address.state') as state
FROM users
"""

logger.info("Exemplo 3 - Extraindo valores aninhados")
logger.info(sql_query_extract3)

result_df_extract3 = spark.sql(sql_query_extract3)
display(result_df_extract3)

# COMMAND ----------
# Comparando com outras funções JSON
sql_query_compare = """
SELECT 
    id,
    -- GET_JSON_OBJECT - extrai valor específico
    GET_JSON_OBJECT(details, '$.userId') as user_id_extracted,
    
    -- from_json - converte string JSON em struct
    from_json(details, 'struct<userId:int, name:string, email:string>').userId as user_id_parsed,
    
    -- json_tuple - extrai múltiplos valores como colunas
    json_tuple(details, 'userId', 'name') as (user_id_tuple, name_tuple)
FROM users
"""

logger.info("Comparação com outras funções JSON")
logger.info(sql_query_compare)

result_df_compare = spark.sql(sql_query_compare)
display(result_df_compare)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Use Case Prático: Extraindo o valor específico de userId

# COMMAND ----------
# Solução prática usando GET_JSON_OBJECT
sql_query_solution = """
SELECT 
    id,
    details,
    GET_JSON_OBJECT(details, '$.userId') as user_id
FROM users
WHERE GET_JSON_OBJECT(details, '$.userId') > 102
"""

logger.info("Solução prática: Filtrando usuários com ID > 102")
logger.info(sql_query_solution)

result_df_solution = spark.sql(sql_query_solution)
display(result_df_solution)

# COMMAND ----------
# Usando GET_JSON_OBJECT com funções PySpark
df_pyspark = df_users.withColumn(
    "user_id", expr("GET_JSON_OBJECT(details, '$.userId')").cast("integer")
).withColumn(
    "name", expr("GET_JSON_OBJECT(details, '$.name')")
).withColumn(
    "city", expr("GET_JSON_OBJECT(details, '$.address.city')")
)

logger.info("Usando GET_JSON_OBJECT com funções PySpark:")
display(df_pyspark)

# COMMAND ----------
logger.info("Exemplos de GET_JSON_OBJECT finalizados")
