# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # LATERAL VIEW EXPLODE em Databricks SQL
# MAGIC 
# MAGIC Este notebook demonstra o uso de LATERAL VIEW EXPLODE para desaninhar matrizes de structs e acessar campos individuais.

# COMMAND ----------
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.logger import logger

logger.info("Iniciando exemplos de LATERAL VIEW EXPLODE em Databricks SQL")

question_description = """
Explicação sobre LATERAL VIEW EXPLODE em Spark SQL:

Ao lidar com um conjunto de dados que tem uma estrutura complexa contendo matrizes de structs,
a função LATERAL VIEW EXPLODE é usada para desaninhar as matrizes e acessar campos individuais 
dentro das structs para análise.

Isso nos permite acessar os campos individuais (field1, field2) dentro das structs usando o alias "structs" 
na instrução SELECT.
"""
logger.info(question_description)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Configuração: Criando DataFrame de exemplo com dados que contêm arrays de structs

# COMMAND ----------
# Dados de exemplo - Tabela com array de structs
data = [
    (1, "Usuário A", [
        {"field1": "valor1A", "field2": 100},
        {"field1": "valor2A", "field2": 200},
        {"field1": "valor3A", "field2": 300}
    ]),
    (2, "Usuário B", [
        {"field1": "valor1B", "field2": 150},
        {"field1": "valor2B", "field2": 250}
    ]),
    (3, "Usuário C", [
        {"field1": "valor1C", "field2": 175},
        {"field1": "valor2C", "field2": 275},
        {"field1": "valor3C", "field2": 375},
        {"field1": "valor4C", "field2": 475}
    ])
]

# Definindo o schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("array_column", ArrayType(StructType([
        StructField("field1", StringType(), True),
        StructField("field2", IntegerType(), True)
    ])), True)
])

# Criando DataFrame
df_exemplo = spark.createDataFrame(data, schema)

# Criando view temporária
df_exemplo.createOrReplaceTempView("table")

logger.info("Criada view temporária 'table' com os seguintes dados:")
display(df_exemplo)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Exemplo 1: Consulta incorreta - EXPLODE sem LATERAL VIEW

# COMMAND ----------
# Exemplo de consulta incorreta
sql_query_incorrect = """
SELECT EXPLODE(array_column) AS structs FROM table
"""

logger.info("Exemplo 1 - Consulta incorreta: EXPLODE sem LATERAL VIEW")
logger.info(sql_query_incorrect)
logger.info("Esta consulta está incorreta porque tenta desaninhar o array sem usar LATERAL VIEW e tenta acessar campos usando UNNEST, que não é a sintaxe correta no Spark SQL.")

result_df_incorrect = spark.sql(sql_query_incorrect)
display(result_df_incorrect)
# COMMAND ----------
# MAGIC %md
# MAGIC ### Exemplo 2: Consulta correta - LATERAL VIEW EXPLODE

# COMMAND ----------
# Exemplo de consulta correta
sql_query_correct = """
SELECT 
    id,
    nome,
    structs.field1, 
    structs.field2 
FROM table 
LATERAL VIEW EXPLODE(array_column) AS structs
"""

logger.info("Exemplo 2 - Consulta correta: LATERAL VIEW EXPLODE")
logger.info(sql_query_correct)

result_df_correct = spark.sql(sql_query_correct)
display(result_df_correct)


# COMMAND ----------
# Exemplo de consulta correta
sql_query_correct = """
SELECT 
    id,
    nome,
    structs.field1, 
    structs.field2 
FROM table 
LATERAL VIEW EXPLODE(array_column) AS structs
"""

logger.info("Exemplo 2 - Consulta correta: LATERAL VIEW EXPLODE")
logger.info(sql_query_correct)

result_df_correct = spark.sql(sql_query_correct)
display(result_df_correct)


# COMMAND ----------
# MAGIC %md
# MAGIC ### Explicação

# COMMAND ----------
explanation = """
Explicação:

No Spark SQL, ao lidar com um conjunto de dados que tem uma estrutura complexa contendo matrizes de structs, 
a função LATERAL VIEW EXPLODE é usada para desaninhar as matrizes e acessar campos individuais dentro das 
structs para análise. Nessa operação, a consulta correta consiste em:

1. Selecionar os campos individuais (field1, field2) dentro das structs usando a função EXPLODE na cláusula 
   LATERAL VIEW
   
2. A palavra-chave LATERAL VIEW é usada para criar um alias de tabela temporário "structs" para as structs 
   explodidas
   
3. Isso nos permite acessar os campos individuais dentro das structs usando o alias "structs" na instrução 
   SELECT

Essa consulta é eficiente porque desaninhar diretamente as matrizes e permite o acesso aos campos individuais 
dentro das structs sem a necessidade de transformações adicionais. Ela fornece uma maneira clara e concisa de 
analisar os dados dentro da estrutura complexa do conjunto de dados.
"""

logger.info(explanation)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Exemplo 3: Filtrando resultados após LATERAL VIEW EXPLODE

# COMMAND ----------
# Exemplo de filtragem após EXPLODE
sql_query_filter = """
SELECT 
    id,
    nome,
    structs.field1, 
    structs.field2 
FROM table 
LATERAL VIEW EXPLODE(array_column) AS structs
WHERE structs.field2 > 200
"""

logger.info("Exemplo 3 - Filtrando resultados após LATERAL VIEW EXPLODE")
logger.info(sql_query_filter)

result_df_filter = spark.sql(sql_query_filter)
display(result_df_filter)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Exemplo 4: Agregação após LATERAL VIEW EXPLODE

# COMMAND ----------
# Exemplo de agregação após EXPLODE
sql_query_aggregation = """
SELECT 
    id,
    nome,
    COUNT(structs.field1) as total_items,
    SUM(structs.field2) as soma_field2,
    AVG(structs.field2) as media_field2
FROM table 
LATERAL VIEW EXPLODE(array_column) AS structs
GROUP BY id, nome
"""

logger.info("Exemplo 4 - Agregação após LATERAL VIEW EXPLODE")
logger.info(sql_query_aggregation)

result_df_aggregation = spark.sql(sql_query_aggregation)
display(result_df_aggregation)
