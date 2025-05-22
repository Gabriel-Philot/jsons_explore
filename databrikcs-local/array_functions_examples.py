# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Funções de Array em Databricks SQL
# MAGIC 
# MAGIC Este notebook demonstra o uso das 4 principais funções de array em Databricks SQL:
# MAGIC - FILTER
# MAGIC - TRANSFORM
# MAGIC - AGGREGATE
# MAGIC - REDUCE

# COMMAND ----------
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.logger import logger

logger.info("Iniciando exemplos de funções de array em Databricks SQL")

question_description = """
Explicação e exemplos das 4 principais funções de array em Databricks SQL:

1. FILTER: Filtra elementos de um array com base em uma condição
2. TRANSFORM: Aplica uma transformação a cada elemento do array
3. AGGREGATE: Agrega valores de um array usando um acumulador e uma função
4. REDUCE: Combina todos os elementos de um array em um único valor
"""
logger.info(question_description)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Configuração: Criando DataFrame de exemplo com dados de produtos

# COMMAND ----------
# Dados de exemplo - Produtos com preços, categorias e avaliações
data = [
    (1, "Loja A", [{"id": 101, "nome": "Smartphone", "preco": 1200, "categoria": "Eletrônicos", "avaliacoes": [4, 5, 3, 5, 4]},
                  {"id": 102, "nome": "Headphone", "preco": 150, "categoria": "Eletrônicos", "avaliacoes": [3, 2, 4, 3]},
                  {"id": 103, "nome": "Camiseta", "preco": 50, "categoria": "Vestuário", "avaliacoes": [5, 5, 4, 5]}]),
    (2, "Loja B", [{"id": 201, "nome": "Notebook", "preco": 3500, "categoria": "Eletrônicos", "avaliacoes": [5, 4, 5, 4, 5]},
                  {"id": 202, "nome": "Mouse", "preco": 80, "categoria": "Eletrônicos", "avaliacoes": [4, 3, 4, 4]},
                  {"id": 203, "nome": "Tênis", "preco": 200, "categoria": "Calçados", "avaliacoes": [4, 3, 3, 4, 5]}])
]

# Definindo o schema
schema = StructType([
    StructField("lojaId", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("produtos", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("nome", StringType(), True),
        StructField("preco", IntegerType(), True),
        StructField("categoria", StringType(), True),
        StructField("avaliacoes", ArrayType(IntegerType()), True)
    ])), True)
])

# Criando DataFrame
df_lojas = spark.createDataFrame(data, schema)

# Criando view temporária
df_lojas.createOrReplaceTempView("lojas")

logger.info("Criada view temporária 'lojas' com os seguintes dados:")
display(df_lojas)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. FILTER - Filtrando elementos de um array


# COMMAND ----------
# Exemplo 0: P.produtos
sql_query_filter0 = """
SELECT 
    lojaId,
    nome as loja,
    produtos,
    produtos.preco,
    produtos.categoria,
    produtos.avaliacoes
FROM lojas
"""

logger.info("Exemplo 0 - FILTER: P.produtos")
logger.info(sql_query_filter0)

result_df_filter0 = spark.sql(sql_query_filter0)
display(result_df_filter0)

# COMMAND ----------
# Exemplo 1: Filtrar produtos com preço maior que 100
sql_query_filter1 = """
SELECT 
    lojaId,
    nome as loja,
    FILTER(produtos, p -> p.preco > 100) as produtos_caros
FROM lojas
"""

logger.info("Exemplo 1 - FILTER: Filtrar produtos com preço maior que 100")
logger.info(sql_query_filter1)

result_df_filter1 = spark.sql(sql_query_filter1)
display(result_df_filter1)

# COMMAND ----------
# Exemplo 2: Filtrar produtos da categoria 'Eletrônicos'
sql_query_filter2 = """
SELECT 
    lojaId,
    nome as loja,
    FILTER(produtos, p -> p.categoria = 'Eletrônicos') as produtos_eletronicos
FROM lojas
"""

logger.info("Exemplo 2 - FILTER: Filtrar produtos da categoria 'Eletrônicos'")
logger.info(sql_query_filter2)

result_df_filter2 = spark.sql(sql_query_filter2)
display(result_df_filter2)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. TRANSFORM - Transformando elementos de um array

# COMMAND ----------
# Exemplo 1: Aplicar desconto de 10% em todos os produtos
sql_query_transform1 = """
SELECT 
    lojaId,
    nome as loja,
    TRANSFORM(produtos, p -> NAMED_STRUCT(
        'preco_original', p.preco,
        'preco_com_desconto', p.preco * 0.9,
        'preco_pro_lucao', p.preco * 0.4,
        'categoria', p.categoria,
        'avaliacoes', p.avaliacoes
    )) as produtos_com_desconto
FROM lojas
"""

logger.info("Exemplo 1 - TRANSFORM: Aplicar desconto de 10% em todos os produtos")
logger.info(sql_query_transform1)

result_df_transform1 = spark.sql(sql_query_transform1)
display(result_df_transform1)

# COMMAND ----------
# Exemplo 2: Calcular a avaliação média para cada produto
sql_query_transform2 = """
SELECT 
    produtos.avaliacoes,
    TRANSFORM(produtos, p -> 
    cast(AGGREGATE(p.avaliacoes, 0, (acc, r) -> acc + r) as double) / cast(SIZE(p.avaliacoes) as double)
) as media_avaliacoes FROM lojas

"""

logger.info("Exemplo 2 - TRANSFORM: Calcular a avaliação média para cada produto")
logger.info(sql_query_transform2)

result_df_transform2 = spark.sql(sql_query_transform2)
display(result_df_transform2)

# COMMAND ----------
sql_query_transform_03 = """
SELECT 
    lojaId,
    nome as loja,
    produtos.preco,
    produtos.nome,
    TRANSFORM(produtos, p -> 
    CASE WHEN p.preco > 100 THEN 'Caro' ELSE 'Barato' END
) as classificacao_preco FROM lojas
"""

logger.info("Exemplo 3 - TRANSFORM: Classificar produtos como 'Caro' ou 'Barato' com base no preço")
logger.info(sql_query_transform_03)

result_df_transform_03 = spark.sql(sql_query_transform_03)
display(result_df_transform_03)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. AGGREGATE - Agregando valores de um array

# COMMAND ----------
# Exemplo 1: Calcular o valor total dos produtos de cada loja
sql_query_aggregate1 = """
SELECT 
    lojaId,
    nome as loja,
    produtos.preco,
    AGGREGATE(produtos, 0, (acc, p) -> acc + p.preco) as valor_total_produtos
FROM lojas
"""

logger.info("Exemplo 1 - AGGREGATE: Calcular o valor total dos produtos de cada loja")
logger.info(sql_query_aggregate1)

result_df_aggregate1 = spark.sql(sql_query_aggregate1)
display(result_df_aggregate1)

# COMMAND ----------
# Exemplo 2: Contar quantos produtos de cada categoria existem em cada loja
sql_query_aggregate2 = """
SELECT 
    lojaId,
    nome as loja,
    AGGREGATE(
    produtos, 
    STRUCT(0 as soma, 0 as contador), 
    (acc, p) -> 
        STRUCT(
            acc.soma + p.preco as soma,
            acc.contador + 1 as contador
        ),
    acc -> CASE 
            WHEN acc.contador > 0 THEN acc.soma / acc.contador 
            ELSE 0
        END
    ) as preco_medio_produtos
FROM lojas
"""

logger.info("Exemplo 2 - AGGREGATE: Contar quantos produtos de cada categoria existem em cada loja")
logger.info(sql_query_aggregate2)

result_df_aggregate2 = spark.sql(sql_query_aggregate2)
display(result_df_aggregate2)

# COMMAND ----------
sql_query_pre_reduce = """
SELECT 
    lojaId,
    nome as loja,
    TRANSFORM(produtos, p -> p.nome) as nomes_produtos
FROM lojas
"""

logger.info("Exemplo 0 - Pre-REDUCE: Combinando elementos de um array")
logger.info(sql_query_pre_reduce)

result_df_pre_reduce = spark.sql(sql_query_pre_reduce)
display(result_df_pre_reduce)



# COMMAND ----------
# Exemplo 1: Concatenar os nomes de todos os produtos em uma única string
sql_query_reduce1 = """
SELECT 
    lojaId,
    nome as loja,
    REDUCE(
        TRANSFORM(produtos, p -> p.nome),
        '',
        (acc, nome) -> 
            CASE 
                WHEN acc = '' THEN nome 
                ELSE CONCAT(acc, ', ', nome) 
            END,
        acc -> acc
    ) as lista_produtos
FROM lojas
"""

logger.info("Exemplo 01 - REDUCE: Concatenar os nomes de todos os produtos em uma única string")
logger.info(sql_query_reduce1)

result_df_reduce1 = spark.sql(sql_query_reduce1)
display(result_df_reduce1)

# COMMAND ----------
sql_query_reduce2 = """
SELECT 
    lojaId,
    nome as loja,
    REDUCE(
    TRANSFORM(produtos, p -> p.nome),  
    '',                                
    (acc, nome) -> 
        CASE 
            WHEN acc = '' THEN nome     
            ELSE acc || ', ' || nome    
        END,
    acc -> acc
) as lista_produtos
FROM lojas
"""

logger.info("Exemplo 02 - REDUCE: Concatenar os nomes de todos os produtos em uma única string")
logger.info(sql_query_reduce2)

result_df_reduce2 = spark.sql(sql_query_reduce2)
display(result_df_reduce2)

# COMMAND ----------
