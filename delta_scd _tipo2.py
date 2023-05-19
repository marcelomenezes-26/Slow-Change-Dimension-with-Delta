# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from delta import *
from pyspark.sql.functions import *
import os
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, DateType, BooleanType

# COMMAND ----------

# DBTITLE 1,Criando tabela inicial Clientes no formato delta
data = [('123 Main St', True, '2022-01-01', None, 1),
        ('456 Maple Ave', False, '2022-02-01', None, 2),
        ('789 Oak Blvd', True, '2022-03-01', None, 3)]

schema_clientes = StructType([
        StructField("endereco", StringType(), False),
        StructField("ativo", BooleanType(), False),
        StructField("dt_inicio", StringType(), False),
        StructField("dt_final", StringType(), True),
        StructField("id_cliente", IntegerType(), False)
    ])

df = spark.createDataFrame(data, schema=schema_clientes).select("id_cliente", "endereco", "ativo", "dt_inicio", "dt_final")

df.write.format("delta").mode("overwrite").saveAsTable("default.clientes")

# COMMAND ----------

# DBTITLE 1,simulando a camada raw - Salvando os  novos dados
new_data = [('111 Main St', '2023-03-23', 1),
        ('456 Maple Ave', '2022-02-01',  2),
        ('789 Oak Blvd', '2022-03-01',  3)]

schema_clientes = StructType([
        StructField("endereco", StringType(), False),
        StructField("dt_inicio", StringType(), False),
        StructField("id_cliente", IntegerType(), False)
    ])

new_df = spark.createDataFrame(new_data, schema=schema_clientes).select("id_cliente", "endereco", "dt_inicio")

# COMMAND ----------

# DBTITLE 1,Lendo a tabela clientes
# Ler a tabela com os dados atuais
tb_clientes = DeltaTable.forName(spark, "default.clientes")

# COMMAND ----------

# DBTITLE 1,Criando dataframes intermediários para facilitar no merge com SCD tipo 2
# Dataframe com clientes existentes que possuem um novo endereço.
tb_insert = new_df \
  .alias("updates") \
  .join(tb_clientes.toDF().alias("clientes"), "id_cliente") \
  .where("clientes.ativo = true AND updates.endereco <> clientes.endereco")

# União das linhas que serão inseridas e/ou alteradas
tb_update = (
  tb_insert
  .selectExpr("NULL as mergeKey", "updates.*")   # Linhas que serão inseridas na condição whenNotMatched 
  .union(new_df.selectExpr("id_cliente as mergeKey", "*"))
)

# COMMAND ----------

# DBTITLE 1,Exibindo o dataframe tb_update para facilitar o entendimento
# As linhas que já estão na tabela atual, mas possuem um novo endereço estão com a coluna MergeKey = nulo, pois na etapa do merge serão inseridas como uma nova versão da linha
display(tb_update)

# COMMAND ----------

# DBTITLE 1,Realizando o merge final
# Merge SCD tipo 2
tb_clientes.alias("old").merge(
    tb_update.alias("new"),
    "old.id_cliente = mergeKey") \
.whenMatchedUpdate(
    condition = "old.ativo = true AND old.endereco <> new.endereco",
    set = {
        "ativo": "false",
        "dt_final": "new.dt_inicio"
    }
).whenNotMatchedInsert(
    values = {
        "id_cliente": "new.id_cliente",
        "endereco": "new.endereco",
        "ativo": "true",
        "dt_inicio": "new.dt_inicio",
        "dt_final": "null"
    }
).execute()

# COMMAND ----------

# DBTITLE 1,Exibindo o dataframe final da tabela Clientes
display(spark.table("default.clientes"))
