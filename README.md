# Hello everyone, welcome!
I hope the code below can help you.

The code involves creating a table called "clientes" in the Delta format, inserting new data into this table, and applying a merge based on a Slowly Changing Dimension (SCD) type 2 strategy.

## Code Steps

### 1.Importing necessary libraries:
```
from delta import *
from pyspark.sql.functions import *
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, BooleanType
```
### 2. Creating the initial "clientes" table in the Delta format:
```
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
```

### 3. Simulating the "raw" layer and saving new data:
```
new_data = [('111 Main St', '2023-03-23', 1),
        ('456 Maple Ave', '2022-02-01',  2),
        ('789 Oak Blvd', '2022-03-01',  3)]

schema_clientes = StructType([
        StructField("endereco", StringType(), False),
        StructField("dt_inicio", StringType(), False),
        StructField("id_cliente", IntegerType(), False)
    ])

new_df = spark.createDataFrame(new_data, schema=schema_clientes).select("id_cliente", "endereco", "dt_inicio")
```

### 4. Reading the "clientes" table:
```
tb_clientes = DeltaTable.forName(spark, "default.clientes")
```

### 5. Creating intermediate dataframes to facilitate the SCD type 2 merge:
```
tb_insert = new_df \
  .alias("updates") \
  .join(tb_clientes.toDF().alias("clientes"), "id_cliente") \
  .where("clientes.ativo = true AND updates.endereco <> clientes.endereco")

tb_update = (
  tb_insert
  .selectExpr("NULL as mergeKey", "updates.*")
  .union(new_df.selectExpr("id_cliente as mergeKey", "*"))
)
```

### 6.Performing the final merge to implement SCD type 2:
```
tb_clientes.alias("old").merge(
    tb_update.alias("new"),
    "old.id_cliente = mergeKey"
).whenMatchedUpdate(
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
```

### 7. Displaying the final dataframe of the "clientes" table:
```
display(spark.table("default.clientes"))
```

This code illustrates how to create a Delta table, read data from a "raw" layer, and apply an SCD type 2 strategy to update existing records and insert new records into the table.
