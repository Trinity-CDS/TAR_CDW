# Databricks notebook source
# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------


from pyspark.sql.functions import concat, sha2, col, lit,current_timestamp

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ####reads a table named SRC_DECILE_DEFINITION into mdmistprod_SRC_DECILE_DEFINITION and initializes an empty DataFrame prod_DIM_DECILE_DEFINITION with the specified schema, likely intended for data loading or transformation

# COMMAND ----------

schema = StructType([
    StructField("DECILE_DEFINITION_ID", StringType(), True),
    StructField("DECILE_DEFINITION_NAME", IntegerType(), True),
    StructField("INTERNAL_PROD_ID", IntegerType(), True),
    StructField("NOTE", StringType(), True),
    StructField("DW_HASH", StringType(), True)  
])


mdmistprod_SRC_DECILE_DEFINITION = spark.read.table("db_sil_temp_delta.SRC_DECILE_DEFINITION")
prod_DIM_DECILE_DEFINITION = spark.createDataFrame([], schema)

# COMMAND ----------


mdmistprod_SRC_DECILE_DEFINITION=mdmistprod_SRC_DECILE_DEFINITION.drop("DW_HASH")

mdmistprod_SRC_DECILE_DEFINITION.createOrReplaceTempView("mdmistprod_SRC_DECILE_DEFINITION")


sql = """
    SELECT
        DECILE_DEFINITION_ID,
        INTERNAL_PROD_ID,
        DECILE_DEFINITION_NAME,
        NOTE,
        sha2(CONCAT_WS('|', DECILE_DEFINITION_ID, INTERNAL_PROD_ID,DECILE_DEFINITION_NAME,NOTE), 256) AS DW_HASH
    FROM
        mdmistprod_SRC_DECILE_DEFINITION
"""

result_df = spark.sql(sql)
# result_df.display()

result_df.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.mdmistprod_SRC_DECILE_DEFINITION") 

# COMMAND ----------

mdmistprod_SRC_DECILE_DEFINITION_read=spark.read.table("db_sil_temp_delta.mdmistprod_SRC_DECILE_DEFINITION")

update_condition = (
    (prod_DIM_DECILE_DEFINITION["DECILE_DEFINITION_ID"] == 
     mdmistprod_SRC_DECILE_DEFINITION_read["DECILE_DEFINITION_ID"]) &
    (prod_DIM_DECILE_DEFINITION["INTERNAL_PROD_ID"] == mdmistprod_SRC_DECILE_DEFINITION_read["INTERNAL_PROD_ID"]) &
    (prod_DIM_DECILE_DEFINITION["DW_HASH"] != mdmistprod_SRC_DECILE_DEFINITION_read["DW_HASH"])
)

prod_DIM_DECILE_DEFINITION = prod_DIM_DECILE_DEFINITION.alias("D").join(
    mdmistprod_SRC_DECILE_DEFINITION_read.alias("V"),
    update_condition,
    "inner"
).select(
"D.*",
col("V.INTERNAL_PROD_ID").alias("INTERNAL_PROD_ID"),
col("V.DECILE_DEFINITION_NAME").alias("DECILE_DEFINITION_NAME"),
col("V.NOTE").alias("NOTE"),
col("V.DW_HASH").alias("DW_HASH"),
lit(1).alias("DIRTY_FLAG"),
F.current_timestamp().alias("CHANGE_DATE")
)

# COMMAND ----------

insert_condition = ~col("DECILE_DEFINITION_ID").isin(
    [row.DECILE_DEFINITION_ID for row in prod_DIM_DECILE_DEFINITION.select("DECILE_DEFINITION_ID").distinct().collect()]
)

new_records = mdmistprod_SRC_DECILE_DEFINITION_read.alias("P").filter(insert_condition).select(
    col("P.DECILE_DEFINITION_ID"),
    col("P.INTERNAL_PROD_ID"),
    col("P.DECILE_DEFINITION_NAME"),
    col("P.NOTE"),
    col("P.DW_HASH"),
    lit(1).alias("DIRTY_FLAG"),
    current_timestamp().alias("CHANGE_DATE")
)

prod_DIM_DECILE_DEFINITION = prod_DIM_DECILE_DEFINITION.select(
    col("DECILE_DEFINITION_ID").cast("string").alias("DECILE_DEFINITION_ID"),  # Alias added
    col("D.INTERNAL_PROD_ID").cast("string").alias("INTERNAL_PROD_ID"),  # Alias added
    col("D.DECILE_DEFINITION_NAME"),
    col("D.NOTE").cast("string"),
    col("D.DW_HASH").cast("string"),
    col("DIRTY_FLAG").cast("integer"),
    col("CHANGE_DATE")
)

prod_DIM_DECILE_DEFINITION = prod_DIM_DECILE_DEFINITION.unionByName(new_records)

#  Show the resulting DataFrame
# prod_DIM_DECILE_DEFINITION.display()

# COMMAND ----------

prod_DIM_DECILE_DEFINITION.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_DECILE_DEFINITION") 

mdmistprod_SRC_DECILE_DEFINITION_read.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.mdmistprod_SRC_DECILE_DEFINITION") 

# COMMAND ----------

# MAGIC %md
# MAGIC ####DW_HASH, and to update DIRTY_FLAG to 0 where it is currently 1 in prod_DIM_DECILE_DEFINITION

# COMMAND ----------


delete_sql = """
    DELETE FROM db_sil_temp_delta.DIM_DECILE_DEFINITION Z
    WHERE NOT EXISTS (
        SELECT 1
        FROM db_sil_temp_delta.mdmistprod_SRC_DECILE_DEFINITION I
        WHERE Z.DW_HASH = I.DW_HASH 	
    )
"""


update_sql = """
    UPDATE db_sil_temp_delta.DIM_DECILE_DEFINITION
    SET DIRTY_FLAG = 0
    WHERE DIRTY_FLAG = 1
"""

# Execute the SQL queries
spark.sql(delete_sql)
spark.sql(update_sql)

# COMMAND ----------

abc=spark.read.table("db_sil_temp_delta.DIM_DECILE_DEFINITION")
abc.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_DECILE_DEFINITION_intm") 

# COMMAND ----------

from pyspark.sql.types import DecimalType,BooleanType
from pyspark.sql.functions import lit,col
abc=spark.read.table("db_sil_temp_delta.DIM_DECILE_DEFINITION_intm")
abc = abc.withColumn("ADD_DATE", lit('null'))
abc=abc.withColumn('DIRTY_FLAG', col('DIRTY_FLAG').cast(BooleanType()))

# COMMAND ----------

# %sql
# truncate table cdw_curated.DIM_DECILE_DEFINITION

# COMMAND ----------

abc.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("parquet").saveAsTable("db_sil_temp_delta.DIM_DECILE_DEFINITION")
