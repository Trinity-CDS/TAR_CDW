# Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------


from pyspark.sql.functions import concat, sha2, col, lit,current_timestamp

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ####reads a table named SRC_DECILE into mdmistprod_SRC_DECILE and initializes an empty DataFrame prod_DIM_DECILE with the specified schema, likely intended for data loading or transformation

# COMMAND ----------

schema = StructType([
    StructField("DECILE_ID", IntegerType(), True),
    StructField("DECILE_GROUP", StringType(), True),
    StructField("DECILE_CAT", IntegerType(), True),
    StructField("DECILE_GROUP_SORT", IntegerType(), True),
    StructField("DW_HASH", StringType(), True)  
])


mdmistprod_SRC_DECILE = spark.read.table("db_sil_temp_delta.SRC_DECILE")
prod_DIM_DECILE = spark.createDataFrame([], schema)

# COMMAND ----------


mdmistprod_SRC_DECILE=mdmistprod_SRC_DECILE.drop("DW_HASH")

mdmistprod_SRC_DECILE.createOrReplaceTempView("mdmistprod_SRC_DECILE")


sql = """
    SELECT
        DECILE_ID,
        DECILE_GROUP,
        DECILE_CAT,
        DECILE_GROUP_SORT,
        sha2(CONCAT_WS('|', DECILE_ID, DECILE_GROUP,DECILE_CAT,DECILE_GROUP_SORT), 256) AS DW_HASH
    FROM
        mdmistprod_SRC_DECILE
"""

result_df = spark.sql(sql)
# result_df.display()

result_df.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.mdmistprod_SRC_DECILE")

# COMMAND ----------

mdmistprod_SRC_DECILE_read=spark.read.table("db_sil_temp_delta.mdmistprod_SRC_DECILE")

update_condition = (
    (prod_DIM_DECILE["DECILE_ID"] == mdmistprod_SRC_DECILE_read["DECILE_ID"])  &
    (prod_DIM_DECILE["DW_HASH"] != mdmistprod_SRC_DECILE_read["DW_HASH"])
)

prod_DIM_DECILE = prod_DIM_DECILE.alias("D").join(
    mdmistprod_SRC_DECILE_read.alias("V"),
    update_condition,
    "inner"
).select(
"D.*",
col("V.DECILE_GROUP").alias("DECILE_GROUP"),
col("V.DECILE_CAT").alias("DECILE_CAT"),
col("V.DECILE_GROUP_SORT").alias("DECILE_GROUP_SORT"),
col("V.DW_HASH").alias("DW_HASH"),
lit(1).alias("DIRTY_FLAG"),
F.current_timestamp().alias("CHANGE_DATE")
)

# COMMAND ----------

insert_condition = ~col("DECILE_ID").isin(
    [row.DECILE_ID for row in prod_DIM_DECILE.select("DECILE_ID").distinct().collect()]
)

new_records = mdmistprod_SRC_DECILE_read.alias("P").filter(insert_condition).select(
    col("P.DECILE_ID"),
    col("P.DECILE_GROUP"),
    col("P.DECILE_CAT"),
    col("P.DECILE_GROUP_SORT"),
    col("P.DW_HASH"),
    lit(1).alias("DIRTY_FLAG"),
    current_timestamp().alias("CHANGE_DATE")
)

prod_DIM_DECILE = prod_DIM_DECILE.select(
    col("DECILE_ID").cast("string").alias("DECILE_ID"),  # Alias added
    col("D.DECILE_GROUP").cast("string").alias("DECILE_GROUP"),  # Alias added
    col("D.DECILE_CAT"),
    col("D.DECILE_GROUP_SORT").cast("string"),
    col("D.DW_HASH").cast("string"),
    col("DIRTY_FLAG").cast("integer"),
    col("CHANGE_DATE")
)

prod_DIM_DECILE = prod_DIM_DECILE.unionByName(new_records)

#  Show the resulting DataFrame
# prod_DIM_DECILE_.display()

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/db_sil_temp_delta.db/dim_decile',recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table db_sil_temp_delta.DIM_DECILE

# COMMAND ----------

prod_DIM_DECILE.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_DECILE") 

mdmistprod_SRC_DECILE_read.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.mdmistprod_SRC_DECILE")

# COMMAND ----------

# MAGIC %md
# MAGIC ####DW_HASH, and to update DIRTY_FLAG to 0 where it is currently 1 in prod_DIM_DECILE_DEFINITION

# COMMAND ----------


delete_sql = """
    DELETE FROM db_sil_temp_delta.DIM_DECILE Z
    WHERE NOT EXISTS (
        SELECT 1
        FROM db_sil_temp_delta.mdmistprod_SRC_DECILE I
        WHERE Z.DW_HASH = I.DW_HASH 	
    )
"""


update_sql = """
    UPDATE db_sil_temp_delta.DIM_DECILE
    SET DIRTY_FLAG = 0
    WHERE DIRTY_FLAG = 1
"""

# Execute the SQL queries
spark.sql(delete_sql)
spark.sql(update_sql)

# COMMAND ----------

abc=spark.read.table("db_sil_temp_delta.DIM_DECILE")
abc.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_DECILE_intm") 

# COMMAND ----------

from pyspark.sql.types import DecimalType,BooleanType
from pyspark.sql.functions import lit,col
abc=spark.read.table("db_sil_temp_delta.DIM_DECILE_intm")
abc = abc.withColumn("ADD_DATE", lit('null'))
abc=abc.withColumn('DECILE_ID', abc['DECILE_ID'].cast("decimal(10,0)")).withColumn('DIRTY_FLAG', col('DIRTY_FLAG').cast(BooleanType()))

# COMMAND ----------

abc.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_DECILE")
