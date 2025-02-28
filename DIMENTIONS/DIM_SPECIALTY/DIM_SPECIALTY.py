# Databricks notebook source
from pyspark.sql.functions import concat, sha2, col, lit,current_timestamp,expr,current_date
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,BinaryType

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop and recreate DW_HASH in DIM_SPECIALTY

# COMMAND ----------

df = spark.sql("select * from db_sil_temp_delta.SRC_SPEC_SPECGRP")
table_name = "SRC_SPEC_SPECGRP"
column_name = "DW_HASH"
binary_length = 32

if column_name in df.columns:
    # Drop 
    df = df.drop(column_name)

# Recreate 
df = df.withColumn(column_name, F.lit(" ").cast(BinaryType()))


df = df.withColumn(
    "DW_HASH",
    sha2(expr("concat('|',SPEC_CD, SPEC_DESC, SPEC_GRP_CD,SPEC_GRP_DESC,UPDATE_DATE,UPDATE_USER)"), 256)
)


df.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.SRC_SPEC_SPECGRP")

# COMMAND ----------

df=spark.read.table("db_sil_temp_delta.SRC_SPEC_SPECGRP")
df = df.withColumn("DW_HASH", F.col("DW_HASH").cast("string"))
df.write.option("compression", "snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.SRC_SPEC_SPECGRP")

# COMMAND ----------

abc=spark.sql("""
SELECT 
   SPEC_CD,
SPEC_DESC,
SPEC_GRP_CD,
SPEC_GRP_DESC,
UPDATE_DATE,
UPDATE_USER,
DW_HASH,
    1 AS DIRTY_FLAG,
    CURRENT_TIMESTAMP() AS CHANGE_DATE
FROM db_sil_temp_delta.SRC_SPEC_SPECGRP 
""")

# COMMAND ----------

abc=abc.withColumn("SPEC_ID", F.monotonically_increasing_id() + lit(lit(10000000)))

abc.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_SPECIALTY")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform the INSERT operation

# COMMAND ----------

spark.sql("""
 INSERT INTO db_sil_temp_delta.DIM_SPECIALTY
(
    SPEC_ID,
    SPEC_CD,
    SPEC_DESC,
    SPEC_GRP_CD,
    SPEC_GRP_DESC,
    UPDATE_DATE,
    UPDATE_USER,
    DW_HASH,
    DIRTY_FLAG,
    CHANGE_DATE
)
SELECT
    SPEC_ID,
    P.SPEC_CD,
    P.SPEC_DESC,
    P.SPEC_GRP_CD,
    P.SPEC_GRP_DESC,
    P.UPDATE_DATE,
    P.UPDATE_USER,
    P.DW_HASH,
    1 AS DIRTY_FLAG,
    current_date() AS CHANGE_DATE
FROM
    db_sil_temp_delta.SRC_SPEC_SPECGRP P
LEFT JOIN
    db_sil_temp_delta.DIM_SPECIALTY Q
ON
    P.SPEC_CD = Q.SPEC_CD AND NOT P.DW_HASH = Q.DW_HASH
WHERE
    P.SPEC_CD NOT IN (SELECT SPEC_CD FROM db_sil_temp_delta.DIM_SPECIALTY);
 """)

# COMMAND ----------

abc=spark.read.table("db_sil_temp_delta.SRC_SPEC_SPECGRP")
abc.createOrReplaceTempView("SRC_SPEC_SPECGRP")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Delete records not in the source table

# COMMAND ----------

target_table=spark.read.table("db_sil_temp_delta.DIM_SPECIALTY")
target_table.createOrReplaceTempView("DIM_SPECIALTY")
spark.sql("""
    DELETE FROM DIM_SPECIALTY
    WHERE DW_HASH NOT IN (SELECT DW_HASH FROM SRC_SPEC_SPECGRP)
""")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update DIRTY_FLAG in DIM_TEAM_prior

# COMMAND ----------

spark.sql("""
    UPDATE DIM_SPECIALTY
    SET DIRTY_FLAG = 0
    WHERE DIRTY_FLAG = 1
""")

# COMMAND ----------

from pyspark.sql.types import DecimalType, BooleanType
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_timestamp ,to_date

# Convert columns to their respective data types
target_table = target_table.withColumn("SPEC_ID", col("SPEC_ID").cast(DecimalType(10, 0))) \
                 .withColumn("DIRTY_FLAG", col("DIRTY_FLAG").cast(BooleanType())) \
                 .withColumn("ADD_DATE", lit('null')) \
                 .withColumn("UPDATE_DATE", to_timestamp("UPDATE_DATE", "MMM dd yyyy'  'h:ma")).withColumn("SPEC_GRP_SFA_DESC", lit(None)).withColumn("SPEC_GRP_SFA_CD", lit(None)).withColumn("CUSTOM_SPEC_GRP_DESC", lit(None))

target_table.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_SPECIALTY") 

# COMMAND ----------

# MAGIC %md
# MAGIC **Checking**
