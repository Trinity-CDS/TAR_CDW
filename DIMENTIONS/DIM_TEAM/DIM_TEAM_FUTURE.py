# Databricks notebook source
# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# Import Statements
from pyspark.sql.functions import sha2, concat_ws, col, lit,current_date
from pyspark.sql.functions import when, col
from pyspark.sql.types import *
from pyspark.sql.window import Window

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------


# Create SparkSession with LEGACY time parser policy
spark = SparkSession.builder \
    .appName("Change Date Format") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update DW_HASH column

# COMMAND ----------

src_future = spark.read.table("db_sil_temp_delta.SRC_TEAM_FUTURE").drop("dw_hash")
src_hash = src_future.withColumn("DW_HASH",sha2(concat_ws("|","TEAM_ID", "TEAM_NAME", "GROUP_ID", "GROUP_NAME", "SFA_TEAM_ID","SFA_TEAM_ID_NUM","ALIGNMENT_FLAG","ALIGNMENT_TYPE","TEAM_ACRONYM","LAUNCH_DATE", "SFA_ALIGNMENT_FLAG","SYSTEM_ALIGNMENT_HANDLE"), 256)
.cast(BinaryType()))

src_hash.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.SRC_TEAM_FUTURE")

# COMMAND ----------

abc=spark.sql("""
SELECT 
    TEAM_ID
, TEAM_NAME
, GROUP_ID
, GROUP_NAME
, SFA_TEAM_ID
, SFA_TEAM_ID_NUM
, ALIGNMENT_FLAG
, ALIGNMENT_TYPE
, TEAM_ACRONYM
, LAUNCH_DATE
, SYSTEM_ALIGNMENT_HANDLE
, SFA_ALIGNMENT_FLAG
, DW_HASH,
    1 AS DIRTY_FLAG,
    CURRENT_TIMESTAMP() AS CHANGE_DATE
FROM db_sil_temp_delta.SRC_TEAM_FUTURE
""")
abc.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable("db_sil_temp_delta.DIM_TEAM_FUTURE")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Updating the flag

# COMMAND ----------


spark.sql("""
    UPDATE db_sil_temp_delta.DIM_TEAM_FUTURE
    SET DIRTY_FLAG = 0
    WHERE DIRTY_FLAG = 1
""")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Add new columns based on the logic

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

df = spark.read.table("db_sil_temp_delta.DIM_TEAM_FUTURE")
# .drop("CHANGE_DATE")
windowSpec = Window.partitionBy("TEAM_NAME").orderBy(F.col("TEAM_NAME").desc())
#.withColumn("DIRTY_FLAG", F.when(df["DIRTY_FLAG"] == "1", "True").otherwise("false")) \
df = df.withColumn("SFA_TEAM_ID", F.when(df["SFA_TEAM_ID"] == "", None).otherwise(df["SFA_TEAM_ID"])) \
       .withColumn("SFA_TEAM_ID_NUM", F.when(df["SFA_TEAM_ID_NUM"] == "", None).otherwise(df["SFA_TEAM_ID_NUM"])) \
       .withColumn("TEAM_ACRONYM", F.when(df["TEAM_ACRONYM"] == "", None).otherwise(df["TEAM_ACRONYM"])) \
       .withColumn("LAUNCH_DATE", F.date_format(F.to_date("LAUNCH_DATE", "MM/dd/yyyy"), "yyyy-MM-dd")) \
       .withColumn("ADD_DATE", F.lit(None).cast("date"))

df.write.option("compression", "snappy") \
       .option("overwriteSchema", "true") \
       .option("mergeSchema", "true") \
       .mode("overwrite") \
       .format("delta") \
       .saveAsTable("db_sil_temp_delta.DIM_TEAM_FUTURE")

# COMMAND ----------

spark.conf.set("spark.databricks.delta.checkLatestSchemaOnRead", "false")


# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.types import DecimalType, TimestampType, BooleanType

df = spark.read.table("db_sil_temp_delta.DIM_TEAM_FUTURE")
df = df.withColumn('TEAM_ID', col('TEAM_ID').cast(DecimalType(10, 0))) \
       .withColumn('GROUP_ID', col('GROUP_ID').cast(DecimalType(10, 0))) \
       .withColumn('SFA_TEAM_ID_NUM', col('SFA_TEAM_ID_NUM').cast(DecimalType(10, 0))) \
       .withColumn('LAUNCH_DATE', col('LAUNCH_DATE').cast(TimestampType())) \
       .withColumn('DIRTY_FLAG', col('DIRTY_FLAG').cast(BooleanType())) 



# COMMAND ----------

df=df.select("TEAM_ID",
"TEAM_NAME",
"GROUP_ID",
"GROUP_NAME",
"SFA_TEAM_ID",
"SFA_TEAM_ID_NUM",
"ALIGNMENT_FLAG",
"ALIGNMENT_TYPE",
"TEAM_ACRONYM",
"LAUNCH_DATE",
"SYSTEM_ALIGNMENT_HANDLE",
"SFA_ALIGNMENT_FLAG",
"DW_HASH",
"ADD_DATE",
"CHANGE_DATE",
"DIRTY_FLAG")

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("db_sil_temp_delta.DIM_TEAM_FUTURE")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Code ends
