# Databricks notebook source
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, sha2,col, current_date
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# COMMAND ----------



spark = SparkSession.builder \
    .appName("Change Date Format") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Code Starts

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop and recreate DW_HASH in SRC_TEAM_CURR 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, sha2
from pyspark.sql.types import StringType



df = spark.sql("select * from db_sil_temp_delta.SRC_TEAM_CURR")
df=df.drop("DW_HASH")
df.createOrReplaceTempView("SRC_TEAM_CURR")


sql = """
    SELECT
        ROW_ID,
        TEAM_ID,
        TEAM_NAME,
        GROUP_ID,
        GROUP_NAME,
        SFA_TEAM_ID,
        SFA_TEAM_ID_NUM,
        ALIGNMENT_FLAG,
        ALIGNMENT_TYPE,
        TEAM_ACRONYM,
        LAUNCH_DATE,
        SFA_ALIGNMENT_FLAG,
        SYSTEM_ALIGNMENT_HANDLE,
        sha2(CONCAT_WS('|', TEAM_ID, TEAM_NAME, GROUP_ID, GROUP_NAME, SFA_TEAM_ID, SFA_TEAM_ID_NUM, ALIGNMENT_FLAG, ALIGNMENT_TYPE, TEAM_ACRONYM, LAUNCH_DATE, SFA_ALIGNMENT_FLAG, SYSTEM_ALIGNMENT_HANDLE), 256) AS DW_HASH
    FROM
        SRC_TEAM_CURR
"""

result_df = spark.sql(sql)
# result_df.display()

result_df.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.SRC_TEAM_CURR") 


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
, SFA_ALIGNMENT_FLAG
, SYSTEM_ALIGNMENT_HANDLE
, DW_HASH,
    1 AS DIRTY_FLAG,
    CURRENT_TIMESTAMP() AS CHANGE_DATE
FROM db_sil_temp_delta.SRC_TEAM_CURR
""")
abc.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("db_sil_temp_delta.DIM_TEAM_CURR")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Count the number of rules

# COMMAND ----------



source_table = spark.read.table("db_sil_temp_delta.SRC_TEAM_CURR")


target_table = spark.read.table("db_sil_temp_delta.DIM_TEAM_CURR")


n_rule_cnt = spark.sql("""
    SELECT COUNT(*) AS nRuleCnt
    FROM terra_sp.SRC_CFG_DIMENSION_DIRTY_RULE
    WHERE DIMENSION_NAME = 'DIM_TEAM_CURR'
""").collect()[0]["nRuleCnt"]



# COMMAND ----------

delete_sql = """
    DELETE FROM db_sil_temp_delta.DIM_TEAM_CURR
    WHERE DW_HASH NOT IN (SELECT DW_HASH FROM  db_sil_temp_delta.SRC_TEAM_CURR)

"""
spark.sql(delete_sql)



# COMMAND ----------

# MAGIC %md
# MAGIC ####Update DIRTY_FLAG in DIM_TEAM_CURR

# COMMAND ----------


spark.sql("""
    UPDATE db_sil_temp_delta.DIM_TEAM_CURR
    SET DIRTY_FLAG = 0
    WHERE DIRTY_FLAG = 1
""")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate SQL statement to replace null values with empty strings

# COMMAND ----------

cc = """SFA_TEAM_ID,SFA_TEAM_ID_NUM,TEAM_ACRONYM"""

columns_to_update = cc.split(',')

for column_name in columns_to_update:
    
    sql_statement = f"""
        UPDATE db_sil_temp_delta.DIM_TEAM_CURR
        SET {column_name} = COALESCE({column_name}, '');
    """
    
    spark.sql(sql_statement)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Add new columns based on the logic

# COMMAND ----------

from pyspark.sql.types import DecimalType, TimestampType, BooleanType
from pyspark.sql.functions import unhex, col, when, row_number, current_date, to_date, date_format,unbase64
from pyspark.sql import Window

df = spark.read.table("db_sil_temp_delta.dim_team_curr")
windowSpec = Window.partitionBy("TEAM_NAME").orderBy(col("TEAM_NAME").desc())

df = df.withColumn("ADD_DATE", when(row_number().over(windowSpec) == 1, None).otherwise(None))
df = df.withColumn("CHANGED_DATE", when(row_number().over(windowSpec) == 1, current_date()).otherwise(current_date())).drop("CHANGE_DATE")
df = df.withColumn("SFA_TEAM_ID", when(df["SFA_TEAM_ID"] == "", None).otherwise(df["SFA_TEAM_ID"])) \
       .withColumn("SFA_TEAM_ID_NUM", when(df["SFA_TEAM_ID_NUM"] == "", None).otherwise(df["SFA_TEAM_ID_NUM"])) \
       .withColumn("DIRTY_FLAG", when(df["DIRTY_FLAG"] == "1", "True").otherwise("false")) \
       .withColumn("TEAM_ACRONYM", when(df["TEAM_ACRONYM"] == "", None).otherwise(df["TEAM_ACRONYM"]))
df = df.withColumn('DIRTY_FLAG', col('DIRTY_FLAG').cast(BooleanType()))

df = df.withColumn('TEAM_ID', col('TEAM_ID').cast(DecimalType(10, 0))) \
       .withColumn('GROUP_ID', col('GROUP_ID').cast(DecimalType(10, 0))) \
       .withColumn('SFA_TEAM_ID_NUM', col('SFA_TEAM_ID_NUM').cast(DecimalType(10, 0))) \
       .withColumn('CHANGE_DATE', col('CHANGED_DATE').cast(TimestampType())) \
       .withColumn('ADD_DATE', col('ADD_DATE').cast(TimestampType())) \
       .withColumn('DIRTY_FLAG', col('DIRTY_FLAG')) \
       .drop(col("CHANGED_DATE")) \
       .withColumn("LAUNCH_DATE", col('LAUNCH_DATE').cast(TimestampType()))\
       .withColumn("DW_HASH", unhex("DW_HASH"))
df.display()



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


# COMMAND ----------

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("db_sil_temp_delta.dim_team_curr")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Code Ends
