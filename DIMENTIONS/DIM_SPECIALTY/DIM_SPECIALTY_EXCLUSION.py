# Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

from pyspark.sql.functions import concat, sha2, col, lit,current_timestamp,expr,current_date
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,BinaryType


# COMMAND ----------

# MAGIC %md
# MAGIC # Fetching the source table from SQL Server

# COMMAND ----------

df = spark.sql("select * from db_sil_temp_delta.SRC_SPECIALTY_EXCLUSION")
table_name = "SRC_SPECIALTY_EXCLUSION"
column_name = "DW_HASH"
binary_length = 32

if column_name in df.columns:
    # Drop 
    df = df.drop(column_name)

# Recreate 
df = df.withColumn(column_name, F.lit(" ").cast(BinaryType()))


df = df.withColumn(
    "DW_HASH",
    sha2(expr("concat('|',SPEC_CD, SPEC_DESC, SPEC_GRP_CD,SPEC_GRP_DESC,MOVE_TO_TERR,TEAM_ID,MARKET_ID)"), 256)
)


df.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.SRC_SPECIALTY_EXCLUSION") 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Casting Dtype to avoid conflict

# COMMAND ----------

DIM_SPECIALTY_query = """
SELECT
    CAST(INTERNAL_EXCLUSION_ID AS string) AS INTERNAL_EXCLUSION_ID,
    CAST(SPEC_CD AS string) AS SPEC_CD,
    CAST(SPEC_DESC AS string) AS SPEC_DESC,
    CAST(SPEC_GRP_CD AS string) AS SPEC_GRP_CD,
    CAST(SPEC_GRP_DESC AS string) AS SPEC_GRP_DESC,
    CAST(MOVE_TO_TERR AS string) AS MOVE_TO_TERR,
    CAST(TEAM_ID AS string) AS TEAM_ID,
    CAST(MARKET_ID AS string) AS MARKET_ID,
    CAST(DW_HASH AS string) AS DW_HASH,
    
    CAST(CHANGE_DATE AS string) AS CHANGE_DATE,
    CAST(DIRTY_FLAG AS string) AS DIRTY_FLAG
FROM db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION_servercopy;

"""

DIM_SPECIALTY = spark.sql(DIM_SPECIALTY_query)

DIM_SPECIALTY.write.option("compression", "snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION")
# DIM_SPECIALTY.display()

# COMMAND ----------

df=spark.read.table("db_sil_temp_delta.SRC_SPECIALTY_EXCLUSION")
df = df.withColumn("DW_HASH", F.col("DW_HASH").cast("string"))

# COMMAND ----------

df.write.option("compression", "snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.SRC_SPECIALTY_EXCLUSION")

# COMMAND ----------

update_query_sql = spark.sql("""
    MERGE INTO db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION AS D
    USING db_sil_temp_delta.SRC_SPECIALTY_EXCLUSION AS V
    ON D.SPEC_CD = V.SPEC_CD AND D.TEAM_ID = V.TEAM_ID AND D.DW_HASH != V.DW_HASH
    WHEN MATCHED THEN
    UPDATE SET 
        D.SPEC_DESC = V.SPEC_DESC,
        D.SPEC_GRP_CD = V.SPEC_GRP_CD,
        D.SPEC_GRP_DESC = V.SPEC_GRP_DESC,
        D.MOVE_TO_TERR = V.MOVE_TO_TERR,
        D.TEAM_ID = V.TEAM_ID,
        D.MARKET_ID = V.MARKET_ID,
        D.DW_HASH = V.DW_HASH,
        D.DIRTY_FLAG = 1,
        D.CHANGE_DATE = current_date()
""")

# COMMAND ----------

# dim_team_prior = spark.table("db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION")
# src_team_prior = spark.table("db_sil_temp_delta.SRC_SPECIALTY_EXCLUSION")

# updated_data = dim_team_prior.join(
#     src_team_prior,
#     (dim_team_prior.SPEC_CD == src_team_prior.SPEC_CD) & (dim_team_prior.TEAM_ID == src_team_prior.TEAM_ID) & (dim_team_prior.DW_HASH != src_team_prior.DW_HASH),
#     "inner"
# ).select(
#     src_team_prior.SPEC_CD,
#     src_team_prior.SPEC_DESC,
#     src_team_prior.SPEC_GRP_CD.alias("SPEC_GRP_CD"),
#     src_team_prior.SPEC_GRP_DESC.alias("SPEC_GRP_DESC"),
#     src_team_prior.MOVE_TO_TERR.alias("MOVE_TO_TERR"),
#     src_team_prior.TEAM_ID.alias("TEAM_ID"),
#     src_team_prior.MARKET_ID.alias("MARKET_ID"),
#     src_team_prior.DW_HASH.alias("DW_HASH"),
#     lit("1").alias("DIRTY_FLAG"),
#     current_date().alias("CHANGE_DATE")
# ).dropDuplicates()




# updated_data.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION") 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform the INSERT operation

# COMMAND ----------

spark.sql("""
 INSERT INTO db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION
(
    SPEC_CD,
    SPEC_DESC,
    SPEC_GRP_CD,
    SPEC_GRP_DESC,
    MOVE_TO_TERR,
    TEAM_ID,
    MARKET_ID,
    DW_HASH,
    DIRTY_FLAG,
    CHANGE_DATE
)
SELECT
    P.SPEC_CD,
    P.SPEC_DESC,
    P.SPEC_GRP_CD,
    P.SPEC_GRP_DESC,
    P.MOVE_TO_TERR,
    P.TEAM_ID,
    P.MARKET_ID,
    P.DW_HASH,
    1 AS DIRTY_FLAG,
    CURRENT_DATE() AS CHANGE_DATE
FROM
    db_sil_temp_delta.SRC_SPECIALTY_EXCLUSION P
WHERE
    P.SPEC_CD NOT IN (SELECT SPEC_CD FROM db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION E WHERE E.TEAM_ID = P.TEAM_ID);

 """)

# COMMAND ----------

spark.sql("""UPDATE db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION
SET DIRTY_FLAG = CASE WHEN DIRTY_FLAG = true THEN 1 ELSE 0 END;
""")

# COMMAND ----------

abc=spark.read.table("db_sil_temp_delta.SRC_SPECIALTY_EXCLUSION")
abc.createOrReplaceTempView("SRC_SPECIALTY_EXCLUSION")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Delete records not in the source table

# COMMAND ----------

target_table=spark.read.table("db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION")
target_table.createOrReplaceTempView("DIM_SPECIALTY_EXCLUSION")
spark.sql("""
    DELETE FROM DIM_SPECIALTY_EXCLUSION
    WHERE DW_HASH NOT IN (SELECT DW_HASH FROM SRC_SPECIALTY_EXCLUSION)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update DIRTY_FLAG in DIM_TEAM_prior

# COMMAND ----------

spark.sql("""
    UPDATE DIM_SPECIALTY_EXCLUSION
    SET DIRTY_FLAG = 0
    WHERE DIRTY_FLAG = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a window specification based on the order of SPEC_CD values

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.orderBy(F.col("TEAM_ID").desc(),F.col("SPEC_CD").asc())


target_table = target_table.withColumn(
    "INTERNAL_EXCLUSION_ID",
    (10000000 + F.row_number().over(window_spec) - 1).cast("string")
)


# COMMAND ----------

from pyspark.sql.types import DecimalType, TimestampType, BooleanType
target_table=target_table.withColumn("ADD_DATE", lit(None).cast("date")) \
                           .withColumn('DIRTY_FLAG', F.col('DIRTY_FLAG').cast(BooleanType()))



# COMMAND ----------

target_table =  target_table.select(
"INTERNAL_EXCLUSION_ID",
"SPEC_CD",
"SPEC_DESC",
"SPEC_GRP_CD",
"SPEC_GRP_DESC",
"MOVE_TO_TERR",
"TEAM_ID",
"MARKET_ID",
"DW_HASH",
"ADD_DATE",
"CHANGE_DATE",
"DIRTY_FLAG")

# COMMAND ----------

target_table.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_SPECIALTY_EXCLUSION")
