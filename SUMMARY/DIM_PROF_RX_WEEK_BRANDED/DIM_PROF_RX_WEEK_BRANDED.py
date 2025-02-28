# Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

from pyspark.sql.functions import concat, sha2, col, lit,current_timestamp,expr,current_date
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,BinaryType
from pyspark.sql.functions import col, when

# COMMAND ----------

# MAGIC %scala
# MAGIC val user = "TPITarsProcessManager"
# MAGIC val password ="TPITkr@ProjeM8nkger"
# MAGIC val jdbcHostname = "tars-prd.database.windows.net" 
# MAGIC val jdbcPort = 1433
# MAGIC val database ="DW_PROD"
# MAGIC
# MAGIC val tables_list: List[String] = List("FCT_IMS_XPO_OPH_PRIMARY")
# MAGIC val dbName="db_sil_temp_delta"
# MAGIC
# MAGIC tables_list.foreach { tableName =>
# MAGIC   spark.sql(s"DROP TABLE IF EXISTS $dbName.$tableName")
# MAGIC   println(s"Dropped table: $tableName")
# MAGIC }
# MAGIC
# MAGIC for (table <- tables_list){
# MAGIC     val sqlServerDF = spark.read
# MAGIC       .format("jdbc")
# MAGIC       .option("url", s"jdbc:sqlserver://${jdbcHostname}:1433;databaseName=${database};")
# MAGIC       .option("databaseName", database)
# MAGIC       .option("user", user)
# MAGIC       .option("password", password)
# MAGIC       .option("dbtable", s"prod.$table")
# MAGIC       .load()
# MAGIC
# MAGIC     sqlServerDF.write
# MAGIC       .format("delta")
# MAGIC       .mode(SaveMode.Overwrite) // Change the save mode if needed
# MAGIC       .saveAsTable(s"db_sil_temp_delta.$table"+"_servercopy")
# MAGIC
# MAGIC     println(s"Created table: $table")
# MAGIC }

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS db_sil_temp_delta.DIM_PROF_RX_WEEK_BRANDED

# COMMAND ----------

# # Load data from tables
fct_ims_xpo_oph_primary = spark.table("db_sil_temp_delta.FCT_IMS_XPO_OPH_PRIMARY_servercopy")
dim_prod = spark.table("db_sil_temp_delta.DIM_PROD")
dim_prof = spark.table("db_sil_temp_delta.DIM_PROF")
ist_current_mth = spark.table("db_sil_temp_delta.IST_CURRENT_MTH")

# COMMAND ----------

# Get the current week date
dtDataWeek = ist_current_mth.select("CURRENT_WEEK_IMS_XPO").collect()[0][0]

# COMMAND ----------

table = spark.sql("""create table db_sil_temp_delta.DIM_PROF_RX_WEEK_BRANDED as
SELECT PROF_ID, BRAND_ID, MIN(FIRST_RX_WEEK) AS FIRST_RX_WEEK, MIN(FIRST_RX_MTH) AS FIRST_RX_MTH, MAX(LAST_RX_WEEK) AS LAST_RX_WEEK, MAX(LAST_RX_MTH) AS LAST_RX_MTH, WRITER_TYPE, WRITER_TYPE_MTH, RX_DATA_DATE, REFRESH_DATE
FROM (
    SELECT 
        F.PROF_ID, 
        P.BRAND_ID,
        MIN(DATE_ID_WEEK) AS FIRST_RX_WEEK,
        MIN(DATE_ID_MTH) AS FIRST_RX_MTH,
        MAX(DATE_ID_WEEK) AS LAST_RX_WEEK,
        MAX(DATE_ID_MTH) AS LAST_RX_MTH,
        CAST(NULL AS STRING) AS WRITER_TYPE,
        CAST(NULL AS STRING) AS WRITER_TYPE_MTH,    
        '{dtDataWeek}' AS RX_DATA_DATE,
        CURRENT_DATE() AS REFRESH_DATE
    FROM db_sil_temp_delta.FCT_IMS_XPO_OPH_PRIMARY_servercopy AS F
    JOIN db_sil_temp_delta.DIM_PROD AS P ON
        F.INTERNAL_PROD_ID = P.INTERNAL_PROD_ID AND
        P.BRAND_NAME = 'XDEMVY'
    JOIN db_sil_temp_delta.DIM_PROF AS D ON
        F.PROF_ID = D.PROF_ID
    WHERE F.PROF_ID IS NOT NULL
    GROUP BY F.PROF_ID, P.BRAND_ID
) AS qry
GROUP BY PROF_ID, BRAND_ID, WRITER_TYPE, WRITER_TYPE_MTH, RX_DATA_DATE, REFRESH_DATE
""".format(dtDataWeek=dtDataWeek))

# COMMAND ----------

# MAGIC %md
# MAGIC Update the columns

# COMMAND ----------

update = spark.sql("select * from db_sil_temp_delta.DIM_PROF_RX_WEEK_BRANDED")

# COMMAND ----------

# Update WRITER_TYPE and WRITER_TYPE_MTH columns
updated_df = update.withColumn(
    "WRITER_TYPE",
    when(col("FIRST_RX_WEEK") == col("LAST_RX_WEEK"), "NEW")
    .when(col("FIRST_RX_WEEK") < col("LAST_RX_WEEK"), "REPEAT")
    .otherwise(col("WRITER_TYPE"))
).withColumn(
    "WRITER_TYPE_MTH",
    when(col("FIRST_RX_MTH") == col("LAST_RX_MTH"), "NEW")
    .when(col("FIRST_RX_MTH") < col("LAST_RX_MTH"), "REPEAT")
    .otherwise(col("WRITER_TYPE_MTH"))
)


# COMMAND ----------

updated_df.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_PROF_RX_WEEK_BRANDED")

# COMMAND ----------

# MAGIC %md
# MAGIC VALIDATION

# COMMAND ----------

Snowflake_final_DIM_PROF_ATTRIBUTE = spark.sql("""
    select *
    from db_sil_temp_delta.DIM_PROF_RX_WEEK_BRANDED
""").drop("RX_DATA_DATE", "REFRESH_DATE")

server_final_DIM_PROF_ATTRIBUTE = spark.sql("""
    select *
    from db_sil_temp_delta.DIM_PROF_RX_WEEK_BRANDED_servercopy
""").drop("RX_DATA_DATE","REFRESH_DATE")

common_columns = Snowflake_final_DIM_PROF_ATTRIBUTE.columns
Snowflake_final_DIM_PROF_ATTRIBUTE = Snowflake_final_DIM_PROF_ATTRIBUTE.select(common_columns)
server_final_DIM_PROF_ATTRIBUTE = server_final_DIM_PROF_ATTRIBUTE.select(common_columns)

# COMMAND ----------

Snowflake_final_DIM_PROF_ATTRIBUTE.exceptAll(server_final_DIM_PROF_ATTRIBUTE).display()
server_final_DIM_PROF_ATTRIBUTE.exceptAll(Snowflake_final_DIM_PROF_ATTRIBUTE).display()

# COMMAND ----------


