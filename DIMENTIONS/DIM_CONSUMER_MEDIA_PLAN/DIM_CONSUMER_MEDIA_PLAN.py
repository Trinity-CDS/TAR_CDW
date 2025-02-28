# Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

from pyspark.sql.functions import concat, sha2, col, lit,current_timestamp,expr,current_date
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,BinaryType

# COMMAND ----------

# MAGIC %scala
# MAGIC val user = "TPITarsProcessManager"
# MAGIC val password ="TPITkr@ProjeM8nkger"
# MAGIC val jdbcHostname = "tars-prd.database.windows.net" 
# MAGIC val jdbcPort = 1433
# MAGIC val database ="DW_PROD"
# MAGIC
# MAGIC val tables_list: List[String] = List("IST_VEEVA_CROSSIX_CONSUMER_DIGITAL_MEDIA_PLAN")
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
# MAGIC       .option("dbtable", s"terra.$table")
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
# MAGIC DROP TABLE IF EXISTS db_sil_temp_delta.DIM_CONSUMER_MEDIA_PLAN

# COMMAND ----------

select_query = spark.sql("""CREATE TABLE db_sil_temp_delta.DIM_CONSUMER_MEDIA_PLAN AS SELECT 'VEEVA-CROSSIX' AS RECORD_SOURCE
			--, 'CROSSIX' AS RECORD_TYPE
			, MEDIA_ID AS SRC_MEDIA_ID 
			, CAMPAIGN AS SRC_CAMPAIGN 
			, DCM_CAMPAIGN_NAME AS DCM_CAMPAIGN_NAME 
			, PUBLISHER AS SRC_PUBLISHER
			, PLACEMENT_NAME AS SRC_PLACEMENT_NAME
			, PLACEMENT_DESCRIPTION AS SRC_PLACEMENT_DESCRIPTION
			, PLACEMENT_GROUP AS SRC_PLACEMENT_GROUP
			, TACTIC AS SRC_TACTIC
			, AUDIENCE AS SRC_AUDIENCE
			, AD_TYPE AS SRC_AD_TYPE
			, PLACEMENT_TYPE AS SRC_PLACEMENT_TYPE
			, CREATIVE_ID AS SRC_CREATIVE_ID
			, INVENTORY_LOCATION AS SRC_INVENTORY_LOCATION
			, AD_SERVER AS SRC_AD_SERVER
			, AD_UNIT_SIZE AS SRC_AD_UNIT_SIZE
			, PRICING_TYPE AS SRC_PRICING_TYPE
			, FLIGHT_START_DATE AS SRC_FLIGHT_START_DATE
			, FLIGHT_END_DATE AS SRC_FLIGHT_END_DATE
			, IMPRESSIONS AS SRC_IMPRESSIONS
			, INCLUDES_RICH_MEDIA AS SRC_INCLUDES_RICH_MEDIA
			, PRIMARY_CONTACT AS SRC_PRIMARY_CONTACT
			, SOURCE AS SRC_SOURCE_ENTRY
			, CREATION_DATE
			, DIRECT_FEED
			, GETDATE() AS INSERT_DATE
		FROM db_sil_temp_delta.IST_VEEVA_CROSSIX_CONSUMER_DIGITAL_MEDIA_PLAN_servercopy""")


# COMMAND ----------

add_identity_column = spark.sql("select * from db_sil_temp_delta.DIM_CONSUMER_MEDIA_PLAN")
add_identity_column = add_identity_column.withColumn("MEDIA_PLAN_ID", F.monotonically_increasing_id() + lit(10000000))

# COMMAND ----------

add_identity_column.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("db_sil_temp_delta.DIM_CONSUMER_MEDIA_PLAN")
