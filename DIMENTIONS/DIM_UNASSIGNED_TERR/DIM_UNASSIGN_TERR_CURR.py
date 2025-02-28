# Databricks notebook source
from pyspark.sql.functions import col, lit, current_date, expr,when

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load Validation tables for CURR,FUTURE and PRIOR to perform validation

# COMMAND ----------

# MAGIC %md
# MAGIC #Current

# COMMAND ----------

# Process Current data
spark.sql("DROP TABLE IF EXISTS db_sil_temp_delta.DIM_SALES_FORCE_CURR")
df_curr = spark.sql("SELECT * FROM db_sil_temp_delta.IST_DIM_SALES_FORCE_CURR")

# COMMAND ----------

# Filter and write Current data
curr_table = df_curr.select("INTERNAL_TERR_ID", "TEAM_ID", "TERR_NAME", "TERR_ID",
                            lit("GENERAL").alias("UNASSIGN_TYPE"),
                            lit(None).cast("string").alias("NOTE"),
                            current_date().alias("UPDATE_DATE"),
                            expr("CURRENT_USER()").alias("UPDATE_USER")) \
                    .filter("UPPER(TERR_NAME) like '%UNASSIGNED%' or UPPER(TERR_NAME) LIKE '%WHITESPACE%'")

curr_table.write.option("compression", "snappy") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("db_sil_temp_delta.DIM_UNASSIGN_TERR_CURR")

# Validation for Current data
dim_team_curr = spark.read.table("db_sil_temp_delta.DIM_UNASSIGN_TERR_CURR_validation")
if dim_team_curr.count() != curr_table.count():
    raise ValueError("Counts are not equal: {} != {}".format(dim_team_curr.count(), curr_table.count()))
