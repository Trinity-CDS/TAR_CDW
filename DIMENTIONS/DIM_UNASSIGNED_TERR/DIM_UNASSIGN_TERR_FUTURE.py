# Databricks notebook source
from pyspark.sql.functions import col, lit, current_date, expr,when

# COMMAND ----------

# MAGIC %md
# MAGIC #FUTURE

# COMMAND ----------

# Process Future data
spark.sql("DROP TABLE IF EXISTS db_sil_temp_delta.DIM_SALES_FORCE_FUTURE")
df_future = spark.sql("SELECT * FROM db_sil_temp_delta.IST_DIM_SALES_FORCE_FUTURE")

# Filter and write Future data
future_table = df_future.select("INTERNAL_TERR_ID", "TEAM_ID", "TERR_NAME", "TERR_ID",
                                lit("GENERAL").alias("UNASSIGN_TYPE"),
                                lit(None).cast("string").alias("NOTE"),
                                current_date().alias("UPDATE_DATE"),
                                expr("CURRENT_USER()").alias("UPDATE_USER")) \
                        .filter("UPPER(TERR_NAME) like '%UNASSIGNED%' or UPPER(TERR_NAME) LIKE '%WHITESPACE%'")

future_table.write.option("compression", "snappy") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("db_sil_temp_delta.DIM_UNASSIGN_TERR_FUTURE")

# Validation for Future data
dim_team_future = spark.read.table("db_sil_temp_delta.DIM_UNASSIGN_TERR_FUTURE_validation")
if dim_team_future.count() != future_table.count():
    raise ValueError("Counts are not equal: {} != {}".format(dim_team_future.count(), future_table.count()))
