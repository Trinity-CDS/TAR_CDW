# Databricks notebook source
from pyspark.sql.functions import col, lit, current_date, expr

# Allow creating managed tables using non-empty locations
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
# Disable Delta format check
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# COMMAND ----------

# Drop the existing target table if it exists
spark.sql("DROP TABLE IF EXISTS db_sil_temp_delta.DIM_SALES_FORCE_FUTURE")

# COMMAND ----------

# JDBC connection details
user = "TPITarsProcessManager"
password = "eytwbdkjof127@hfurHDOOQ096BVqwatues11AHDSK"
jdbcHostname = "tars-dev.database.windows.net"
jdbcPort = 1433
database = "DW_PROD"
dbName = "db_sil_temp_delta"

# List of tables to process
tables_list = ["IST_DIM_SALES_FORCE_FUTURE"]

# Load data from SQL Server and write to Delta tables
for table in tables_list:
    sqlServerDF = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={database};") \
        .option("databaseName", database) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", f"stage.{table}") \
        .load()

    sqlServerDF.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{dbName}.{table}")

    print(f"Created table: {table}")

# COMMAND ----------

from pyspark.sql.functions import col, when, lit


df = spark.sql("select * from db_sil_temp_delta.IST_DIM_SALES_FORCE_FUTURE")

# Replace 'NULL' string values with actual None values
columns = df.columns
for column in columns:
    df = df.withColumn(column, when(col(column) == 'NULL', None).otherwise(col(column)))


# Write the dataframe to a Parquet table
df.write.option("compression", "snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("parquet").saveAsTable("db_sil_temp_delta.DIM_SALES_FORCE_FUTURE")

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table db_sil_temp_delta.DIM_SALES_FORCE_FUTURE
