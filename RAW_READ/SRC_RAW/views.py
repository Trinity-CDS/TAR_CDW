# Databricks notebook source
tables_list = [
    "MDV_DW_IMS_XPO",
    "MDV_DW_IMS_PDRP",
    "MDV_DW_IMS_SPLIT_WEEKS",
    "MDV_DW_ORGID_AFFILIATIONS",
    "MDV_DW_IMS_UIDSPEC",
    "MDV_DW_SFDC",
    "MDV_DW_NPI_STLIC",
    "MDV_DW_SOURCE_STATUS",
    "MDV_DW_ODW",
    "MDV_DW_LOCID",
    "MDV_DW_BLDGID",
    "MDV_DW_PROFID",
    "MDV_DW_ONEKEY_PRO",
    "MDV_DW_DEA_ORG",
    "MDV_DW_ORGID",
    "MDV_DW_CAIO_ORG",
    "MDV_DW_CAIP_PRO",
    "MDV_DW_REFRESH",
    "MDV_DW_LAAD_PRO",
    "MDV_DW_AMA_PDRP",
    "MDV_DW_ORGID","MDV_DW_BLDGID","MDV_DW_LOCID","MDV_DW_PROFID"
]

# COMMAND ----------

jdbcHostname = "tars-prd.database.windows.net"
database = "DW_PROD"
user = "TPITarsProcessManager"
password = "TPITkr@ProjeM8nkger"
jdbcPort = 1433
 
dbName = "db_sil_temp_delta"
 
# Loop through the tables_list
for tableName in tables_list:
    # Drop table if exists
    spark.sql(f"DROP TABLE IF EXISTS {dbName}.{tableName}")
    print(f"Dropped table: {tableName}")
 
for table in tables_list:
    # Read data from SQL Server
    sql_server_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://{jdbcHostname}:1433;databaseName={database};") \
        .option("databaseName", database) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", f"views.{table}") \
        .load()
 
    # Write data to Delta table
    sql_server_df.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable(f"db_sil_temp_delta.{table}")
 
    print(f"Created table: {table}")
