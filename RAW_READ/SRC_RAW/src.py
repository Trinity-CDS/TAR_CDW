# Databricks notebook source
# import configparser
# config = configparser.ConfigParser()
# config.read("dbfs:/FileStore/tar_config_sp.ini")
# print(config.sections())
# for section in config.sections():
#     for key in config[section]:
#         print(f"{section}.{key} = {config[section][key]}")

# COMMAND ----------

# account_name="tarsusstorageaccount"
# container_name="cdstarsuslz"
# sas_token_lz = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-02-27T16:47:35Z&st=2025-02-27T08:47:35Z&spr=https&sig=fnFsSv5bdYQxlZ1r2zQalmq2pdv%2FUGpowWq%2FSPOKMFc%3D"

# spark.conf.set(f"fs.azure.sas.{container_name}.{account_name}.blob.core.windows.net", sas_token_lz)

# path = "/"

# files_df = spark.read.format("binaryFile").load(f"wasbs://{container_name}@{account_name}.blob.core.windows.net/{path}")
# display(files_df)

# COMMAND ----------

tables_list = [
    "BAK_PROF_OVERRIDE_PRIOR",
    "SRC_PROF_OVERRIDE_PRIOR",
    "src_prof_override_curr_bkp",
    "SRC_PROF_TARGETS_CURR_BKP",
    "BAK_PROF_TARGETS_CURR",
    "SRC_ZIP_TERR_CURR_BKP",
    "SRC_PROF_TARGETS_CURR",
    "MD_LOG",
    "BAK_ZIP_TERR_CURR",
    "BAK_TEAM_CURR",
    "BAK_SPEC_SPECGRP",
    "BAK_PROF_TARGETS_FUTURE",
    "SRC_PROF_TARGETS_FUTURE",
    "SRC_LAUNCH_PANEL_EMAIL_IQVIA",
    "SRC_TEAM_CURR",
    "SRC_TEAM_FUTURE",
    "SRC_TEAM_PRIOR",
    "SRC_ATL_TERRITORY_MAPPING_CURR",
    "SRC_LAUNCH_PANEL_EMAIL",
    "BAK_COMPANY_HOLIDAY",
    "SRC_COMPANY_HOLIDAY",
    "BAK_IQVIA_EMAIL_ATTRIBUTE",
    "SRC_PROF_TARGETS_PRIOR",
    "SRC_IQVIA_EMAIL_ATTRIBUTE",
    "SRC_ZIP_TERR_CURR",
    "SRC_ZIP_TERR_PRIOR",
    "BAK_PROF_ATTRIBUTE",
    "SRC_PROF_ATTRIBUTE",
    "SRC_SPEC_SPECGRP_SPH",
    "SRC_EMPLOYEE",
    "SRC_ZIP_TERR_PRIOR_2024_Q1",
    "SRC_SPEC_SPECGRP",
    "SRC_ORG_OVERRIDE_CURR",
    "SRC_ORG_OVERRIDE_FUTURE",
    "SRC_ORG_OVERRIDE_PRIOR",
    "BAK_PROD_BRAND",
    "SRC_PROD_BRAND",
    "SRC_SPECIALTY_EXCLUSION",
    "SRC_PROF_OVERRIDE_FUTURE",
    "SRC_SALES_ROSTER_FUTURE",
    "SRC_SALES_ROSTER_PRIOR",
    "SRC_PROF_TARGETS_FUTURE_backup_Dup",
    "SRC_PROF_OVERRIDE_CURR_SDB",
    "SRC_FRM_TARGETS",
    "SRC_FRM_TEAM_ROSTER",
    "SRC_FRM_ZIP_TERR",
    "SRC_ZIP_TERR_FUTURE_TM1",
    "BAK_TARSUS_SPECIALTY_MAPPING",
    "BAK_PROF_OVERRIDE_CURR",
    "SRC_TARSUS_SPECIALTY_MAPPING",
    "SRC_TARGET_LIST",
    "SRC_ATL_TERRITORY_MAPPING_PRIOR",
    "SRC_ATL_TERRITORY_MAPPING_FUTURE",
    "SRC_ZIP_TERR_FUTURE",
    "SRC_ZIP_TERR_INSIDE_SALES",
    "SRC_DAILY_FORECAST_TRX",
    "src_prof_override_curr",
    "SRC_ZIP_TERR_INSIDE_SALES_PRIOR",
    "SRC_IMS_CHANNEL_MAP",
    "BAK_RLT_EXTERNAL_PROD",
    "SRC_FRM_FTL_TERR",
    "SRC_RLT_EXTERNAL_PROD",
    "BAK_SALES_ROSTER_CURR",
    "SRC_SALES_ROSTER_CURR",
    "SRC_DECILE",
    "SRC_DECILE_DEFINITION",
    "SRC_COMPANY_HOLIDAY",
    "SRC_TEAM_CURR",
    "SRC_TEAM_FUTURE",
    "SRC_TEAM_PRIOR",
    "SRC_ZIP_TERR_CURR",
    "SRC_ZIP_TERR_FUTURE",
    "SRC_ZIP_TERR_PRIOR",
    "SRC_PROD_BRAND",
    "SRC_RLT_EXTERNAL_PROD",
    "SRC_DECILE",
    "SRC_DECILE_DEFINITION",
    "SRC_SALES_ROSTER_CURR",
    "SRC_EMPLOYEE",
    "SRC_SPEC_SPECGRP",
    "SRC_SPECIALTY_EXCLUSION"
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
        .option("dbtable", f"mdmistprod.{table}") \
        .load()
 
    # Write data to Delta table
    sql_server_df.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable(f"db_sil_temp_delta.{table}")
 
    print(f"Created table: {table}")
