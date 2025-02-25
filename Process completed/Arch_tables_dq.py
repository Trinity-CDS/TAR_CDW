# Databricks notebook source
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")

# COMMAND ----------

# MAGIC %pip install typing_extensions --upgrade

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp

# COMMAND ----------

table_names = [
    "src_sales_roster_curr",
    "mdv_dw_bldgid",
    "mdv_dw_bluegrot_pro",
    "mdv_dw_ims_split_weeks",
    "mdv_dw_ims_xpo",
    "mdv_dw_intchain",
    "mdv_dw_locid",
    "mdv_dw_orgid",
    "mdv_dw_profid",
    "mdv_dw_sfdc",
    "oce__accountaddress__c",
    "oce__call__c",
    "oce__emailtemplate__c",
    "oce__emailtransaction__c",
    "oce__inquiry__c",
    "oce__inquiry__c_test",
    "oce__inquiryquestion__c",
    "rc_decile_definition",
    "recordtype",
    "src_age_grp_definition",
    "src_bi_override",
    "src_company_holiday",
    "src_decile",
    "src_decile_definition",
    "src_employee",
    "src_org_override_curr",
    "src_org_override_future",
    "src_org_override_prior",
    "src_org_target_curr",
    "src_org_target_prior",
    "src_org_targets_curr",
    "src_org_targets_future",
    "src_org_targets_prior",
    "src_payer_model_bridge",
    "src_prod_brand",
    "src_prof_override_curr",
    "src_prof_override_future",
    "src_prof_override_prior",
    "src_prof_targets_curr",
    "src_prof_targets_future",
    "src_prof_targets_prior",
    "src_rlt_external_prod",
    "src_sales_goal_curr",
    "src_sales_roster_future",
    "src_sales_roster_prior",
    "src_spec_specgrp",
    "src_specialty_exclusion",
    "src_team_curr",
    "src_team_future",
    "src_team_prior",
    "src_trialcard_pbm",
    "src_zip_terr_curr",
    "src_zip_terr_future",
    "src_zip_terr_prior",
    "territory2",
    "territory2model",
    "user",
    "userterritory2association"
]


# COMMAND ----------

# Remove tables ending with specified suffixes
suffixes_to_remove = ['_val', '_sql_copy', '_snowflakecopy', '_arch', '_july3rd', '_july7th', '_test','_hist']
table_names_filtered = [name for name in table_names if not any(name.endswith(suffix) for suffix in suffixes_to_remove)]

for table_name in table_names_filtered:
    try:
        df = spark.read.table(f'cdw_raw.{table_name}')
        df_with_time_columns = df.withColumn("etl_Create_date", current_timestamp())
        try:
            
            dbutils.fs.rm(f"dbfs:/mnt/iro/CDW_RAW/{table_name.lower()}_arch", recurse=True)
            spark.sql(f"truncate table cdw_raw.{table_name}_arch")
        except Exception as e:
            print(f'{table_name}')

        df_with_time_columns.write.option("compression", "snappy") \
                              .option("overwriteSchema", "true") \
                              .option("mergeSchema", "true") \
                              .mode("overwrite") \
                              .format("parquet") \
                              .saveAsTable(f"cdw_raw.{table_name}_arch")

        print(f"Backup created for table: {table_name}")
    except Exception as e:
        print(f"Table {table_name} does not exist in path /mnt/iro/Stored_Proc/ or another error occurred: {e}")

# COMMAND ----------

!pip install snowflake.connector.python
import snowflake.connector
import configparser

# COMMAND ----------

table_names_arch = [
    "CDW_RAW.SRC_TEAM_PRIOR_arch",
    "CDW_RAW.SRC_TEAM_CURR_arch",
    "CDW_RAW.SRC_TEAM_FUTURE_arch",
    "CDW_RAW.SRC_SALES_ROSTER_PRIOR_arch",
    "CDW_RAW.SRC_SALES_ROSTER_CURR_arch",
    "CDW_RAW.SRC_SALES_ROSTER_FUTURE_arch",
    "CDW_RAW.SRC_EMPLOYEE_arch",
    "CDW_RAW.SRC_ZIP_TERR_CURR_arch",
    "CDW_RAW.SRC_ZIP_TERR_FUTURE_arch",
    "CDW_RAW.SRC_ZIP_TERR_PRIOR_arch",
    "CDW_RAW.SRC_PROD_BRAND_arch",
    "CDW_RAW.SRC_RLT_EXTERNAL_PROD_arch",
    "CDW_RAW.src_spec_specgrp_arch",
    "CDW_RAW.SRC_SPECIALTY_EXCLUSION_arch",
    # "CDW_RAW.IST_CURRENT_MTH_arch",
    "CDW_RAW.SRC_COMPANY_HOLIDAY_arch",
    "CDW_RAW.SRC_DECILE_DEFINITION_arch",
    "CDW_RAW.MDV_DW_PROFID_arch",
    "CDW_RAW.MDV_DW_ORGID_arch",
    "CDW_RAW.MDV_DW_BLDGID_arch",
    "CDW_RAW.MDV_DW_LOCID_arch"
]

# COMMAND ----------

config = configparser.ConfigParser()
config.read("/dbfs/mnt/iro/Ironshore/PROD/Config_files/config_sp.ini")
sf_user = config['Snowflake credentials']['user']
sf_pwrd = config['Snowflake credentials']['password']
 
 
def refresh_snowflake_external_table(table_name):
    snowflake_user = sf_user
    snowflake_password = sf_pwrd
    snowflake_account = "zj86325.west-us-2.azure"
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse="IRONSHORE_WH"
    )
 
    cursor = conn.cursor()
    sql = f"ALTER EXTERNAL TABLE IRONSHORE_CDW_PROD.{table_name} REFRESH;"
    cursor.execute(sql)
    print("snowflake table refresh successful.")
    conn.close()
 
 
for table_name in table_names_arch:
    refresh_snowflake_external_table(table_name)
    print(f"Refreshing table: {table_name}")

# COMMAND ----------

table_names = [
    "DIM_PROF",
    "DIM_ORG",
    "DIM_BLDG",
    "DIM_LOC",
    "DIM_PROF_ATTRIBUTE",
    "DIM_ORG_ATTRIBUTE",
    "DIM_ODW_ATTRIBUTE",
    "DIM_OPT_ATTRIBUTE",
    "DIM_SYSTEM_ATTRIBUTE",
    "FCT_SFDC_CALL_ACTIVITY",
    "FCT_TRIALCARD_CLAIM",
    "FCT_OCE_MIRF",
    "FCT_BLITZHEALTH_MEETING",
    "FCT_CARDINAL_SHIPMENT",
    "FCT_INTEGRICHAIN_852",
    "FCT_INTEGRICHAIN_SUBNAT",
    "FCT_INTEGRICHAIN_867",
    "FCT_IQVIA_FIA",
    "FCT_IQVIA_NPA_WEEK",
    "FCT_IQVIA_NPA_MONTH",
    "FCT_OCE_RTE",
    "FCT_SALES_GOAL",
    "FCT_IMS_XPO_ADHD_PRIMARY",
    "FCT_IQVIA_XPD",
    "DIM_TEAM_PRIOR",
    "DIM_TEAM_CURR",
    "DIM_TEAM_FUTURE",
    "DIM_SPECIALTY",
    "DIM_SALES_FORCE_PRIOR",
    "DIM_SALES_FORCE_CURR",
    "DIM_SALES_FORCE_FUTURE",
    "ZIP_TERR_CURR",
    "ZIP_TERR_FUTURE",
    "ZIP_TERR_PRIOR",
    "DIM_PROD",
    "FCT_BLUEGROT_ATTENDEE"
]

# # Add _ARCH to each table name
# table_names_arch = [table_name + "_ARCH" for table_name in table_names]



# COMMAND ----------

# Remove tables ending with specified suffixes
suffixes_to_remove = ['_val', '_sql_copy', '_snowflakecopy', '_arch', '_july3rd', '_july7th', '_test','_hist']
table_names_filtered = [name for name in table_names if not any(name.endswith(suffix) for suffix in suffixes_to_remove)]

for table_name in table_names_filtered:
    try:
        df = spark.read.table(f'cdw_curated.{table_name}')
        df_with_time_columns = df.withColumn("etl_Create_date", current_timestamp())
        try:
            
            dbutils.fs.rm(f"dbfs:/mnt/iro/cdw_curated/{table_name.lower()}_arch", recurse=True)
            spark.sql(f"truncate table cdw_curated.{table_name}_arch")
        except Exception as e:
            print(e)
            
        
        df_with_time_columns.write.option("compression", "snappy") \
                              .option("overwriteSchema", "true") \
                              .option("mergeSchema", "true") \
                              .mode("overwrite") \
                              .format("parquet") \
                              .saveAsTable(f"cdw_curated.{table_name}_arch")

        print(f"Backup created for table: {table_name}")
    except Exception as e:
        print(f"Table {table_name} does not exist in path /mnt/iro/Stored_Proc/ or another error occurred: {e}")

# COMMAND ----------

config = configparser.ConfigParser()
config.read("/dbfs/mnt/iro/Ironshore/PROD/Config_files/config_sp.ini")
sf_user = config['Snowflake credentials']['user']
sf_pwrd = config['Snowflake credentials']['password']
 
 
def refresh_snowflake_external_table(table_name):
    snowflake_user = sf_user
    snowflake_password = sf_pwrd
    snowflake_account = "zj86325.west-us-2.azure"
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse="IRONSHORE_WH"
    )
 
    cursor = conn.cursor()
    sql = f"ALTER EXTERNAL TABLE IRONSHORE_CDW_PROD.cdw_curated.{table_name}_ARCH REFRESH;"
    cursor.execute(sql)
    print("snowflake table refresh successful.")
    conn.close()
 
 
for table_name in table_names:
    refresh_snowflake_external_table(table_name)
    print(f"Refreshing table: {table_name}_ARCH")
