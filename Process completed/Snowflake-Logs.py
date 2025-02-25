# Databricks notebook source
from pyspark.sql.functions import lit

# COMMAND ----------

raw_tables = [
    "db_sil_temp_delta.account",
    "db_sil_temp_delta.cfg_alignment_rule",
    "db_sil_temp_delta.cfg_alignment_rule_key",
    "db_sil_temp_delta.cfg_alignment_type",
    "db_sil_temp_delta.cfg_dimension_dirty_rule",
    "db_sil_temp_delta.cfg_dynamic_time_anchor_groups",
    "db_sil_temp_delta.cfg_sales_roster_columns",
    "db_sil_temp_delta.cfg_sales_roster_hierarchy",
    "db_sil_temp_delta.dim_specialty",
    "db_sil_temp_delta.ist_dim_sales_force_curr",
    "db_sil_temp_delta.ist_dim_sales_force_future",
    "db_sil_temp_delta.ist_dim_sales_force_prior",
    "db_sil_temp_delta.ist_fct_sfdc_call_activity_calls",
    "db_sil_temp_delta.ist_ims_xpo_fake_prof",
    "db_sil_temp_delta.ist_iqvia_email_combined",
    "db_sil_temp_delta.ist_rlt_odw_decile",
    "db_sil_temp_delta.ist_rlt_opt_decile",
    "db_sil_temp_delta.ist_rlt_org_decile",
    "db_sil_temp_delta.ist_rlt_prof_decile",
    "db_sil_temp_delta.ist_rlt_sys_decile",
    "db_sil_temp_delta.mdv_dw_bldgid",
    "db_sil_temp_delta.mdv_dw_bluegrot_pro",
    "db_sil_temp_delta.mdv_dw_ims_split_weeks",
    "db_sil_temp_delta.mdv_dw_ims_xpo",
    "db_sil_temp_delta.mdv_dw_intchain",
    "db_sil_temp_delta.mdv_dw_locid",
    "db_sil_temp_delta.mdv_dw_orgid",
    "db_sil_temp_delta.mdv_dw_profid",
    "db_sil_temp_delta.mdv_dw_sfdc",
    "db_sil_temp_delta.oce__accountaddress__c",
    "db_sil_temp_delta.oce__call__c",
    "db_sil_temp_delta.oce__emailtemplate__c",
    "db_sil_temp_delta.oce__emailtransaction__c",
    "db_sil_temp_delta.oce__inquiry__c",
    "db_sil_temp_delta.oce__inquiryquestion__c",
    "db_sil_temp_delta.rc_decile_definition",
    "db_sil_temp_delta.recordtype",
    "db_sil_temp_delta.src_age_grp_definition",
    "db_sil_temp_delta.src_bi_override",
    "db_sil_temp_delta.src_company_holiday",
    "db_sil_temp_delta.src_decile",
    "db_sil_temp_delta.src_decile_definition",
    "db_sil_temp_delta.src_employee",
    "db_sil_temp_delta.src_org_override_curr",
    "db_sil_temp_delta.src_org_override_future",
    "db_sil_temp_delta.src_org_override_prior",
    "db_sil_temp_delta.SRC_ORG_TARGETS_CURR",
    "db_sil_temp_delta.src_org_targets_prior",
    "db_sil_temp_delta.src_org_targets_curr",
    "db_sil_temp_delta.src_org_targets_future",
    "db_sil_temp_delta.src_org_targets_prior",
    "db_sil_temp_delta.src_payer_model_bridge",
    "db_sil_temp_delta.src_prod_brand",
    "db_sil_temp_delta.src_prof_override_curr",
    "db_sil_temp_delta.src_prof_override_future",
    "db_sil_temp_delta.src_prof_override_prior",
    "db_sil_temp_delta.src_prof_targets_curr",
    "db_sil_temp_delta.src_prof_targets_future",
    "db_sil_temp_delta.src_prof_targets_prior",
    "db_sil_temp_delta.src_rlt_external_prod",
    "db_sil_temp_delta.src_sales_goal_curr",
    "db_sil_temp_delta.src_sales_roster_curr",
    "db_sil_temp_delta.src_sales_roster_future",
    "db_sil_temp_delta.src_sales_roster_prior",
    "db_sil_temp_delta.src_spec_specgrp",
    "db_sil_temp_delta.src_specialty_exclusion",
    "db_sil_temp_delta.src_team_curr",
    "db_sil_temp_delta.src_team_future",
    "db_sil_temp_delta.src_team_prior",
    "db_sil_temp_delta.src_trialcard_pbm",
    "db_sil_temp_delta.src_zip_terr_curr",
    "db_sil_temp_delta.src_zip_terr_future",
    "db_sil_temp_delta.src_zip_terr_prior",
    "db_sil_temp_delta.user"
]


# COMMAND ----------

from pyspark.sql.functions import lit

senario = "Raw"
Environment = "PROD"
all_data = []  


for table in raw_tables:
    df = spark.sql(f"describe history {table}").limit(1)
    table = table.replace("db_sil_temp_delta.", "")
    
   
    df = df.withColumn("Client", lit("IRO")) \
           .withColumn("Environment", lit(Environment)) \
           .withColumn("Senorio_Name", lit(senario)) \
           .withColumn("Table_name", lit(table)) \
           .withColumnRenamed("timestamp", "Load_Time") \
           .withColumnRenamed("userName", "Run_By") \
           .select("Client", "Environment", "Senorio_Name", "Table_name", "Load_Time", "Run_By")
    
    # Append each DataFrame to the list
    all_data.append(df)


final_df = all_data[0]
for df in all_data[1:]:
    final_df = final_df.unionByName(df)

# Write the final combined DataFrame
final_df.write.option("compression", "snappy") \
              .option("overwriteSchema", "true") \
              .option("mergeSchema", "true") \
              .mode("append") \
              .format("parquet") \
              .saveAsTable("CDW_CURATED.SP_LOGS_PROD")


# COMMAND ----------

curated_tables = [
    "db_sil_temp_delta.dim_alignment_curr",
    "db_sil_temp_delta.dim_alignment_future",
    "db_sil_temp_delta.dim_alignment_prior",
    "db_sil_temp_delta.dim_bldg",
    "db_sil_temp_delta.dim_customer",
    "db_sil_temp_delta.dim_customer_bi_filtered",
    "db_sil_temp_delta.dim_date",
    "db_sil_temp_delta.dim_date_flags",
    "db_sil_temp_delta.dim_decile",
    "db_sil_temp_delta.dim_decile_definition",
    "db_sil_temp_delta.dim_employee",
    "db_sil_temp_delta.dim_loc",
    "db_sil_temp_delta.dim_mmit_org",
    "db_sil_temp_delta.dim_odw_attribute",
    "db_sil_temp_delta.dim_opt_attribute",
    "db_sil_temp_delta.dim_org",
    "db_sil_temp_delta.dim_org_attribute",
    "db_sil_temp_delta.dim_payer_plan",
    "db_sil_temp_delta.dim_prod",
    "db_sil_temp_delta.dim_prof",
    "db_sil_temp_delta.dim_prof_attribute",
    "db_sil_temp_delta.dim_prof_decile",
    "db_sil_temp_delta.dim_sfa_customer",
    "db_sil_temp_delta.dim_sfa_employee",
    "db_sil_temp_delta.dim_specialty",
    "db_sil_temp_delta.dim_specialty_exclusion",
    "db_sil_temp_delta.dim_system_attribute",
    "db_sil_temp_delta.dim_team_curr",
    "db_sil_temp_delta.dim_team_future",
    "db_sil_temp_delta.dim_team_prior",
    "db_sil_temp_delta.dim_terr_attribute",
    "db_sil_temp_delta.fct_blitzhealth_meeting",
    "db_sil_temp_delta.fct_bluegrot_attendee",
    "db_sil_temp_delta.fct_cardinal_shipment",
    "db_sil_temp_delta.fct_ims_xpo_adhd_primary",
    "db_sil_temp_delta.fct_integrichain_852",
    "db_sil_temp_delta.fct_integrichain_867",
    "db_sil_temp_delta.fct_integrichain_subnat",
    "db_sil_temp_delta.fct_iqvia_fia",
    "db_sil_temp_delta.fct_iqvia_npa_month",
    "db_sil_temp_delta.fct_iqvia_npa_mth",
    "db_sil_temp_delta.fct_iqvia_npa_week",
    "db_sil_temp_delta.fct_iqvia_xpd",
    "db_sil_temp_delta.fct_iqvia_xpd_add",
    "db_sil_temp_delta.fct_iqvia_xpd_restart",
    "db_sil_temp_delta.fct_iqvia_xpd_switch",
    "db_sil_temp_delta.fct_mmit_state_lives",
    "db_sil_temp_delta.fct_oce_mirf",
    "db_sil_temp_delta.fct_oce_rte",
    "db_sil_temp_delta.fct_sales_goal",
    "db_sil_temp_delta.fct_sfdc_call_activity",
    "db_sil_temp_delta.fct_trialcard_claim",
    "db_sil_temp_delta.fsdb_hcp_overview",
    "db_sil_temp_delta.ist_blitz_health_raw",
    "db_sil_temp_delta.ist_bluegrotto_mdm_stage",
    "db_sil_temp_delta.ist_current_mth",
    "db_sil_temp_delta.ist_ims_xpo_fake_prof",
    "db_sil_temp_delta.ist_integrichain_mdm_stage",
    "db_sil_temp_delta.ist_iqvia_stage_xpo_weekly",
    "db_sil_temp_delta.ist_trialcard_mdm_stage",
    "db_sil_temp_delta.rlt_external_product",
   
    # "db_sil_temp_delta.rlt_opt_decile",
    # "db_sil_temp_delta.rlt_org_decile",
    # "db_sil_temp_delta.rlt_prof_decile",
    # "db_sil_temp_delta.rlt_sys_decile",
    
    "db_sil_temp_delta.zip_terr_curr",
    "db_sil_temp_delta.zip_terr_future",
    "db_sil_temp_delta.zip_terr_prior"
]


# COMMAND ----------

from pyspark.sql.functions import lit

senario = "Curated"
Environment = "PROD"
all_data = []  


for table in curated_tables:
    df = spark.sql(f"describe history {table}").limit(1)
    table = table.replace("db_sil_temp_delta.", "")
    

    df = df.withColumn("Client", lit("IRO")) \
           .withColumn("Environment", lit(Environment)) \
           .withColumn("Senorio_Name", lit(senario)) \
           .withColumn("Table_name", lit(table)) \
           .withColumnRenamed("timestamp", "Load_Time") \
           .withColumnRenamed("userName", "Run_By") \
           .select("Client", "Environment", "Senorio_Name", "Table_name", "Load_Time", "Run_By")

    all_data.append(df)


final_df = all_data[0]
for df in all_data[1:]:
    final_df = final_df.unionByName(df)


final_df.write.option("compression", "snappy") \
              .option("overwriteSchema", "true") \
              .option("mergeSchema", "true") \
              .mode("append") \
              .format("parquet") \
              .saveAsTable("CDW_CURATED.SP_LOGS_PROD")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame

# List of parquet tables
parquet_tables = [
    "db_sil_temp_delta.dim_sales_force_curr",
    "db_sil_temp_delta.dim_sales_force_future",
    "db_sil_temp_delta.dim_sales_force_prior",
    "db_sil_temp_delta.dim_unassign_terr_curr",
    "db_sil_temp_delta.dim_unassign_terr_future",
    "db_sil_temp_delta.dim_unassign_terr_prior"
]

senario = "Curated"
Environment = "PROD"
all_data = []  # List to store DataFrames for each table's log

# Loop through each table and collect the data
for table in parquet_tables:
    # Remove "db_sil_temp_delta." from the table name
    table = table.replace("db_sil_temp_delta.", "")
    
    # Create the log data for each table
    df = spark.sql(f"""
    SELECT 
        'IRO' AS Client,
        '{Environment}' AS Environment,
        '{senario}' AS Senorio_Name,
        '{table}' AS Table_name,
        current_timestamp() AS Load_Time,
        'ironshore_terra@trinitypartners.com' AS Run_By
    """)

    # Append the DataFrame to the list
    all_data.append(df)

# Combine all the DataFrames into one
final_df = all_data[0]
for df in all_data[1:]:
    final_df = final_df.unionByName(df)

# Write the final DataFrame to the table in one go
final_df.write.option("compression", "snappy") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .format("parquet") \
    .saveAsTable("CDW_CURATED.SP_LOGS_PROD")


# COMMAND ----------


!pip install snowflake.connector.python
import snowflake.connector
import configparser

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
    sql = f"ALTER EXTERNAL TABLE COMMON_UTIL.COMMON_UTIL_T.{table_name} REFRESH;"
    cursor.execute(sql)
    print("snowflake table refresh successful.")
    conn.close()
 
 
# refresh the table
refresh_snowflake_external_table("SP_LOGS_PROD")

# COMMAND ----------

# %sql
# describe table extended CDW_CURATED.SP_LOGS_PROD

# COMMAND ----------

# dbutils.fs.rm("dbfs:/mnt/iro/cdw_curated/sp_logs_prod",recurse=True)

# COMMAND ----------

# %sql
# drop table CDW_CURATED.SP_LOGS_PROD
