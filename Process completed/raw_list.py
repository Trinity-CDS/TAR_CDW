# Databricks notebook source
# List of tables
tables_list = [
    "account", "cfg_alignment_rule", "cfg_alignment_rule_key", "cfg_alignment_type", 
    "cfg_dimension_dirty_rule", 
    "cfg_sales_roster_columns", "cfg_sales_roster_hierarchy", "dim_date_test", "ist_dim_sales_force_curr", 
    "ist_dim_sales_force_future", "ist_dim_sales_force_prior", "ist_fct_sfdc_call_activity_calls",
    "ist_ims_xpo_fake_prof", "ist_iqvia_email_combined", "ist_rlt_odw_decile", "ist_rlt_opt_decile", 
    "ist_rlt_org_decile", "ist_rlt_prof_decile", "ist_rlt_sys_decile", "mdv_dw_bldgid", 
    "mdv_dw_bluegrot_pro", "mdv_dw_ims_split_weeks", "mdv_dw_ims_xpo", "mdv_dw_intchain", 
    "mdv_dw_locid", "mdv_dw_orgid", "mdv_dw_profid", "mdv_dw_sfdc", "oce__accountaddress__c", "oce__call__c",
    "oce__emailtemplate__c", "oce__emailtransaction__c", "oce__inquiry__c", "oce__inquiry__c_test", 
    "oce__inquiryquestion__c", "rc_decile_definition", "recordtype", "src_age_grp_definition", 
    "src_bi_override", "src_call_credit_rules", "src_company_holiday", "src_decile", 
    "src_decile_definition", "src_employee", "src_org_override_curr", "src_org_override_future", 
    "src_org_override_prior", "src_org_target_curr", "src_org_target_prior", "src_org_targets_curr", 
    "src_org_targets_future", "src_org_targets_prior", "src_payer_model_bridge", "src_prod_brand", 
    "src_prof_override_curr", "src_prof_override_curr_test", "src_prof_override_future", 
    "src_prof_override_prior", "src_prof_targets_curr", "src_prof_targets_future", "src_prof_targets_prior",
    "src_rlt_external_prod", "src_sales_goal_curr", "src_sales_roster_curr", "src_sales_roster_future", 
    "src_sales_roster_prior", "src_spec_specgrp", "src_specialty_exclusion", "src_team_curr", 
    "src_team_future", "src_team_prior", "src_trialcard_pbm", "src_zip_terr_curr", "src_zip_terr_future", 
    "src_zip_terr_prior", "territory2", "territory2model", "user", "userterritory2association"
]

# Loop through the table names and run the REFRESH command in Spark SQL
for table in tables_list:
    # Construct the SQL command to refresh the table
    refresh_command = f"REFRESH TABLE cdw_raw.{table}"
    
    # Run the refresh command in Spark
    spark.sql(refresh_command)

    # Print the command (optional, for verification)
    print(f"Refreshed table: cdw_raw.{table}")


# COMMAND ----------

jdbcHostname = "iro-prd.database.windows.net"
database = "DW_PROD"
user = "terra_iro_user"
password = "edvvS?8#Lm"
jdbcPort = 1433

tables_list = ["MDV_DW_WEB_PORTAL_ACTIONS"]
dbName = "db_sil_temp_delta"



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
    sql_server_df.write \
    .option("compression", "snappy") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .format("delta").saveAsTable(f"db_sil_temp_delta.{table}")

    print(f"Created table: {table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table cdw_curated.MDV_DW_WEB_PORTAL_ACTIONS

# COMMAND ----------

m1=spark.read.table("db_sil_temp_delta.MDV_DW_WEB_PORTAL_ACTIONS")

m1.write \
    .option("compression", "snappy") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .format("parquet").saveAsTable(f"cdw_curated.MDV_DW_WEB_PORTAL_ACTIONS")

# COMMAND ----------

tables_list = [
    "account", "cfg_alignment_rule", "cfg_alignment_rule_key", "cfg_alignment_type", 
    "cfg_dimension_dirty_rule", 
    "cfg_sales_roster_columns", "cfg_sales_roster_hierarchy", "dim_date_test", "ist_dim_sales_force_curr", 
    "ist_dim_sales_force_future", "ist_dim_sales_force_prior", "ist_fct_sfdc_call_activity_calls",
    "ist_ims_xpo_fake_prof", "ist_iqvia_email_combined", "ist_rlt_odw_decile", "ist_rlt_opt_decile", 
    "ist_rlt_org_decile", "ist_rlt_prof_decile", "ist_rlt_sys_decile", "mdv_dw_bldgid", 
    "mdv_dw_bluegrot_pro", "mdv_dw_ims_split_weeks", "mdv_dw_ims_xpo", "mdv_dw_intchain", 
    "mdv_dw_locid", "mdv_dw_orgid", "mdv_dw_profid", "mdv_dw_sfdc", "oce__accountaddress__c", "oce__call__c",
    "oce__emailtemplate__c", "oce__emailtransaction__c", "oce__inquiry__c", "oce__inquiry__c_test", 
    "oce__inquiryquestion__c", "rc_decile_definition", "recordtype", "src_age_grp_definition", 
    "src_bi_override", "src_call_credit_rules", "src_company_holiday", "src_decile", 
    "src_decile_definition", "src_employee", "src_org_override_curr", "src_org_override_future", 
    "src_org_override_prior", "src_org_target_curr", "src_org_target_prior", "src_org_targets_curr", 
    "src_org_targets_future", "src_org_targets_prior", "src_payer_model_bridge", "src_prod_brand", 
    "src_prof_override_curr", "src_prof_override_curr_test", "src_prof_override_future", 
    "src_prof_override_prior", "src_prof_targets_curr", "src_prof_targets_future", "src_prof_targets_prior",
    "src_rlt_external_prod", "src_sales_goal_curr", "src_sales_roster_curr", "src_sales_roster_future", 
    "src_sales_roster_prior", "src_spec_specgrp", "src_specialty_exclusion", "src_team_curr", 
    "src_team_future", "src_team_prior", "src_trialcard_pbm", "src_zip_terr_curr", "src_zip_terr_future", 
    "src_zip_terr_prior", "territory2", "territory2model", "user", "userterritory2association"
]


# COMMAND ----------

for table in tables_list:

    sql_server_df = spark.read \
        .table(f"cdw_raw.{table}")


    sql_server_df.write \
    .option("compression", "snappy") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .format("parquet").saveAsTable(f"cdw_curated.{table}")

    print(f"Created table: {table}")

# COMMAND ----------

!pip install snowflake.connector.python
import snowflake.connector
import configparser

# COMMAND ----------

# "ist_ims_xpo_fake_prof""ist_iqvia_email_combined","oce__accountaddress__c", "oce__call__c",
    # "oce__emailtemplate__c", "oce__emailtransaction__c", "oce__inquiry__c", "oce__inquiry__c_test", 
    # "oce__inquiryquestion__c","src_decile", 

# COMMAND ----------

tables_list = ["MDV_DW_WEB_PORTAL_ACTIONS",
    "ist_dim_sales_force_curr", 
    "ist_dim_sales_force_future", "ist_dim_sales_force_prior", "ist_fct_sfdc_call_activity_calls",  "ist_rlt_odw_decile", "ist_rlt_opt_decile", 
    "ist_rlt_org_decile", "ist_rlt_prof_decile", "ist_rlt_sys_decile", "mdv_dw_bldgid", 
    "mdv_dw_bluegrot_pro", "mdv_dw_ims_split_weeks", "mdv_dw_ims_xpo", "mdv_dw_intchain", 
    "mdv_dw_locid", "mdv_dw_orgid", "mdv_dw_profid", "mdv_dw_sfdc", "src_age_grp_definition", 
    "src_bi_override",  "src_company_holiday", 
    "src_decile_definition", "src_employee", "src_org_override_curr", "src_org_override_future", 
    "src_org_override_prior", "src_org_target_curr", "src_org_target_prior", "src_payer_model_bridge", "src_prod_brand", 
    "src_prof_override_curr",  "src_prof_override_future", 
    "src_prof_override_prior",
    "src_rlt_external_prod", "src_sales_goal_curr", "src_sales_roster_curr", "src_sales_roster_future", 
    "src_sales_roster_prior", "src_spec_specgrp", "src_specialty_exclusion", "src_team_curr", 
    "src_team_future", "src_team_prior", "src_trialcard_pbm", "src_zip_terr_curr", "src_zip_terr_future", 
    "src_zip_terr_prior"
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
    sql = f"ALTER EXTERNAL TABLE IRONSHORE_CDW_PROD.CDW_CURATED.{table_name} REFRESH;"
    cursor.execute(sql)
    print("snowflake table refresh successful.")
    conn.close()
 
 
for table_name in tables_list:
    refresh_snowflake_external_table(table_name)
    print(f"Refreshing table: {table_name}")

# COMMAND ----------

# "ist_ims_xpo_fake_prof","ist_iqvia_email_combined",

# COMMAND ----------

# List of tables
tables_list = [
    "account", "cfg_alignment_rule", "cfg_alignment_rule_key", "cfg_alignment_type", 
    "cfg_dimension_dirty_rule", 
    "cfg_sales_roster_columns", "cfg_sales_roster_hierarchy", "dim_date_test", "ist_dim_sales_force_curr", 
    "ist_dim_sales_force_future", "ist_dim_sales_force_prior", "ist_fct_sfdc_call_activity_calls",
    "ist_ims_xpo_fake_prof", "ist_iqvia_email_combined", "ist_rlt_odw_decile", "ist_rlt_opt_decile", 
    "ist_rlt_org_decile", "ist_rlt_prof_decile", "ist_rlt_sys_decile", "mdv_dw_bldgid", 
    "mdv_dw_bluegrot_pro", "mdv_dw_ims_split_weeks", "mdv_dw_ims_xpo", "mdv_dw_intchain", 
    "mdv_dw_locid", "mdv_dw_orgid", "mdv_dw_profid", "mdv_dw_sfdc", "oce__accountaddress__c", "oce__call__c",
    "oce__emailtemplate__c", "oce__emailtransaction__c", "oce__inquiry__c", "oce__inquiry__c_test", 
    "oce__inquiryquestion__c",  "recordtype", "src_age_grp_definition", 
    "src_bi_override", "src_call_credit_rules", "src_company_holiday", "src_decile", 
    "src_decile_definition", "src_employee", "src_org_override_curr", "src_org_override_future", 
    "src_org_override_prior", "src_org_target_curr", "src_org_target_prior", "src_payer_model_bridge", "src_prod_brand", 
    "src_prof_override_curr", "src_prof_override_curr_test", "src_prof_override_future", 
    "src_prof_override_prior", "src_prof_targets_curr", "src_prof_targets_future", "src_prof_targets_prior",
    "src_rlt_external_prod", "src_sales_goal_curr", "src_sales_roster_curr", "src_sales_roster_future", 
    "src_sales_roster_prior", "src_spec_specgrp", "src_specialty_exclusion", "src_team_curr", 
    "src_team_future", "src_team_prior", "src_trialcard_pbm", "src_zip_terr_curr", "src_zip_terr_future", 
    "src_zip_terr_prior", "territory2", "territory2model", "user", "userterritory2association"
]

# Loop through the table names and run the REFRESH command in Spark SQL
for table in tables_list:
    # Construct the SQL command to refresh the table
    refresh_command = f"REFRESH TABLE cdw_raw.{table}"
    
    # Run the refresh command in Spark
    spark.sql(refresh_command)

    # Print the command (optional, for verification)
    print(f"Refreshed table: cdw_raw.{table}")

