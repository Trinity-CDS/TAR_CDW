# Databricks notebook source
tables_list = [
    "MDSRC_PRO_IMS_ADDR",
    "MDSRC_CALLS_SFA_CORE",
    "MDSRC_CALLS_SFA_HIST",
    "MDVIEW_DW_IMS_XPO",
    "MDSRC_CALLS_SFA_LAST",
    "MDSRC_CALLS_SFA_RAW",
    "MDSRC_ORG_CAIO_TRAN",
    "MDSRC_ORG_DEA_ADDR",
    "MDSRC_ORG_DEA_HIST",
    "MDSRC_PRO_LAAD_TRAN",
    "MDSRC_PRO_CAIP_ADDR",
    "SUMA_TARGET_LIST_CAT",
    "MDSRC_ORG_DEA_LAST",
    "MDVIEW_SFA_IRD_ACCT_LOC",
    "ARC_UPDATE_WEBPORTAL_ACTIONS_20240828",
    "MD_ADDRESS_TRAN2",
    "MDSRC_ORG_DEA_RAW",
    "MDSRC_PRO_CAIP_CONFIG",
    "T1LAAD23719",
    "MD_AUTOMATION",
    "MDSRC_ORG_DEA_STAGE",
    "MDSRC_ORG_DEA_TRAN",
    "MDSRC_PRO_CAIP_CORE",
    "MD_BLDGID",
    "MDSRC_ORG_NPI_ADDR",
    "MD_CONFIG_247_LOADING",
    "MDSRC_ORG_NPI_HIST",
    "MDSRC_ORG_NPI_LAST",
    "MDSRC_ORG_NPI_RAW",
    "SUMA_TARGET_LIST_CAT_NPI",
    "ARC_STATUS_CHANGE_20241126",
    "MD_CONFIG_USERS",
    "MDSRC_ORG_NPI_STAGE",
    "MDSRC_ORG_SFA_ADDR",
    "MDSRC_ORG_SFA_CONFIG",
    "MDSRC_ORG_SFA_HIST",
    "MD_EMAILS",
    "MDSRC_ORG_SFA_LAST",
    "MDSRC_PRO_CAIP_TRAN",
    "MD_LOCID",
    "MDSRC_ORG_SFA_RAW",
    "MD_LOG",
    "MDSRC_ORG_SFA_STAGE",
    "MD_LOG_TRAFFIC",
    "MDSRC_ORG_SFA_STAGE_DATE",
    "MDSRC_ORG_SFA_TRAN",
    "MDSRC_ORG_SYSTEM_ADDR",
    "ARC_PRI_LOC_IC_UPDATE_20240612",
    "MD_ORGID_TEMP",
    "MDSRC_ORG_SYSTEM_HIST",
    "MDVIEW_SFA_IRD_CFG_MAPPING",
    "MD_PROFID",
    "MDSRC_ORG_SYSTEM_LAST",
    "MDVIEW_SFA_IRD_CFG_SETTINGS",
    "MDSRC_ORG_TDDD_STAGE_CSV",
    "MD_PROFID_TEMP",
    "MDSRC_ORG_SYSTEM_RAW",
    "MDRES_ALL",
    "MDSRC_ORG_TEACHHOSP_ADDR",
    "MDRES_ORGIDDW_BY_ADDRESS",
    "MDSRC_ORG_TEACHHOSP_CORE",
    "MDVIEW_DW_ONEKEY_PRO",
    "MDSRC_ORG_TDDD_STAGE",
    "MDSRC_ORG_TEACHHOSP_HIST",
    "MDVIEW_SFA_IRD_AFFILIATION",
    "MDSRC_ORG_TEACHHOSP_LAST",
    "MDSRC_ORG_TDDD_RAW",
    "MD_LOG_PRILOC_CHANGES",
    "MDSRC_ORG_TEACHHOSP_RAW",
    "MDREF_SPECIALTIES_20230919",
    "MDREF_RESERVED_TOKENS",
    "MDSRC_ORG_USER_ADDR",
    "SUMA_VEEVA_SANDBOX_PROF",
    "MDVIEW_SFA_IRD_MERGEREQ",
    "MDSRC_PRO_LAAD_STAGE",
    "MDSRC_ORG_USER_HIST",
    "MDVIEW_SFA_IRD_ORGANIZATION",
    "MDSRC_ORG_USER_LAST",
    "MDVIEW_SFA_IRD_PERSON",
    "MDSRC_ORG_USER_RAW",
    "ARC_PRI_LOC_IC_UPDATE_20240410",
    "MDVIEW_WEB_PORTAL_ACTIONS",
    "MDSRC_ORG_VA_ADDR",
    "ARC_APPROVED_DCRS_20240124",
    "ARC_PRI_LOC_CURR_UPDATE_20240410",
    "MDSRC_ORG_VA_HIST",
    "MDSRC_ORG_VA_LAST",
    "MDSRC_ORG_VA_RAW",
    "MDSRC_ORG_TDDD_LAST",
    "MDSRC_PRO_ONEKEY_STAGE",
    "MDSRC_PRO_AMA_ADDR",
    "MDSRC_PRO_AMA_AO_MEDSCH",
    "MDSRC_ORG_TDDD_HIST",
    "MDSRC_PRO_AMA_CORE",
    "MDSRC_PRO_AMA_HIST",
    "MDSRC_ORG_TDDD_CORE",
    "MDSRC_PRO_AMA_LAST",
    "MDSRC_PRO_AMA_RAW",
    "MDSRC_ORG_TDDD_CONFIG",
    "MDSRC_PRO_AMA_TRAN",
    "MDSRC_PRO_DEA_ADDR",
    "MDSRC_ORG_TDDD_ADDR",
    "MDSRC_PRO_DEA_BUSACT",
    "MDSRC_PRO_DEA_CORE",
    "TMP_LAAD_IDS_TO_DROP",
    "MDSRC_PRO_DEA_DRUGSCH",
    "SUMA_FINAL_IQVIA_LIST",
    "MDSRC_PRO_DEA_HIST",
    "MDSRC_PRO_LAAD_RAW",
    "MDSRC_PRO_DEA_LAST",
    "MDSRC_PRO_DEA_RAW",
    "MDSRC_PRO_LAAD_LAST",
    "MDSRC_PRO_DEA_STAGE",
    "MDSRC_PRO_DEA_TRAN",
    "MD_ORGID",
    "MDSRC_PRO_LAAD_HIST",
    "MDSRC_PRO_NPI_ADDR",
    "MDSRC_PRO_NPI_CORE",
    "ARC_SET_SFA_PRIMARY",
    "MDSRC_PRO_NPI_HIST",
    "MDSRC_ORG_NPI_CORE",
    "MDSRC_PRO_NPI_IDENTIFIER",
    "MDSRC_ORG_DEA_CORE",
    "MDSRC_PRO_NPI_LAST",
    "MDSRC_ORG_SFA_CORE",
    "SUMA_UPDATE_TARGETS",
    "ARC_UPDATE_WEBPORTAL_ACTIONS_20240603",
    "MDSRC_PRO_NPI_MEDICARE",
    "MDSRC_ORG_SYSTEM_CORE",
    "MDSRC_PRO_NPI_RAW",
    "MDSRC_ORG_USER_CORE",
    "MDSRC_PRO_NPI_STAGE",
    "MDSRC_ORG_VA_CORE",
    "MDSRC_PRO_LAAD_CORE",
    "MDSRC_PRO_NPI_TAXONOMY",
    "MDSRC_PRO_NPI_TRAN",
    "MD_CONFIG_EMAIL_NOTIFICATION_20230519",
    "MDSRC_PRO_NPI_TRAN_DISALLOW",
    "MDSRC_PRO_SFA_ADDR",
    "MDSRC_PRO_SFA_CONFIG",
    "MDSRC_PRO_SFA_CORE",
    "ARC_STATUS_UPDATE_20241211",
    "MDVIEW_DW_PROFID",
    "ARC_PRI_LOC_UPDATE_20240911",
    "MDSRC_PRO_SFA_MTCH",
    "MDSRC_PRO_SFA_MTCH_AM",
    "MD_CHECK",
    "ARC_UPDATE_WEBPORTAL_ACTIONS_20240912",
    "MDSRC_PRO_SFA_STAGE_DATE",
    "MD_CONFIG_EMAIL_NOTIFICATION",
    "MDSRC_PRO_SFA_TRAN",
    "MDSRC_PRO_AMA_STAGE",
    "MDSRC_PRO_SLN_ADDR",
    "MD_CONFIG_LUGRID",
    "MDSRC_PRO_ONEKEY_RAW",
    "MDSRC_PRO_SLN_CORE",
    "MD_CONFIG_MATCH_RULES",
    "MDSRC_PRO_SLN_HIST",
    "MD_CONFIG_ORG_LOVS",
    "MDSRC_PRO_ONEKEY_LAST",
    "MDSRC_PRO_SLN_LAST",
    "MD_CONFIG_PRO_LOVS",
    "MDSRC_PRO_SLN_RAW",
    "MD_CONFIG_SOURCES",
    "MDSRC_PRO_ONEKEY_HIST",
    "MDSRC_PRO_SLN_STAGE",
    "MDSRC_PRO_SLN_STAGE_MEDPRO",
    "MDSRC_PRO_SLN_STAGE_MEDPRO_EXT",
    "MDSRC_PRO_SLN_STAGE_REJECTS",
    "MDSRC_PRO_SLN_TRAN",
    "MD_LOG_AUDIT",
    "MDVIEW_DW_ORGID",
    "MDSRC_PRO_CAIP_STAGE",
    "MDSRC_PRO_USER_ADDR",
    "MD_LOG_RESOLVE_ADDRESS",
    "MDSRC_PRO_CAIP_RAW",
    "MDSRC_PRO_USER_CORE",
    "MDSRC_PRO_USER_HIST",
    "MDSRC_PRO_CAIP_LAST",
    "SUMA_TARGET_LIST_UPDATE",
    "MDSRC_PRO_USER_LAST",
    "SUMA_FINAL_VEEVA_LIST",
    "MDSRC_PRO_USER_RAW",
    "MD_PRO_AUTOMATCH",
    "MDVIEW_DW_AMA_PDRP",
    "MD_CONFIG_GLOBAL",
    "MDSRC_PRO_CAIP_HIST",
    "MDVIEW_DW_BLDGID",
    "MD_PROFID_AFFILIATIONS",
    "MDVIEW_DW_DEA_ORG",
    "MDSRC_ORG_CAIO_STAGE",
    "MDVIEW_DW_LOCID",
    "MD_PROFID_USER_ADDRESSES",
    "MDSRC_ORG_CAIO_RAW",
    "MD_QCREPORTS",
    "MD_SRC_CURRDATE",
    "MDSRC_ORG_CAIO_LAST",
    "MDVIEW_DW_ODW",
    "MDREF_COUNTRY_CODES",
    "MDREF_DEGREES",
    "MDSRC_ORG_CAIO_HIST",
    "MDREF_NAMEPARSE_DEGREE_TRAN",
    "TMP_SFA_SET_PRIMARY",
    "MDVIEW_DW_REFRESH_REQUEST",
    "MDREF_NAMEPARSE_LEFTOVERS",
    "MDSRC_ORG_IMS_TRAN",
    "MDVIEW_DW_SFDC",
    "MDREF_NAMEPARSE_STRING_TRAN",
    "MDSRC_PRO_ONEKEY_TRAN",
    "MDVIEW_ODW_WEIGHT",
    "MDREF_NAMEPARSE_SUFFIX_TRAN",
    "MDSRC_ORG_IMS_RAW",
    "MDVIEW_SFA_FILE_STATUS",
    "MDREF_NICKNAMES",
    "MDVIEW_DW_CAIO_ORG",
    "MDREF_ORGNAME_DROPIT",
    "MDSRC_PRO_SFA_HIST",
    "MDSRC_ORG_IMS_LAST",
    "MDREF_ORGNAME_TRAN",
    "MDSRC_PRO_ONEKEY_CORE",
    "MDVIEW_DW_CAIP_PRO",
    "SUMA_IQVIA_TARGET_LIST",
    "MDREF_ORGNAME_TRANFORM_AFTER",
    "MDSRC_PRO_SFA_LAST",
    "MDSRC_ORG_IMS_HIST",
    "MDVIEW_SLN_NEEDSLN",
    "MDREF_ORGNAME_TRANFORM_BEFORE",
    "MDSRC_PRO_ONEKEY_ADDR",
    "MDVIEW_SLN_NEEDSLN_HIST",
    "MDREF_SLN_SITES",
    "ARC_OK_WY",
    "MDSRC_PRO_SFA_RAW",
    "MDSRC_ORG_IMS_CORE",
    "MDREF_SPECIALTIES",
    "MDVIEW_WEB_PORTAL_ACTIONS_AUTO",
    "MDREF_TAXONOMY",
    "MDSRC_PRO_SFA_STAGE",
    "MDSRC_ORG_IMS_ADDR",
    "MDREF_USPS_CANADA",
    "MDREF_USPS_CITY",
    "MDSRC_ORG_IMS_STAGE",
    "MDREF_USPS_COUNTY",
    "MDSRC_PRO_IMS_STAGE_XPO",
    "TEMP_BEST_USER",
    "ARC_PRI_LOC_IC_UPDATE_20240410_2",
    "MDREF_USPS_DETAIL",
    "MDREF_USPS_GEO",
    "MDSRC_PRO_IMS_TRAN",
    "MDREF_USPS_STATES",
    "MDREF_USPS_ZIPSTATE",
    "MDSRC_PRO_IMS_STAGE_PDRP",
    "MDSRC_PRO_IMS_SOPS",
    "MDRES_GENERAL",
    "MDSRC_ORG_TEACHHOSP_STAGE",
    "MDSRC_ORG_CAIO_ADDR",
    "MDSRC_PRO_IMS_REF_UIDSPEC",
    "MDRES_SFA_ORG_DUPS",
    "MDSRC_PRO_IMS_RAW",
    "MDRES_SFA_PRO_DUPS",
    "MDSRC_ORG_CAIO_CONFIG",
    "MDSRC_ADDR_SFA_ADDR",
    "MDSRC_PRO_IMS_LAST",
    "MDVIEW_DW_LAAD_PRO",
    "MDSRC_ADDR_SFA_CORE",
    "ARC_PRI_LOC_IC_UPDATE_20241206",
    "MDSRC_ORG_CAIO_CORE",
    "MDSRC_ADDR_SFA_HIST",
    "MDSRC_PRO_LAAD_ADDR",
    "MDSRC_PRO_IMS_HIST",
    "MDSRC_ADDR_SFA_LAST",
    "MDSRC_ADDR_SFA_RAW",
    "MDSRC_PRO_IMS_CORE",
    "MDSRC_AFFIL_SFA_RAW",
    "MDSRC_CALLS_SFA_ADDR"
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
        .option("dbtable", f"mdmcore.{table}") \
        .load()
 
    # Write data to Delta table
    sql_server_df.write.option("compression","snappy").option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable(f"db_sil_temp_delta.{table}")
 
    print(f"Created table: {table}")
