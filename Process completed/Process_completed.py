# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, expr
import json
import requests




table_name = "db_sil_temp_delta.time_log"


df = spark.read.table(table_name)


df_with_runtime = df.withColumn("current_time", current_timestamp())


df_with_runtime = df_with_runtime.withColumn("total_run_time_seconds",
                                             expr("unix_timestamp(current_time) - unix_timestamp(start_time)"))


total_run_time_seconds = df_with_runtime.agg({"total_run_time_seconds": "sum"}).collect()[0][0]


hours, remainder = divmod(total_run_time_seconds, 3600)
minutes, seconds = divmod(remainder, 60)


if hours > 0:
    formatted_run_time = f"{int(hours)}:{int(minutes):02d} hours"
else:
    formatted_run_time = f"{int(minutes)}:{int(seconds):02d} minutes"

# Webhook 
teams_webhook_url = "https://prod-137.westus.logic.azure.com:443/workflows/6b7837d3a9094ad6a01d77b59870e7b4/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=vj0L94VlDu6LQ2bXGcSvdb4TCY9eq3NIRPJV8W9N8tw"



# Adaptive Card
adaptive_card_payload = {
    "type": "message",
    "attachments": [
        {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": "Process Completed:",
                        "weight": "Bolder",
                        "size": "Medium"
                    },
                    {
                        "type": "TextBlock",
                        "text": f"Total Run Time: {formatted_run_time}",
                        "wrap": True
                    }
                ]
            }
        }
    ]
}


response = requests.post(
    teams_webhook_url,
    headers={"Content-Type": "application/json"},
    data=json.dumps(adaptive_card_payload)
)

if response.status_code == 200:
    print("Notification successfully sent to Microsoft Teams.")
else:
    print(f"Failed to send notification. Status code: {response.status_code}, Response: {response.text}")


# COMMAND ----------

# %run "/Workspace/Shared/TAR_CDW_PROD/Process completed/Arch_tables_dq"

# COMMAND ----------

# %run "/Workspace/Shared/IRO_CDW_PROD/Process completed/Snowflake-Logs"

# COMMAND ----------

# %run "/Workspace/Shared/TAR_CDW_PROD/Process completed/raw_list"
