# Databricks notebook source
import json
import requests
from datetime import datetime
import pytz

# Webhook URL
teams_webhook_url = "https://prod-137.westus.logic.azure.com:443/workflows/6b7837d3a9094ad6a01d77b59870e7b4/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=vj0L94VlDu6LQ2bXGcSvdb4TCY9eq3NIRPJV8W9N8tw"

# Current time in EST
est = pytz.timezone('US/Eastern')
current_time_est = datetime.now(est)
formatted_time = current_time_est.strftime('%I:%M %p %d %B')

# Upcoming tasks
upcoming_tasks = ["Read Raw - Daily", "Dimension - Daily", "Facts - Daily", "Summary - Daily", "Reporting -Weekly (Sunday)/On demand", "Vendor File Exports - Daily"]


table_rows = [{"Upcoming tasks": task} for task in upcoming_tasks]


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
                        "text": "Process Initiated:",
                        "weight": "Bolder",
                        "size": "Medium"
                    },
                    {
                        "type": "TextBlock",
                        "text": f"Start time (EST): {formatted_time}",
                        "wrap": True
                    },
                    {
                        "type": "Container",
                        "separator": True,
                        "items": [
                            {
                                "type": "ColumnSet",
                                "columns": [
                                    {"type": "Column", "width": "stretch", "items": [{"type": "TextBlock", "text": "Upcoming tasks", "weight": "Bolder", "size": "Small"}]},
                                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": " ", "separator": True, "size": "Small"}]},
                                ]
                            }
                        ]
                    },
                    {
                        "type": "Container",
                        "items": [
                            {
                                "type": "ColumnSet",
                                "columns": [
                                    {"type": "Column", "width": "stretch", "items": [{"type": "TextBlock", "text": f"{index + 1}. {row['Upcoming tasks']}", "size": "Small"}]},
                                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": " ", "separator": True, "size": "Small"}]},
                                ]
                            } for index, row in enumerate(table_rows)
                        ]
                    }
                ]
            }
        }
    ]
}

# Send notification
response = requests.post(
    teams_webhook_url,
    headers={"Content-Type": "application/json"},
    data=json.dumps(adaptive_card_payload)
)


# COMMAND ----------

from pyspark.sql import SparkSession

df = spark.createDataFrame([(spark.sql("SELECT current_timestamp()").first()[0],)], ["current_time"])

df = df.withColumnRenamed("current_time", "start_time")

df.write.mode("overwrite").saveAsTable("db_sil_temp_delta.time_log")

spark.sql("SELECT * FROM db_sil_temp_delta.time_log").show()

