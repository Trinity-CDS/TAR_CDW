# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, expr, concat_ws, sha2, when,current_timestamp

# COMMAND ----------

# Configuration settings
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# COMMAND ----------

# Load the source table into a DataFrame
src_team_prior_df = spark.read.table("db_sil_temp_delta.SRC_TEAM_PRIOR")

# COMMAND ----------

src_team_prior_df = src_team_prior_df.withColumn(
    "DW_HASH",
    sha2(concat_ws("|",
                   col("TEAM_ID"),
                   when(col("TEAM_NAME").isNull(), "").otherwise(col("TEAM_NAME")),
                   when(col("GROUP_ID").isNull(), "0").otherwise(col("GROUP_ID")),
                   when(col("GROUP_NAME").isNull(), "").otherwise(col("GROUP_NAME")),
                   when(col("SFA_TEAM_ID").isNull(), "").otherwise(col("SFA_TEAM_ID")),
                   when(col("SFA_TEAM_ID_NUM").isNull(), "").otherwise(col("SFA_TEAM_ID_NUM")),
                   when(col("ALIGNMENT_FLAG").isNull(), "").otherwise(col("ALIGNMENT_FLAG")),
                   when(col("ALIGNMENT_TYPE").isNull(), "").otherwise(col("ALIGNMENT_TYPE")),
                   when(col("TEAM_ACRONYM").isNull(), "").otherwise(col("TEAM_ACRONYM")),
                   when(col("LAUNCH_DATE").isNull(), "").otherwise(col("LAUNCH_DATE")),
                   when(col("SYSTEM_ALIGNMENT_HANDLE").isNull(), "").otherwise(col("SYSTEM_ALIGNMENT_HANDLE")),
                   when(col("SFA_ALIGNMENT_FLAG").isNull(), "").otherwise(col("SFA_ALIGNMENT_FLAG"))
                  ), 256)
)

# COMMAND ----------

# Load the target table into a DataFrame
dim_team_prior_df = spark.read.table("db_sil_temp_delta.DIM_TEAM_PRIOR")

# COMMAND ----------

# Ensure data types match between source and target DataFrames
common_columns = [col for col in src_team_prior_df.columns if col in dim_team_prior_df.columns]
for column in common_columns:
    src_team_prior_df = src_team_prior_df.withColumn(column, col(column).cast(dim_team_prior_df.schema[column].dataType))

# COMMAND ----------

# Ensure data types match between source and target DataFrames
src_team_prior_df = src_team_prior_df.withColumn("SFA_TEAM_ID", col("SFA_TEAM_ID").cast("string"))
src_team_prior_df = src_team_prior_df.withColumn("SFA_TEAM_ID_NUM", col("SFA_TEAM_ID_NUM").cast("decimal(10,0)"))
src_team_prior_df = src_team_prior_df.withColumn("TEAM_ACRONYM", col("TEAM_ACRONYM").cast("string"))
src_team_prior_df = src_team_prior_df.withColumn("LAUNCH_DATE", col("LAUNCH_DATE").cast("timestamp"))

# COMMAND ----------

# Update existing records in the target table
updated_dim_team_prior_df = dim_team_prior_df.alias("D").join(
    src_team_prior_df.alias("V"),
    on="TEAM_ID"
).where("D.DW_HASH != V.DW_HASH").select(
    col("D.TEAM_ID"),
    col("V.TEAM_NAME"),
    col("V.GROUP_ID"),
    col("V.GROUP_NAME"),
    col("V.SFA_TEAM_ID"),
    col("V.SFA_TEAM_ID_NUM"),
    col("V.ALIGNMENT_FLAG"),
    col("V.ALIGNMENT_TYPE"),
    col("V.TEAM_ACRONYM"),
    col("V.LAUNCH_DATE"),
    col("V.SYSTEM_ALIGNMENT_HANDLE"),
    col("V.SFA_ALIGNMENT_FLAG"),
    col("V.DW_HASH"),
    lit(True).alias("DIRTY_FLAG"),  # Convert DIRTY_FLAG to boolean
    current_timestamp().alias("CHANGE_DATE")  # Use current timestamp
)

# COMMAND ----------

# Insert new records into the target table
new_records_df = src_team_prior_df.alias("P").join(
    dim_team_prior_df.alias("D"),
    on="TEAM_ID",
    how="left_anti"
).select(
    col("P.TEAM_ID"),
    col("P.TEAM_NAME"),
    col("P.GROUP_ID"),
    col("P.GROUP_NAME"),
    col("P.SFA_TEAM_ID"),
    col("P.SFA_TEAM_ID_NUM"),
    col("P.ALIGNMENT_FLAG"),
    col("P.ALIGNMENT_TYPE"),
    col("P.TEAM_ACRONYM"),
    col("P.LAUNCH_DATE"),
    col("P.SYSTEM_ALIGNMENT_HANDLE"),
    col("P.SFA_ALIGNMENT_FLAG"),
    col("P.DW_HASH"),
    lit(True).alias("DIRTY_FLAG"),  # Convert DIRTY_FLAG to boolean
    current_timestamp().alias("CHANGE_DATE")  # Use current timestamp
)


# COMMAND ----------

spark.sql("Drop table db_sil_temp_delta.N_DIM_TEAM_PRIOR ")

# COMMAND ----------


# Combine updated and new records
final_dim_team_prior_df = updated_dim_team_prior_df.union(new_records_df)

# Write the final DataFrame to the target table
final_dim_team_prior_df.write.mode("overwrite").saveAsTable("db_sil_temp_delta.N_DIM_TEAM_PRIOR")

# Remove records that no longer exist in the source
non_existing_records_df = dim_team_prior_df.alias("Z").join(
    src_team_prior_df.alias("I"),
    on="DW_HASH",
    how="left_anti"
).select("Z.*")

# Write the non-existing records to the target table
non_existing_records_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("db_sil_temp_delta.N_DIM_TEAM_PRIOR")

# Reset DIRTY_FLAG to 0
spark.sql("UPDATE db_sil_temp_delta.N_DIM_TEAM_PRIOR SET DIRTY_FLAG = False WHERE DIRTY_FLAG = True")
