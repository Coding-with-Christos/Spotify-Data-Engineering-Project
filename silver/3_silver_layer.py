# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer: Data Cleansing & Transformation
# MAGIC
# MAGIC ## 1. Overview
# MAGIC In this notebook, we transform the raw data from the Bronze layer into a clean, structured table. 
# MAGIC - **Input:** `workspace.spotify_schema.bronze_spotify`
# MAGIC - **Output:** `workspace.spotify_schema.silver_spotify` (Delta Table)
# MAGIC - **Objective:** Apply our deduplication strategy to resolve technical duplicates while retaining the highest-quality version of each track.

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# Read Bronze table
df_bronze = spark.read.table("workspace.spotify_schema.bronze_spotify")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deduplication Strategy & Window Functions
# MAGIC Based on our exploratory data analysis, duplicate records can appear due to re-releases across multiple albums. 
# MAGIC To handle this cleanly:
# MAGIC - We partition the dataset by `track_id`.
# MAGIC - We order by `popularity` in descending order.
# MAGIC - We assign a row number (`rn`) and keep only the top record (`rn = 1`) for each unique track.

# COMMAND ----------

# Strategy: Remove pure technical duplicates by track_id
# If a track appears multiple times with the same ID, keep the one with the highest popularity
windowSpec = Window.partitionBy("track_id").orderBy(col("popularity").desc())

df_silver = df_bronze.withColumn("rn", row_number().over(windowSpec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Persistence & Validation
# MAGIC We save the cleaned dataset as a Delta table in our Silver layer and validate the final row count to ensure data integrity.

# COMMAND ----------

# Save as Silver Delta Table
df_silver.write.format("delta").mode("overwrite").saveAsTable("workspace.spotify_schema.silver_spotify")

# COMMAND ----------

print(f"Silver table created successfully! Cleaned row count: {df_silver.count()}")