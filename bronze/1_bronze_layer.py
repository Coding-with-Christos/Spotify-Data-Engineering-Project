# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer: Ingestion & Raw Data Storage
# MAGIC
# MAGIC ## 1. Overview
# MAGIC This notebook is responsible for the **Ingestion Layer** of our Spotify Data Pipeline. 
# MAGIC - **Source:** Raw CSV dataset (`spotify_dataset.csv`) stored in our isolated Unity Catalog Volume (`workspace.spotify_schema.spotify_volume`).
# MAGIC - **Destination:** Delta Lake Raw Table (`workspace.spotify_schema.bronze_spotify`).
# MAGIC - **Goal:** Ingest raw data as-is, preserving the original schema without any modifications or filtering, leveraging ACID transactions via Delta format.

# COMMAND ----------

# Define the secure path to our raw dataset inside the Unity Catalog Volume
file_path = "/Volumes/workspace/spotify_schema/spotify_volume/spotify_dataset.csv"

# COMMAND ----------

# Read the raw CSV file into a Spark DataFrame with inferred schema and header detection
df_bronze = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# COMMAND ----------

# Persist the raw DataFrame into a Delta Lake table (Bronze Layer)
df_bronze.write.format("delta").mode("overwrite").saveAsTable("workspace.spotify_schema.bronze_spotify")
print("Bronze table successfully created and stored in Delta format!")

# COMMAND ----------

# Validate ingestion by previewing the raw data structure
df_validated = spark.read.table("workspace.spotify_schema.bronze_spotify")
display(df_validated.limit(10))