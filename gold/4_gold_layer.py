# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer: Star Schema Modeling (Dimensional Modeling)
# MAGIC
# MAGIC ## 1. Overview
# MAGIC In this notebook, we transform our clean Silver table (`workspace.spotify_schema.silver_spotify`) into a **Star Schema** optimized for Business Intelligence (Power BI).
# MAGIC - **Architecture:** 1 Fact Table (`fact_spotify`) surrounded by Dimension Tables (`dim_artists`, `dim_tracks`).
# MAGIC - **Objective:** Enable high-performance analytical queries and prevent data redundancy.

# COMMAND ----------

# Read from the Silver Layer
df_silver = spark.read.table("workspace.spotify_schema.silver_spotify")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating Dimension: Artists
# MAGIC Extracts unique artists to serve as a descriptive dimension.

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Clean approach: using the artist name as the natural key for the dimension
dim_artists = df_silver.select("artists").distinct()
dim_artists.write.format("delta").mode("overwrite").saveAsTable("workspace.spotify_schema.dim_artists")

print("dim_artists created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creating Fact Table: Track Performance
# MAGIC Contains the metrics (`popularity`, `duration_ms`) and foreign keys linking back to the dimensions.

# COMMAND ----------

fact_spotify = df_silver.select(
    "track_id", 
    "track_name", 
    "artists", 
    "popularity", 
    "duration_ms", 
    "explicit"
)

fact_spotify.write.format("delta").mode("overwrite").saveAsTable("workspace.spotify_schema.fact_spotify")
print("fact_spotify created successfully!")