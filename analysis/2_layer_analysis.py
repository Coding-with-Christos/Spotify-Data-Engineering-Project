# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Exploratory Data Analysis & Data Quality Report
# MAGIC
# MAGIC ## 1. Objective
# MAGIC Before applying any transformation or cleaning logic in the Silver layer, this notebook performs a thorough **Data Profiling** on the raw Bronze table (`workspace.spotify_schema.bronze_spotify`). 
# MAGIC The goal is to:
# MAGIC - Assess data volume and schema integrity.
# MAGIC - Detect technical duplicates vs. legitimate business variations (e.g., track re-releases across multiple albums).
# MAGIC - Establish a robust data-cleaning strategy.

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct

# Load data from the Bronze layer
df = spark.read.table("workspace.spotify_schema.bronze_spotify")

# Calculate metrics
total_rows = df.count()
unique_track_ids = df.select(countDistinct("track_id")).collect()[0][0]
unique_song_artist_combos = df.select(countDistinct("track_name", "artists")).collect()[0][0]

# Displaying a simple summary report
print(f"--- DATA QUALITY REPORT ---")
print(f"Total Rows in Raw Dataset: {total_rows}")
print(f"Unique Track IDs: {unique_track_ids}")
print(f"Unique (Track Name + Artist) Combinations: {unique_song_artist_combos}")
print(f"Potential Duplicates/Re-releases: {total_rows - unique_song_artist_combos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deep Dive into Multi-Occurrence Tracks
# MAGIC The analysis reveals thousands of occurrences where the same song appears multiple times. Let's investigate the top recurring tracks to understand if they are data errors or compilation/remix albums.

# COMMAND ----------

# Grouping by track and artist to find re-releases
df.groupBy("track_name", "artists") \
  .agg(count("*").alias("occurrence_count")) \
  .filter(col("occurrence_count") > 1) \
  .orderBy(col("occurrence_count").desc()) \
  .show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cleaning Strategy Decision
# MAGIC - **Finding:** Songs like holiday classics or hit singles appear dozens of times due to being featured on multiple compilation albums.
# MAGIC - **Cleaning Strategy:** Instead of deleting all duplicates blindly, we will implement a logic in the Silver Layer that partitions by `track_id` and preserves the instance with the **highest popularity**.