# 🚀 Spotify End-to-End Analytics Lakehouse | Databricks & PySpark

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-007D9C?style=for-the-badge&logo=delta-lake&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)

> An end-to-end cloud data engineering project implementing the **Medallion Architecture** to ingest, clean, explore, and model a raw Spotify dataset using Databricks and PySpark.

## 📌 Business Context & Dataset Origin
Streaming music platforms process massive volumes of track and artist data that often contain technical duplicates, re-releases across multiple compilation albums, and data inconsistencies. 

* **Dataset Source:** Downloaded a raw Spotify dataset containing over 114,000 track and artist records (CSV format).
* **Objective:** Design and implement an automated, scalable end-to-end data pipeline to ingest raw data, execute Exploratory Data Analysis (EDA), apply robust data cleansing strategies, and structure the data into a high-performance **Star Schema**.

## 🏗️ Architecture & Tech Stack
The solution implements a strict **Medallion Lakehouse Architecture**:

* **Data Processing & Lakehouse:** Databricks (Community Edition), PySpark, Delta Lake
* **Orchestration & Automation:** Databricks Jobs (Multi-task DAG workflow)
* **Data Modeling:** Dimensional Modeling (Star Schema - Kimball methodology)

## ⚙️ Pipeline Implementation & Logic

### 🥉 Bronze Layer (Raw Ingestion)
* **Goal:** Ingest raw Spotify dataset into the lakehouse preserving its original state.
* **Process:** Created `workspace.spotify_schema.bronze_spotify` as an immutable Delta table acting as the single source of truth for downstream layers, ensuring complete data lineage.

### 🔍 Exploratory Data Analysis (EDA) & Data Quality
* **Goal:** Uncover data anomalies and shape the cleansing strategy.
* **Process:** 
  * Assessed schema integrity, missing values, and data volume (~114k rows).
  * Investigated multi-occurrence tracks caused by compilation albums, live versions, and remixes.
  * Formulated a data-cleansing strategy targeting track duplicates based on popularity metrics.

### 🥈 Silver Layer (Data Cleansing & Deduplication)
* **Goal:** Transform raw data into a trusted, analytics-ready state.
* **Process:** 
  * Applied PySpark **Window Functions** (`partitionBy("track_id").orderBy(col("popularity").desc())`) to isolate unique tracks.
  * Filtered out lower-ranked duplicate instances, retaining only the highest-popularity version per track.
  * Saved the cleaned output as `workspace.spotify_schema.silver_spotify`.

### 🥇 Gold Layer (Star Schema Dimensional Modeling)
* **Goal:** Serve business-ready metrics using an optimized schema.
* **Process:** Modeled the cleansed Silver data into a dimensional layout:
  * **Dimension Table (`dim_artists`):** Stores unique artist records and descriptive attributes.
  * **Fact Table (`fact_spotify`):** Stores track analytical metrics (popularity, duration, explicit flags, keys).

### 🤖 Orchestration (Databricks Jobs)
* **Automation:** Configured an automated DAG workflow (`SpotifyAppJob`) linking Bronze ingestion, Silver cleansing, and Gold modeling sequentially to ensure seamless execution.

## 📁 Repository Structure

```text
📦 Spotify-Data-Engineering-Lakehouse
 ┣ 📂 bronze/                 # Raw data ingestion notebook
 ┣ 📂 analysis/               # Exploratory Data Analysis & Data Quality Report
 ┣ 📂 silver/                 # Data cleansing & window-based deduplication notebook
 ┣ 📂 gold/                   # Star schema (Fact & Dimensions) modeling notebook
 ┣ 📂 jobs/                   # Databricks workflow configuration
 ┗ 📜 README.md               # Project documentation
