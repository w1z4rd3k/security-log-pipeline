# Databricks notebook source
from pyspark.sql import functions as F

df_raw = spark.read.option("header", True).csv("/Volumes/workspace/default/log_data/access.csv")

display(df_raw)

# COMMAND ----------

df_parsed = (
    df_raw
    .withColumn("timestamp_raw", F.regexp_extract("raw_log", r"^\[(.*?)\]", 1))
    .withColumn("level", F.regexp_extract("raw_log", r"^\[.*?\]\s\[(.*?)\]", 1))
    .withColumn("client_ip", F.regexp_extract("raw_log", r"\[client\s([^\]]+)\]", 1))
    .withColumn("message", F.regexp_replace("raw_log", r"^\[.*?\]\s\[(.*?)\]\s?", ""))
    .withColumn("message", F.regexp_replace("message", r"^\[client\s[^\]]+\]\s?", ""))
)

display(df_parsed)

# COMMAND ----------

df_parsed = df_parsed.withColumn(
    "event_time",
    F.to_timestamp("timestamp_raw", "E MMM dd HH:mm:ss yyyy")
)

# COMMAND ----------

df_parsed.select("timestamp_raw").show(10, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

df_parsed = df_parsed.withColumn(
    "event_time",
    F.try_to_timestamp("timestamp_raw", F.lit("E MMM dd HH:mm:ss yyyy"))
)

# COMMAND ----------

df_parsed.select("timestamp_raw", "event_time").show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

df_parsed = (
    df_parsed
    .withColumn(
        "event_type",
        F.when(F.col("message").contains("Directory index forbidden"), F.lit("directory_forbidden"))
         .when(F.col("message").contains("File does not exist"), F.lit("file_missing"))
         .when(F.col("message").contains("Invalid method in request"), F.lit("invalid_method"))
         .when(F.col("message").contains("script not found or unable to stat"), F.lit("script_missing"))
         .otherwise(F.lit("other"))
    )
)

display(df_parsed)

# COMMAND ----------

from pyspark.sql import functions as F

df_clean = df_parsed.filter(F.col("event_time").isNotNull())

# COMMAND ----------

display(
    df_clean.groupBy("event_type").count().orderBy(F.desc("count"))
)

# COMMAND ----------

df_clean = (
    df_parsed
    .filter(F.col("event_time").isNotNull())
    .withColumn(
        "event_type",
        F.when(F.col("message").contains("Directory index forbidden"), F.lit("directory_forbidden"))
         .when(F.col("message").contains("File does not exist"), F.lit("file_missing"))
         .when(F.col("message").contains("Invalid method in request"), F.lit("invalid_method"))
         .when(F.col("message").contains("script not found or unable to stat"), F.lit("script_missing"))
         .otherwise(F.lit("other"))
    )
)

# COMMAND ----------

display(
    df_clean.groupBy("event_type").count().orderBy(F.desc("count"))
)

# COMMAND ----------

from pyspark.sql import functions as F

df_raw = spark.read.option("header", True).csv("/Volumes/workspace/default/log_data/access.csv")
display(df_raw)

# COMMAND ----------

df_parsed = (
    df_raw
    .withColumn("timestamp_raw", F.regexp_extract("raw_log", r"^\[(.*?)\]", 1))
    .withColumn("level", F.regexp_extract("raw_log", r"^\[.*?\]\s\[(.*?)\]", 1))
    .withColumn("client_ip", F.regexp_extract("raw_log", r"\[client\s([^\]]+)\]", 1))
    .withColumn("message", F.regexp_replace("raw_log", r"^\[.*?\]\s\[(.*?)\]\s?", ""))
    .withColumn("message", F.regexp_replace("message", r"^\[client\s[^\]]+\]\s?", ""))
)

display(df_parsed)

# COMMAND ----------

df_parsed = df_parsed.withColumn(
    "event_time",
    F.try_to_timestamp("timestamp_raw", F.lit("E MMM dd HH:mm:ss yyyy"))
)

display(df_parsed)

# COMMAND ----------

df_clean = (
    df_parsed
    .filter(F.col("event_time").isNotNull())
    .withColumn(
        "event_type",
        F.when(F.col("message").contains("Directory index forbidden"), F.lit("directory_forbidden"))
         .when(F.col("message").contains("File does not exist"), F.lit("file_missing"))
         .when(F.col("message").contains("Invalid method in request"), F.lit("invalid_method"))
         .when(F.col("message").contains("script not found or unable to stat"), F.lit("script_missing"))
         .otherwise(F.lit("other"))
    )
)

display(df_clean)

# COMMAND ----------

df_clean.write.mode("overwrite").parquet("/Volumes/workspace/default/log_data/silver_logs")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/log_data"))