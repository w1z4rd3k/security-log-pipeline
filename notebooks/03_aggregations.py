# Databricks notebook source
from pyspark.sql import functions as F

df_clean = spark.read.parquet("/Volumes/workspace/default/log_data/silver_logs")
display(df_clean)

# COMMAND ----------

agg_event_type = (
    df_clean
    .groupBy("event_type")
    .count()
    .orderBy(F.desc("count"))
)

display(agg_event_type)

# COMMAND ----------

agg_event_type.write.mode("overwrite").parquet("/Volumes/workspace/default/log_data/gold_event_type")

# COMMAND ----------

agg_top_ips = (
    df_clean
    .filter(F.col("client_ip") != "")
    .groupBy("client_ip")
    .count()
    .orderBy(F.desc("count"))
)

display(agg_top_ips)

# COMMAND ----------

agg_top_ips.write.mode("overwrite").parquet("/Volumes/workspace/default/log_data/gold_top_ips")

# COMMAND ----------

agg_level = (
    df_clean
    .groupBy("level")
    .count()
    .orderBy(F.desc("count"))
)

display(agg_level)

# COMMAND ----------

agg_level.write.mode("overwrite").parquet("/Volumes/workspace/default/log_data/gold_level")

# COMMAND ----------

agg_hourly = (
    df_clean
    .withColumn("event_hour", F.date_trunc("hour", F.col("event_time")))
    .groupBy("event_hour")
    .count()
    .orderBy("event_hour")
)

display(agg_hourly)

# COMMAND ----------

agg_hourly.write.mode("overwrite").parquet("/Volumes/workspace/default/log_data/gold_hourly")

# COMMAND ----------

suspicious_ips = (
    df_clean
    .filter(F.col("client_ip") != "")
    .groupBy("client_ip")
    .count()
    .filter(F.col("count") > 20)
    .orderBy(F.desc("count"))
)

display(suspicious_ips)

# COMMAND ----------

suspicious_ips.write.mode("overwrite").parquet("/Volumes/workspace/default/log_data/gold_suspicious_ips")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/log_data"))