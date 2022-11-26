# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

raw_url="s3://toptal-exercise-data-raw/analytics/combined_log/"

# COMMAND ----------

ddl="ip_address string, client_identity string, username string, request_data string, request_time string, request string, status_code string, bytes string, referer string, user_agent string, year int"

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("sep", " ")
      .schema(ddl)
      .option("cloudFiles.schemaLocation", "/mnt/streaming/logs_raw/schema")
      .option("badRecordsPath", "s3://toptal-exercise-data-raw/malformed_records/logs/")
      .load(raw_url)
     )

# COMMAND ----------

final_df = (df.withColumn("request_timestamp", 
                          F.concat_ws(" ", F.col("request_data"), F.col("request_time"))
                         )
            .withColumn("created_dt", F.current_timestamp())
            .withColumn("year", F.year(F.current_date()))
           )

# COMMAND ----------

final_df.writeStream\
  .format("delta")\
  .outputMode("append")\
  .partitionBy("year")\
  .option("checkpointLocation", "/mnt/streaming/logs_raw/check_point")\
  .trigger(availableNow=True)\
  .toTable("analytical.logs_raw")

# COMMAND ----------


