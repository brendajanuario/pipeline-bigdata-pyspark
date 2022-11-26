# Databricks notebook source
import pyspark.sql.functions as F


# COMMAND ----------

raw_url = "s3://toptal-exercise-data-raw/transactional/orders/"

ddl = "customer_birth_day date, customer_document_number string, customer_name string, items array<map<string,string>>, order_dt string, order_id string, status string, total_price string, _rescued_data string"

# COMMAND ----------

df_raw = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("badRecordsPath", "s3://toptal-exercise-data-raw/malformed_records/orders/")
      .schema(ddl)
      .option("cloudFiles.schemaLocation", "/mnt/streaming/orders_raw/schema")      
      .load(raw_url)
     )

# COMMAND ----------

df_final = df_raw.withColumn("record_timestamp", F.current_date())

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using MERGE
def upsertToDelta(microBatchOutputDF, batchId): 
    # Set the dataframe to view name
    microBatchOutputDF.createOrReplaceTempView("orders")

    microBatchOutputDF._jdf.sparkSession().sql("""
    MERGE INTO transactional.orders_raw orders
    USING orders streaming
    ON streaming.order_id = orders.order_id
    WHEN MATCHED THEN 
        UPDATE SET *
    WHEN NOT MATCHED THEN 
        INSERT *
        """)

# COMMAND ----------

if spark._jsparkSession.catalog().tableExists('transactional', 'orders_raw'):
    df_final.writeStream.format('delta')\
        .option("checkpointLocation", "/mnt/streaming/orders_raw/check_point") \
        .trigger(once=True)\
        .foreachBatch(upsertToDelta)\
        .outputMode("update")\
        .start()

else:
    print('a')
    df_final.writeStream.format('delta')\
        .option("checkpointLocation", "/mnt/streaming/orders_raw/check_point") \
        .option("delta.enableChangeDataFeed", True) \
        .trigger(once=True)\
        .table("transactional.orders_raw")

# COMMAND ----------


