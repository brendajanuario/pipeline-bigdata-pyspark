# Databricks notebook source
import pyspark.sql.functions as F


# COMMAND ----------

df_raw = spark.readStream.table("transactional.orders_raw")

# COMMAND ----------

exploded_df =( df_raw.withColumn("items", F.explode(F.col("items")))
              .withColumn("amount", (F.col("items").getItem("amount").cast('int')))
              .withColumn("cuit", (F.col("items").getItem("cuit").cast('bigint')))
              .withColumn("customer_name", (F.col("items").getItem("name").cast('string')))
              .withColumn("item_price_default", (F.col("items").getItem("price_default").cast('double')))
              .withColumn("item_price", (F.col("items").getItem("price").cast('double')))
              .withColumn("order_total_price", F.col("total_price").cast('double'))
              .withColumn("order_dt", F.col("order_dt").cast('date'))
              .withColumn("product_id", (F.col("items").getItem("product_id").cast('bigint')))
              .withColumn("customer_document_number", F.col("customer_document_number").cast('bigint'))
              .withColumn("record_timestamp", F.current_timestamp())
             )

# COMMAND ----------

columns =  ['customer_birth_day', 'customer_document_number', 'customer_name', 'order_dt', 'order_id', 'status', 'order_total_price', 'record_timestamp', 'amount', 'cuit', 'item_price_default', 'item_price', 'product_id']

# COMMAND ----------

df_final = exploded_df.select(columns)

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using MERGE
def upsertToDelta(microBatchOutputDF, batchId): 
    # Set the dataframe to view name
    microBatchOutputDF.createOrReplaceTempView("orders")

    microBatchOutputDF._jdf.sparkSession().sql("""
    MERGE INTO transactional.orders orders
    USING orders streaming
    ON streaming.order_id = orders.order_id and
    streaming.product_id = orders.product_id
    WHEN MATCHED THEN 
        UPDATE SET *
    WHEN NOT MATCHED THEN 
        INSERT *
        """)

# COMMAND ----------

if spark._jsparkSession.catalog().tableExists('transactional', 'orders'):
    df_final.writeStream.format('delta')\
        .option("checkpointLocation", "/mnt/streaming/orders/check_point") \
        .trigger(once=True)\
        .foreachBatch(upsertToDelta)\
        .outputMode("update")\
        .start()

else:
    df_final.writeStream.format('delta')\
        .option("checkpointLocation", "/mnt/streaming/orders/check_point") \
        .trigger(once=True)\
        .table("transactional.orders")
