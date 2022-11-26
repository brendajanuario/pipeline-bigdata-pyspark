# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

from faker import Faker
from faker.providers import BaseProvider
import random
import datetime 
import pandas as pd
import boto3
import json
import pyspark.sql.functions as F

# COMMAND ----------

fake = Faker()

# COMMAND ----------


def generate_customer():
    customer = {
        "document_number": fake.unique.random_number(11,11),
        "name": fake.name(),
        "birth_day": fake.date_of_birth(None,18,90),
        "country": fake.country()
    }
    return customer

# COMMAND ----------

customers = []

# COMMAND ----------

for n in range(1, 1000000):
    customer = generate_customer()
    customers.append(customer)

# COMMAND ----------

customers_tb = spark.createDataFrame(customers).withColumn("updated_dt", F.current_timestamp())
customers_tb.display()

# COMMAND ----------

if not spark._jsparkSession.catalog().tableExists('transactional', 'customers'):
    (customers_tb.write.format('delta')
     .mode('overwrite')
     .option("delta.enableChangeDataFeed", True)
     .saveAsTable("transactional.customers")
    )
    
    customers_tb.write.format("delta").mode("append").save("s3://toptal-exercise-data-raw/transactional-backup/customer/")
    dbutils.notebook.exit("transactional.customers was created!")
    
else:
    customers_tb.createOrReplaceTempView("customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO transactions.customers c 
# MAGIC USING customer v ON 
# MAGIC           v.document_number = c.document_number and 
# MAGIC           v.name = c.name and
# MAGIC           v.birth_day = c.birth_day
# MAGIC WHEN MATCHED THEN
# MAGIC       UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC       INSERT *
# MAGIC       
