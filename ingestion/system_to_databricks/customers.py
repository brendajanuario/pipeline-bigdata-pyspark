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

if not spark._jsparkSession.catalog().tableExists('transactional', 'customers'):
    customers_tb = spark.read.load("s3://toptal-exercise-data-raw/transactional-backup/customers/")
    customers_tb.write.format("delta").mode("append").saveAsTable('transactional.customers')
else:
    customers_tb = spark.read.table('transactional.customers')

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

for n in range(0, 1):
    customer = generate_customer()
    customers.append(customer)

# COMMAND ----------

print('New customers')
new_customer = spark.createDataFrame(customers).withColumn("updated_dt", F.current_timestamp())
new_customer.display()

# COMMAND ----------

print('customers to update')
old_customers = customers_tb.limit(1)
old_customers.display()


# COMMAND ----------

print('customers updated')
updated_customers = old_customers.withColumn("country", F.lit("New Country !!")).withColumn("updated_dt", F.current_timestamp())
updated_customers.display()


# COMMAND ----------

final_df = (new_customer.union(updated_customers)
            .orderBy('updated_dt', ascending=False)
            .dropDuplicates(subset=['document_number', 'birth_day', 'name'] )
           )

final_df.createOrReplaceTempView("customer")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO transactional.customers c 
# MAGIC USING customer v ON 
# MAGIC           v.document_number = c.document_number and 
# MAGIC           v.name = c.name and
# MAGIC           v.birth_day = c.birth_day
# MAGIC WHEN MATCHED THEN
# MAGIC       UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC       INSERT *
# MAGIC       

# COMMAND ----------

df = spark.read.table("transactional.customers")
df.write.format("delta").mode("append").save("s3://toptal-exercise-data-raw/transactional-backup/customers/")

