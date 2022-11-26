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

def generate_company():
    company = {
        "cuit": fake.unique.random_number(11,11),
        "name": fake.unique.company()
    }
    return company


# COMMAND ----------

companies = []

# COMMAND ----------

for n in range(0, 300):
    company = generate_company()
    companies.append(company)

# COMMAND ----------

companies_tb = spark.createDataFrame(companies).withColumn("updated_dt", F.current_timestamp())
companies_tb.display()

# COMMAND ----------

if not spark._jsparkSession.catalog().tableExists('transactional', 'companies'):
    (companies_tb.write.format('delta')
     .mode('overwrite')
     .option("delta.enableChangeDataFeed", True)
     .saveAsTable("transactional.companies")
    )
    
    companies_tb.write.format("delta").mode("append").save("s3://toptal-exercise-data-raw/transactional-backup/companies/")
    dbutils.notebook.exit("transactional.companies was created!")
    
else:
    companies_tb.createOrReplaceTempView("companies")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO transactions.companies c 
# MAGIC USING companies v ON 
# MAGIC           v.cuit = c.cuit 
# MAGIC WHEN MATCHED THEN
# MAGIC       UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC       INSERT *
# MAGIC       

# COMMAND ----------


