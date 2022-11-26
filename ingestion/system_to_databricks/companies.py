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

if not spark._jsparkSession.catalog().tableExists('transactional', 'companies'):
    companies_tb = spark.read.load("s3://toptal-exercise-data-raw/transactional-backup/companies/")
    companies_tb.write.format("delta").mode("append").saveAsTable('transactional.companies')
else:
    companies_tb = spark.read.table('transactional.companies')
    #dbutils.notebook.exit("transactional.companies was created!")

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

for n in range(0, 1):
    company = generate_company()
    companies.append(company)

# COMMAND ----------

print('New company')
new_company = spark.createDataFrame(companies).withColumn("updated_dt", F.current_timestamp())
new_company.display()

# COMMAND ----------

print('Company to update')
old_company = companies_tb.limit(1)
old_company.display()


# COMMAND ----------

print('Company updated')
updated_company = old_company.withColumn("name", F.lit("New Name!!")).withColumn("updated_dt", F.current_timestamp())
updated_company.display()


# COMMAND ----------

final_df = (new_company.union(updated_company)
            .orderBy('updated_dt', ascending=False)
            .dropDuplicates(subset=['cuit'])
           )

final_df.createOrReplaceTempView("company")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO transactional.companies c 
# MAGIC USING company v ON 
# MAGIC           v.cuit = c.cuit 
# MAGIC WHEN MATCHED THEN
# MAGIC       UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC       INSERT *
# MAGIC       

# COMMAND ----------

df = spark.read.table("transactional.companies")
df.write.format("delta").mode("append").save("s3://toptal-exercise-data-raw/transactional-backup/companies/")


# COMMAND ----------


