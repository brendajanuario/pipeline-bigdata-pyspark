# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

from faker import Faker
import random
import datetime 
import pandas as pd
import boto3
import json
from datetime import date, timedelta
import logging

# COMMAND ----------

products_tb = spark.read.table("transactional.products").sample(0.3)

# COMMAND ----------

fake = Faker()

start_dt = datetime.date(2021, 11, 15)
end_dt = datetime.date(2022, 11, 14)

product_id_list = products_tb.select('product_id').rdd.map(lambda x: x[0]).collect()
path = ['product', 'login']
log_str = ''
code = ['200', '500', '402']


# COMMAND ----------

def insert_to_s3_bucket(object, record_dt):
    try:
        s3 = boto3.client('s3')
        bucket = "toptal-exercise-data-raw"
        key = f"analytics/combined_log/year={record_dt[:4]}/record_dt={record_dt}.txt"

        s3.put_object(
            Body=object,
            Bucket=bucket,
            Key=key
        )
        print('Successful ingestion on S3 bucket. \n\n')

    except Exception as e:
        logging.error(e)
        logging.warning(f'Error inserting on S3 bucket.')
        raise


# COMMAND ----------

start_date = date(2022, 2, 1)
end_date = date(2022, 3, 1)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)



for single_date in daterange(start_date, end_date):
    record_dt=(single_date.strftime("%Y-%m-%d"))
   
    for el in range(0,30):
        product_id = str(random.choices(product_id_list)[0])
        log_dt = fake.date_time_between_dates(start_dt, end_dt).strftime('[%d/%m/%Y %H:%M:%S]')

        log_str = log_str + f'{fake.ipv4()} - {fake.user_name()} {log_dt} "GET /{random.choices(path)[0]}/{product_id}" {random.choices(code, [9,1,1])[0]} {fake.random_number(5, 1)} - "{fake.user_agent()}"\n'

    insert_to_s3_bucket(log_str, record_dt)
    time.sleep(5)

# COMMAND ----------


