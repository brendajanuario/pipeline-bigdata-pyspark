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
import logging
from datetime import date, timedelta

# COMMAND ----------

columns = ["cuit", "name", "price", "price_default", "product_id"]
products_pd = spark.read.table("transactional.products").select(*columns).sample(0.3).toPandas()

# COMMAND ----------

customers_pd = spark.read.table("transactional.customers").sample(0.3).toPandas()

# COMMAND ----------

customer = customers_pd.sample(1).to_dict('records')[0]

# COMMAND ----------

fake = Faker()

# COMMAND ----------

def generate_order_item(products_pd):
    number_of_products_list = [1,2,3,4]
    amount = [1,2,3,4,5,6,7,8]
    items = []
    
    number_of_products = random.choices(number_of_products_list,[9,7,1,1])[0]
    products = products_pd.sample(n=number_of_products).to_dict('records')  
    
    for product in products:
        product['amount'] = random.choices(amount,[1,3,2,1,1,1,1,1])[0]
        items.append(product)

    return items


# COMMAND ----------

def generate_order(items, customers_pd, timestamp_dt):
    start_dt = datetime.date(2021, 1, 1)
    end_dt = datetime.date(2022, 11, 14)
    status = ["pending", "cancelled", "finished"]
    total_price = 0
    customer = customers_pd.sample(1).to_dict('records')[0]
    
    for item in items:
        total_price = total_price + item['price'] * item['amount']
        
    order = {
        "order_id": fake.unique.uuid4(),
        "status": random.choices(status,[1,1,4])[0],
        "order_dt": timestamp_dt,
        "items": items,
        "total_price": total_price,
        "customer_name": customer["name"],
        "customer_birth_day": customer["birth_day"].strftime("%Y-%m-%d"),
        "customer_document_number": customer["document_number"]
    }
    return order

# COMMAND ----------

def insert_to_s3_bucket(object, record_dt, file_name):
    try:
        s3 = boto3.client('s3')
        bucket = "toptal-exercise-data-raw"
        key = f"transactional/orders/year={record_dt[:4]}/record_dt={record_dt}/{file_name}.json"

        s3.put_object(
            Body=json.dumps(object),
            Bucket=bucket,
            Key=key
        )
        print('Successful ingestion on S3 bucket. \n\n')

    except Exception as e:
        logging.error(e)
        logging.warning(f'Error inserting on S3 bucket.')
        raise


# COMMAND ----------

orders = []
record_dt = date.today().strftime("%Y-%m-%d")
timestamp_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")

for n in range(1, 3):
    items = generate_order_item(products_pd)
    order = generate_order(items, customers_pd, timestamp_dt)
    orders.append(order)

file_name = str(fake.uuid4())
insert_to_s3_bucket(orders, record_dt, file_name)

# COMMAND ----------


