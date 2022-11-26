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

class CustomProvider(BaseProvider):
    def product_genre(self):
        products = ["Underwear", "Top", "Shirt", "Kilt", "Scarf", "Shoes", "Tights", "Blouse", "Robe", "Boxers",
                    "Boots", "Gloves", "Belt", "Skirt", "Hat", "Bikini", "Polo Shirt", "Sweatshirt", "Swimming Shorts",
                    "Shorts", "Camisole", "Coat", "Tie", "Nightgown", "Jeans", "Sunglasses", "Knickers", "Poncho",
                    "Sarong", "Cravat", "Sandals", "Waistcoat"]
        size = ['XS', 'S', "M", "L", "XL"]
        product = f"{random.choice(products)} {fake.color_name()} {fake.random_element(size)}"
        return product


fake = Faker()

fake.add_provider(CustomProvider)


# COMMAND ----------

def generate_product(cuit_list):
    product = {
        "product_id": fake.unique.random_number(11,11),
        "name": fake.product_genre(),
        "price_default":  fake.pyfloat(2,2,"positive", 5),
        "cuit": fake.random_element(cuit_list)

    }
    product["price"] = round(product["price_default"]*1.1,2)
    return product

# COMMAND ----------

products = []

# COMMAND ----------

cuit_list = spark.read.table("transactional.companies").select("cuit").rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

for n in range(1, 150000):
    product = generate_product(cuit_list)
    products.append(product)

# COMMAND ----------

products_tb = spark.createDataFrame(products).withColumn("updated_dt", F.current_timestamp())

# COMMAND ----------

if not spark._jsparkSession.catalog().tableExists('transactional', 'products'):
    (products_tb.write.format('delta')
     .mode('overwrite')
     .option("delta.enableChangeDataFeed", True)
     .saveAsTable("transactional.products")
    )
    
    products_tb.write.format("delta").mode("append").save("s3://toptal-exercise-data-raw/transactional-backup/products/")
    dbutils.notebook.exit("transactional.products was created!")
    
else:
    products_tb.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO transactions.products c 
# MAGIC USING products v ON 
# MAGIC           v.product_id = c.product_id 
# MAGIC WHEN MATCHED THEN
# MAGIC       UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC       INSERT *
# MAGIC       
