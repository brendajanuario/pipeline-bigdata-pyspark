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

if not spark._jsparkSession.catalog().tableExists('transactional', 'products'):
    products_tb = spark.read.load("s3://toptal-exercise-data-raw/transactional-backup/products/")
    products_tb.write.format("delta").mode("append").saveAsTable('transactional.products')
else:
    products_tb = spark.read.table('transactional.products')


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

for n in range(0, 1):
    product = generate_product(cuit_list)
    products.append(product)

# COMMAND ----------

print('New products')
new_products = spark.createDataFrame(products).withColumn("updated_dt", F.current_timestamp())
new_products.display()

# COMMAND ----------

print('products to update')
old_products = products_tb.limit(1)
old_products.display()


# COMMAND ----------

print('products updated')
updated_products = old_products.withColumn("name", F.lit("New Product !!")).withColumn("updated_dt", F.current_timestamp())
updated_products.display()


# COMMAND ----------

final_df = (new_products.union(updated_products)
            .orderBy('updated_dt', ascending=False)
            .dropDuplicates(subset=['product_id'] )
           )

final_df.createOrReplaceTempView("products")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO transactional.products c 
# MAGIC USING products v ON 
# MAGIC           v.product_id = c.product_id 
# MAGIC WHEN MATCHED THEN
# MAGIC       UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC       INSERT *
# MAGIC       

# COMMAND ----------

df = spark.read.table("transactional.products")
df.write.format("delta").mode("append").save("s3://toptal-exercise-data-raw/transactional-backup/products/")


# COMMAND ----------


