# Databricks notebook source
# MAGIC %pip install geoip2
# MAGIC %pip install user-agents

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark import SparkFiles
from pyspark.sql.types import StringType, TimestampType
from datetime import date
import json
import urllib.request
import requests
from user_agents import parse
import time
import datetime
#from geolite2 import geolite2
from geoip2 import database
import pandas as pd


# COMMAND ----------

spark.sparkContext.addFile("dbfs:/mapping/GeoLite2-Country.mmdb")
spark.sparkContext.addFile("dbfs:/mapping/GeoLite2-City.mmdb")

# COMMAND ----------

bronze_df = spark.readStream.table("analytical.logs_raw")

# COMMAND ----------

@udf(returnType=StringType())
def get_country(ip_address):
    geo = database.Reader(SparkFiles.get("GeoLite2-Country.mmdb"))
    try:        
        country = geo.country(ip_address).country.names['en']
    except:
        country=''
    return country

# COMMAND ----------

def normalize_timestamp(str):
    return datetime.datetime.strptime(str,"[%d/%m/%Y %H:%M:%S]")

def normalize_user_agent(ua):
    try:
        parse_ua = parse(ua).device
        user_agent = f"{parse_ua.family} {parse_ua.brand} {parse_ua.model}"
    except:
        user_agent = ua
    return user_agent

udf_country = udf(lambda z: get_country(z), StringType())   
udf_user_agent = udf(lambda z: normalize_user_agent(z), StringType())   
udf_timestamp = udf(lambda z: normalize_timestamp(z), TimestampType())   

# COMMAND ----------

columns = ["ip_address", "request", "country", "status_code", "user_agent", "request_timestamp", "year", "product_id", "action"]

# COMMAND ----------


final_df = (bronze_df.withColumn("request_timestamp", udf_timestamp(F.col("request_timestamp")))
            .withColumn("user_agent", udf_user_agent(F.col("user_agent")))
            .withColumn("created_dt", F.current_timestamp())
            .withColumn("product_id", F.when(F.col("request").contains("product"), F.split(F.col("request"), '/').getItem(2)))
            .withColumn("action", F.split(F.col("request"), '/').getItem(1))
            .withColumn("country", get_country(F.col("ip_address")))
            .select(*columns)
           )

# COMMAND ----------

final_df.writeStream.format('delta')\
    .option("maxFilesPerTrigger", "1")\
    .option("checkpointLocation", "/mnt/streaming/logs/check_point") \
    .partitionBy("country")\
    .trigger(availableNow=True)\
    .table("analytical.logs")

# COMMAND ----------


