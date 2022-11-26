# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

import datetime

import pandas as pd
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

# COMMAND ----------

#dbutils.fs.rm("dbfs:/great_expectations", True)

# COMMAND ----------

root_directory = "/dbfs/great_expectations/"

# COMMAND ----------

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

df = spark.read.table("transactional.orders")

# COMMAND ----------

my_spark_datasource_config = {
    "name": "transactional",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "data_connector": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                "run_id"
            ],
        }
    },
}

# COMMAND ----------

context.test_yaml_config(yaml.dump(my_spark_datasource_config))

# COMMAND ----------

context.add_datasource(**my_spark_datasource_config)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name="transactional",
    data_connector_name="data_connector",
    data_asset_name="orders",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "run_id": f"{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)

# COMMAND ----------

expectation_suite_name = "transactional_orders_processed"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

#print(validator.head())

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="customer_birth_day")
validator.expect_column_values_to_not_be_null(column="customer_document_number")
validator.expect_column_values_to_not_be_null(column="customer_name")
validator.expect_column_values_to_not_be_null(column="product_id")
validator.expect_column_values_to_not_be_null(column="order_dt")
validator.expect_column_values_to_not_be_null(column="order_id")
validator.expect_column_values_to_not_be_null(column="status")
validator.expect_column_values_to_not_be_null(column="order_total_price")
validator.expect_column_values_to_not_be_null(column="record_timestamp")
validator.expect_column_pair_values_a_to_be_greater_than_b("order_total_price", "item_price_default")
validator.expect_column_pair_values_a_to_be_greater_than_b("item_price", "item_price_default")
#validator.expect_column_max_to_be_between(column="order_total_price", min_value =0,max_value=None)
validator.expect_column_pair_values_a_to_be_greater_than_b("order_id", "product_id", False)

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

my_checkpoint_name = "transactional_checkpoint"
checkpoint_config = {
    "name": my_checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S",
}

# COMMAND ----------

context.add_checkpoint(**checkpoint_config)

# COMMAND ----------

checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)

# COMMAND ----------


