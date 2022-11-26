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

root_directory = "/dbfs/great_expectations/"

# COMMAND ----------

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

df = spark.read.table("transactional.orders_raw")

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
    data_asset_name="orders_raw",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "run_id": f"{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)

# COMMAND ----------

context.variables

# COMMAND ----------

expectation_suite_name = "transactional_orders_raw"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)


# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="customer_birth_day")
validator.expect_column_values_to_not_be_null(column="customer_document_number")
validator.expect_column_values_to_not_be_null(column="customer_name")
validator.expect_column_values_to_not_be_null(column="items")
validator.expect_column_values_to_not_be_null(column="order_dt")
validator.expect_column_values_to_not_be_null(column="order_id")
validator.expect_column_values_to_not_be_null(column="status")
validator.expect_column_values_to_not_be_null(column="total_price")
validator.expect_column_values_to_not_be_null(column="record_timestamp")

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

my_checkpoint_name = "oders_raw_checkpoint"
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


