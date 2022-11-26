terraform {
  required_providers {

    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "aws" {
   profile = "brenda"
}

