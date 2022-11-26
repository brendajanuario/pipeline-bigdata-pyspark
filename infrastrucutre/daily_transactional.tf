resource "databricks_job" "daily_transaction" {
  name = "daily_transactional_ingestion_tf"
  max_retries=3
  job_cluster {
     job_cluster_key = "terraform-job-cluster"
    new_cluster {
      num_workers   = 2
      node_type_id = data.databricks_node_type.general_node.id
      spark_version = data.databricks_spark_version.runtime_version.id
       aws_attributes {
        zone_id = "us-east-1d"
        instance_profile_arn = "${var.instance_profile_arn}"
        }
    }
  }

  git_source {
    url = "${var.git_repo}"
    provider = "${var.git_provider}"
    branch = "${var.branch}"
  }

  task {
    task_key = "companies"
    job_cluster_key = "terraform-job-cluster"

    notebook_task {
      notebook_path = "ingestion/system_to_databricks/companies"
    }
  }

  task {
    task_key = "customers"
    job_cluster_key = "terraform-job-cluster"
   
    notebook_task {
      notebook_path = "ingestion/system_to_databricks/customers"
    }
  }

  task {
    task_key = "products"    
    job_cluster_key = "terraform-job-cluster"
    
    notebook_task {
      notebook_path = "ingestion/system_to_databricks/products"
    }
  }

  schedule {
    quartz_cron_expression = "3 0 21 * * ?"
    timezone_id = "UTC"
  }


}


