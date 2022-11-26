resource "databricks_job" "historical_data_ingestion" {
  name = "historical_data_ingestion"
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
      notebook_path = "data_generation_scripts/companies"
    }
  }

  task {
    task_key = "customers"
    job_cluster_key = "terraform-job-cluster"
   
    notebook_task {
      notebook_path = "data_generation_scripts/customers"
    }
  }

  task {
    task_key = "products"    
    job_cluster_key = "terraform-job-cluster"
    depends_on {
        task_key = "companies"
      }
    notebook_task {
      notebook_path = "data_generation_scripts/products"
    }
  }

    task {
    task_key = "orders"    
    job_cluster_key = "terraform-job-cluster"
    depends_on {
        task_key = "products"
      }
    depends_on {
        task_key = "customers"
      }
    notebook_task {
      notebook_path = "data_generation_scripts/orders"
    }
  }

    task {
    task_key = "weblogs"
    depends_on {
        task_key = "orders"
      }
    job_cluster_key = "terraform-job-cluster"
    
    notebook_task {
      notebook_path = "data_generation_scripts/weblogs"
    }
  }

  schedule {
    quartz_cron_expression = "3 0 21 * * ?"
    timezone_id = "UTC"
  }


}