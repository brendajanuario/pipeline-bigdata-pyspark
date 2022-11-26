resource "databricks_job" "orders" {
  name = "daily_orders_ingestion_tf"
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
      task_key = "oders_system_to_s3" 
      job_cluster_key = "terraform-job-cluster"

      notebook_task {
        notebook_path = "ingestion/system_to_s3/orders"
      }
    }

    task {
      task_key = "orders_s3_to_databricks"
      depends_on {
        task_key = "oders_system_to_s3"
      }

      job_cluster_key = "terraform-job-cluster"
      notebook_task {
        notebook_path = "ingestion/s3_to_databricks/orders_raw"
      }
    }

    task {
      task_key = "test_raw_orders"
      depends_on {
        task_key = "orders_s3_to_databricks"
      }

      job_cluster_key = "terraform-job-cluster"
      notebook_task {
        notebook_path = "test/ge_orders_raw"
      }
    }
     task {
      task_key = "orders_transformation"
      depends_on {
        task_key = "test_raw_orders"
      }

      job_cluster_key = "terraform-job-cluster"
      notebook_task {
        notebook_path = "transformations/orders_processed"
      }
    }
     task {
      task_key = "test_processed_orders"
      depends_on {
        task_key = "orders_transformation"
      }

      job_cluster_key = "terraform-job-cluster"
      notebook_task {
        notebook_path = "test/ge_orders"
      }
    }



}
