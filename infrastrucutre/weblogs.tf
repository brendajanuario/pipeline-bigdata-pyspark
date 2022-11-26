resource "databricks_job" "daily_weblogs" {
  name = "daily_weblogs"
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
      task_key = "weblogs_system_to_s3" 
      job_cluster_key = "terraform-job-cluster"

      notebook_task {
        notebook_path = "ingestion/system_to_s3/weblogs"
      }
    }

    task {
      task_key = "weblogs_s3_to_databricks"
      depends_on {
        task_key = "weblogs_system_to_s3"
      }

      job_cluster_key = "terraform-job-cluster"
      notebook_task {
        notebook_path = "ingestion/s3_to_databricks/weblogs_raw"
      }
    }

     task {
      task_key = "weblogs_transformation"
      depends_on {
        task_key = "weblogs_s3_to_databricks"
      }

      job_cluster_key = "terraform-job-cluster"
      notebook_task {
        notebook_path = "transformations/weblogs_processed"
      }
    }
     
}
