resource "databricks_cluster" "shared_cluster" {
  cluster_name            = "Terraform Cluster"
  node_type_id           = data.databricks_node_type.general_node.id
  spark_version            = data.databricks_spark_version.runtime_version.id
  autotermination_minutes = 10
  autoscale {
    min_workers = 1
    max_workers = 3
  }
  spark_env_vars = {
    PYSPARK_PYTHON ="/databricks/python3/bin/python3"
  }
  custom_tags = {
    "project" = "toptal"
  }
  
  aws_attributes {
   zone_id = "us-east-1d"
   instance_profile_arn = "${var.instance_profile_arn}"
 }

}