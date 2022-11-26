data "databricks_node_type" "general_node" {
  local_disk  = true
  min_cores   = 2
  gb_per_core = 8
  category = "General Purpose"
}

data "databricks_spark_version" "runtime_version" {
  scala = 2.12
  spark_version  = 3.2
  long_term_support = true
}
