// Databricks notebook source
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.types._

// COMMAND ----------

/* Define Classes */
case class State(life_cycle_state: String, state_message: String)

case class JobRun(job_id: Long, run_id: Long, number_in_job: Long, 
                       state: State, start_time: Long, 
                       setup_duration: String, creator_user_name: String, run_name: String, 
                       run_page_url: String, run_type: String);

case class HeaderTagElement(id: String, width: Long, column: String)

// COMMAND ----------

/* Build Html Components */
/* Table Header */
def buildTableHeader(headers: List[HeaderTagElement]) : String = {
  val trTag = "<tr role=\"row\">%s</tr>"
  val thTag = "<th class=\"th-sm sorting_asc\" tabindex=\"0\" aria-controls=\"dtBasicExample\" rowspan=\"1\" colspan=\"1\" aria-sort=\"ascending\" aria-label=\"%s : activate to sort column descending\" style=\"width: %spx;\">%s</th>"
  val headerTagsArr = headers.zipWithIndex.map{case (hte, index) => thTag.format(hte.id, hte.width, hte.column)}
  val headerTags = headerTagsArr.mkString("")
  return trTag.format(headerTags)
}

/* Table Data */
def buildTableData(value: String) : String = {
  val tdTag = "<td>%s</td>"
  return tdTag.format(value)
}

/* Table Row */
def buildTableRow(rowValuesArr: List[String]) : String = {
  val trTag = "<tr role=\"row\" class=\"even\">%s</tr>"
  val tdTags = rowValuesArr.map(value => buildTableData(value))
  val rowValues = tdTags.mkString("")
  return trTag.format(rowValues)
}

/* Table Body */
def buildTable(tableHeaderTag: String, tableBodyTags: List[String]) : String = {
  val tableTag = "<table id=\"dtBasicExample\" class=\"table table-striped table-bordered table-sm dataTable\" cellspacing=\"0\" role=\"grid\" aria-describedby=\"dtBasicExample_info\" width=\"100%%\" style=\"width: 100%%;\"> <thead>%s</thead> <tbody>%s</tbody> </table>"
  val tableBodyElements = tableBodyTags.mkString("")
  return tableTag.format(tableHeaderTag, tableBodyElements)
}

/* Anchor */
def buildAnchor(name: String, href: String) : String = {
  val anchorTag = "<a href=\"%s\" target=\"_blank\" style>%s</a>"
  return anchorTag.format(href, name)
}

/* Build Table Row Tag for HTML Display */
def buildTableRowTag(jobId: Long, runId: Long, runName: String, runPageUrl: String, startTime: String, createdBy: String, state: State) : String = {
  val jobRunAsList = List(jobId.toString, buildAnchor(runId.toString, runPageUrl), runName, startTime, createdBy, state.life_cycle_state, state.state_message)
  val tableRowTag = buildTableRow(jobRunAsList)
  return tableRowTag
}

// COMMAND ----------

/* Header Columns */
def getHeaderColumns() : List[HeaderTagElement] = {
  val headers = List(
    HeaderTagElement("jobId", 85, "Job ID"),
    HeaderTagElement("runPage", 85, "Run Page"),
    HeaderTagElement("runName", 255, "Run Name"),
    HeaderTagElement("startTime", 140, "Start Time"),
    HeaderTagElement("createdBy", 200, "Created By"),
    HeaderTagElement("lifeCycleState", 200, "Life Cycle State"),
    HeaderTagElement("stateMessage", 200, "State Message")
  )
  return headers;
}

def getRunsList(): String = {
  val apiUrl = dbutils.notebook.getContext().apiUrl.get
  val accessToken = dbutils.notebook.getContext().apiToken.get

  /* Search API using generated Access Token */
  val apiEndpoint = apiUrl + "/api/2.0/jobs/runs/list?active_only=true&limit=150"

  val createConn = new java.net.URL(apiEndpoint).openConnection.asInstanceOf[java.net.HttpURLConnection]

  createConn.setRequestProperty("Authorization", "Bearer " + accessToken)
  createConn.setDoOutput(true)
  createConn.setRequestProperty("Accept", "application/json")
  createConn.setRequestMethod("GET")

  val responseStr = scala.io.Source.fromInputStream(createConn.getInputStream).mkString
  return responseStr
}

// COMMAND ----------

/* Define UDFs */
val get_table_row_tag = udf(buildTableRowTag _)

// COMMAND ----------

/* Parse JSON */
val runs : List[JobRun] =
try {
  val responseStr = getRunsList()
  implicit val formats = org.json4s.DefaultFormats  
  (parse(responseStr) \ "runs").extract[List[JobRun]]
} catch {
  case e:Exception => {
    println(e)
    null
  }
}

// COMMAND ----------

val dateFormat = "dd-MM-yyyy HH:mm:ss"
val jobRunsDf = runs
.toSeq
.toDF()
.withColumn("run_start_time", from_unixtime($"start_time" / 1000, dateFormat))
.withColumn("table_row_tag", get_table_row_tag($"job_id", $"run_id", $"run_name", $"run_page_url", $"run_start_time", $"creator_user_name", $"state"))
.drop($"start_time")

// COMMAND ----------

display(jobRunsDf)

// COMMAND ----------

display(jobRunsDf.filter(col("run_type") === "SUBMIT_RUN"))

// COMMAND ----------

/* Table Header and Body */
val tableHeaderTag = buildTableHeader(getHeaderColumns())
val jobRunsTableBodyTags = jobRunsDf.filter(col("run_type") === "JOB_RUN").select("table_row_tag").as[String].collect().toList
val runSubmitsTableBodyTags = jobRunsDf.filter(col("run_type") === "SUBMIT_RUN").select("table_row_tag").as[String].collect().toList

// COMMAND ----------

/* Table HTML */
val jobRunsTable = buildTable(tableHeaderTag, jobRunsTableBodyTags)
val runSubmitsTable = buildTable(tableHeaderTag, runSubmitsTableBodyTags)

// COMMAND ----------

val htmlPlainTag = "<!DOCTYPE html> <html lang=\"en\"> <head> <title>Jobs Page</title> <meta charset=\"utf-8\"> <link rel=\"stylesheet\" href=\"https://use.fontawesome.com/releases/v5.8.2/css/all.css\"> <link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css?family=Roboto:300,400,500,700&display=swap\"> <link href=\"https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/4.4.1/css/bootstrap.min.css\" rel=\"stylesheet\"> <link href=\"https://cdnjs.cloudflare.com/ajax/libs/mdbootstrap/4.18.0/css/mdb.min.css\" rel=\"stylesheet\"> <link rel=\"stylesheet\" type=\"text/css\" href=\"https://cdn.datatables.net/1.10.21/css/jquery.dataTables.css\"> <script type=\"text/javascript\" src=\"https://cdnjs.cloudflare.com/ajax/libs/jquery/3.4.1/jquery.min.js\"></script> <script type=\"text/javascript\" src=\"https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.4/umd/popper.min.js\"></script> <script type=\"text/javascript\" src=\"https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/4.4.1/js/bootstrap.min.js\"></script> <script type=\"text/javascript\" src=\"https://cdnjs.cloudflare.com/ajax/libs/mdbootstrap/4.18.0/js/mdb.min.js\"></script> <script type=\"text/javascript\" charset=\"utf8\" src=\"https://cdn.datatables.net/1.10.21/js/jquery.dataTables.js\"></script> <style>table.dataTable thead .sorting:after, table.dataTable thead .sorting:before, table.dataTable thead .sorting_asc:after, table.dataTable thead .sorting_asc:before, table.dataTable thead .sorting_asc_disabled:after, table.dataTable thead .sorting_asc_disabled:before, table.dataTable thead .sorting_desc:after, table.dataTable thead .sorting_desc:before, table.dataTable thead .sorting_desc_disabled:after, table.dataTable thead .sorting_desc_disabled:before{bottom: .5em;} table a{margin: 0; color: #007bff !important;}</style> <script>$(document).ready(function (){$('#dtBasicExample').DataTable(); $('.dataTables_length').addClass('bs-select');}); </script> </head> <body> <div class=\"container\" style=\"margin-left: initial !important;\"> <h2>%s</h2> <div class=\"row\"> <div class=\"col-sm-12\">%s</div></div></div></div></body> </html>"
val htmlFormattedJobRuns = htmlPlainTag.format("Job Runs", jobRunsTable)
val htmlFormattedRunSubmits = htmlPlainTag.format("Run Submits", runSubmitsTable)

// COMMAND ----------

displayHTML(s"""
  $htmlFormattedJobRuns
""")

// COMMAND ----------

displayHTML(s"""
  $htmlFormattedRunSubmits
""")

// COMMAND ----------


