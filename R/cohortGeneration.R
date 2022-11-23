# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



#' Add a cohort table to a cdm object
#'
#' @description
#' This function creates an empty cohort table in a cdm and returns the cdm with the cohort table added.
#'
#' @param cdm A cdm reference created by CDMConnector. write_schema must be specified.
#' @param name Name of the cohort table to be created.
#' @param overwrite Should the cohort table be overwritten if it already exists? (TRUE or FALSE)
#'
#' @export
addCohortTable <- function(cdm,
                           name = "cohort",
                           overwrite = FALSE) {

  if(is.null(attr(cdm, "write_schema"))) stop("write_schema must be set in the cdm object")

  con <- attr(cdm, "dbcon")
  schema <- attr(cdm, "write_schema")

  checkmate::assertCharacter(schema, min.len = 1, max.len = 2, min.chars = 1, null.ok = FALSE)
  checkmate::assertCharacter(name, len = 1, min.chars = 1, null.ok = FALSE)

  tables <- CDMConnector::listTables(con, schema = schema)

  if ((name %in% tables) && !overwrite) {
    rlang::abort(paste0("Table \"", name, "\" already exists. Set overwrite = TRUE to recreate it."))
  }

  sql <- "
      IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL
      	DROP TABLE @cohort_database_schema.@cohort_table;

      CREATE TABLE @cohort_database_schema.@cohort_table (
      	cohort_definition_id BIGINT,
      	subject_id BIGINT,
      	cohort_start_date DATE,
      	cohort_end_date DATE
      );
  "

  sql <- SqlRender::render(sql = sql,
                           cohort_database_schema = attr(cdm, "write_schema"),
                           cohort_table = name,
                           warnOnMissingParameters = TRUE
  )

  sql <- SqlRender::translate(sql = sql, targetDialect = CDMConnector::dbms(attr(cdm, "dbcon")))

  sqlStatements <- SqlRender::splitSql(sql)

  DBI::dbWithTransaction(con, {
    purrr::walk(sqlStatements, ~DBI::dbExecute(attr(cdm, "dbcon"), .x))
  })

  if (length(schema) == 2) {
    cdm[[name]] <- dplyr::tbl(con, dbplyr::in_catalog(schema[[1]], schema[[2]], name))
  } else if (length(schema) == 1) {
    cdm[[name]] <- dplyr::tbl(con, dbplyr::in_schema(schema, name))
  } else {
    cdm[[name]] <- dplyr::tbl(con, name)
  }
  # cdm[[name]] <- dplyr::tbl(con, dbplyr::sql(paste(c(writeSchema, name), collapse = ".")))
  invisible(cdm)
}



#' Generate a set of cohorts
#'
#' @description
#' This function generates a set of cohorts in the cohort table.
#' @importFrom generics generate
#' @param cdm cdm reference object
#'
#' @param CohortDefinitionSet A cohort defefintion set dataframe
#'
#' @returns cdm reference object with the added cohort table containing generated cohorts
#'
#' @export
generate.CohortSet <- function(cdm,
                              cohortDefinitionSet,
                              cohortTableName) {


  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 0, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet), must.include = c("cohortId", "cohortName", "sql"))
  checkmate::assert_character(attr(cdm, "write_schema"))



  con <- attr(cdm, "dbcon")
  writeSchema <- attr(cdm, "write_schema")

  if (!(cohortTableName %in% CDMConnector::listTables(con, writeSchema))) {
    cdm <- addCohortTable(cdm, name = cohortTableName)
  }

  if (nrow(cohortDefinitionSet) == 0) return(cdm)

  cli::cli_progress_bar(total = 5, format = "Generating cohorts {cli::pb_bar} {pb_current}/{pb_total}")
  for (i in 1:nrow(cohortDefinitionSet)) {

    sql <- cohortDefinitionSet$sql[i] %>%
      SqlRender::render(
        cdm_database_schema = attr(cdm, "cdm_schema"),
        vocabulary_database_schema = attr(cdm, "cdm_schema"),
        target_database_schema = attr(cdm, "write_schema"),
        results_database_schema = attr(cdm, "write_schema"),
        target_cohort_table = cohortTableName,
        target_cohort_id = cohortDefinitionSet$cohortId[i],
        warnOnMissingParameters = FALSE
      ) %>%
      SqlRender::translate(
        targetDialect = CDMConnector::dbms(con),
        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
      ) %>%
      SqlRender::splitSql()

    DBI::dbWithTransaction(con, purrr::walk(sql, ~DBI::dbExecute(con, .x)))
    cli::cli_progress_update()
  }
  cli::cli_progress_done()

  if (!(cohortTableName %in% names(cdm))) {
    if (length(schema) == 2) {
      cdm[[cohortTableName]] <- dplyr::tbl(x$src$con, dbplyr::in_catalog(schema[[1]], schema[[2]], name))
    } else if (length(schema) == 1) {
      cdm[[cohortTableName]] <- dplyr::tbl(x$src$con, dbplyr::in_schema(schema, name))
    } else {
      cdm[[cohortTableName]] <- dplyr::tbl(x$src$con, name)
    }
  }

  return(cdm)
}
