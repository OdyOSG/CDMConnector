% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/generate.R
\name{addCohortTable}
\alias{addCohortTable}
\title{Add a cohort table to a cdm object}
\usage{
addCohortTable(cdm, name = "cohort", overwrite = FALSE)
}
\arguments{
\item{cdm}{A cdm reference created by CDMConnector. write_schema must be specified.}

\item{name}{Name of the cohort table to be created.}

\item{overwrite}{Should the cohort table be overwritten if it already exists? (TRUE or FALSE)}
}
\description{
This function creates an empty cohort table in a cdm and returns the cdm with the cohort table added.
}
