% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/bacenVertica.R
\name{rertica}
\alias{rertica}
\title{Function that copies a dataframe from R to Vertica, creating SQL structure}
\usage{
rertica(dataframe, tableName,
  schemaName = getOption("bacen.vertica.schemaName"),
  hostName = getOption("bacen.vertica.hostName"),
  port = getOption("bacen.vertica.port"),
  databaseName = getOption("bacen.vertica.databaseName"),
  user = getOption("bacen.vertica.user"),
  password = getOption("bacen.vertica.password"), abort = FALSE)
}
\arguments{
\item{tableName}{tableName to be created in database}

\item{schemaName}{schemaName - settable via options("bacen.vertica.schemaName") - default: public}

\item{hostName}{hostname - settable via options("bacen.vertica.hostName")}

\item{port}{port number - settable via options("bacen.vertica.port")}

\item{databaseName}{name - settable via options("bacen.vertica.databaseName")}

\item{user}{username - settable via options("bacen.vertica.user")}

\item{password}{password - settable via options("bacen.vertica.password")}

\item{abort}{determines if the copy aborts if any row have errors}

\item{df}{dataframe object to be transfered}
}
\description{
Function that copies a dataframe from R to Vertica, creating SQL structure
}
\examples{
\dontrun{
df <- data.frame(coluna1 = c(1,2,3), coluna2 = c("a","b","c"))
rertica(df, "tabela", "schema")
}
}
