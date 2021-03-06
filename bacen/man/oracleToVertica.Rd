% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/bacenVertica.R
\name{oracleToVertica}
\alias{oracleToVertica}
\title{Function that transfer table from Oracle to Vertica}
\usage{
oracleToVertica(oracleTableName, oracleTableSchema, verticaTableName,
  verticaTableSchema, hostNameOracle = getOption("bacen.oracle.hostName"),
  portOracle = getOption("bacen.oracle.port"),
  sidOracle = getOption("bacen.oracle.sid"),
  userOracle = getOption("bacen.oracle.user"),
  passwordOracle = getOption("bacen.oracle.password"),
  hostNameVertica = getOption("bacen.vertica.hostName"),
  portVertica = getOption("bacen.vertica.port"),
  databaseNameVertica = getOption("bacen.vertica.databaseName"),
  userVertica = getOption("bacen.vertica.user"),
  passwordVertica = getOption("bacen.vertica.password"), abort = FALSE)
}
\arguments{
\item{oracleTableName}{name of existing table in oracle}

\item{oracleTableSchema}{schema of existing table in oracle}

\item{verticaTableName}{name of table to be created in vertica}

\item{verticaTableSchema}{schema of table to be created in vertica}

\item{hostNameOracle}{hostname to oracle}

\item{portOracle}{port to oracle}

\item{sidOracle}{sid to oracle}

\item{userOracle}{user to oracle}

\item{passwordOracle}{password to oracle}

\item{hostNameVertica}{hostname to vertica}

\item{portVertica}{port to vertica}

\item{databaseNameVertica}{databasename to vertica}

\item{userVertica}{user to vertica}

\item{passwordVertica}{password to vertica}

\item{abort}{flag to determine if abort if any line present errors on load to vertica}
}
\description{
Function that transfer table from Oracle to Vertica
}
