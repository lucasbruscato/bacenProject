% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/bacenOracle.R
\name{oracleSendQuery}
\alias{oracleSendQuery}
\title{Function that executes a SQL query that can result in a dataframe or in a modification}
\usage{
oracleSendQuery(query, hostName = getOption("bacen.oracle.hostName"),
  port = getOption("bacen.oracle.port"),
  sid = getOption("bacen.oracle.sid"),
  user = getOption("bacen.oracle.user"),
  password = getOption("bacen.oracle.password"))
}
\arguments{
\item{query}{query to be executed in database}

\item{hostName}{hostname - settable via options("bacen.oracle.hostName")}

\item{port}{port number - settable via options("bacen.oracle.port")}

\item{sid}{name - settable via options("bacen.oracle.sid")}

\item{user}{username - settable via options("bacen.oracle.user")}

\item{password}{password - settable via options("bacen.oracle.password")}
}
\value{
dataframe with resultset
}
\description{
Function that executes a SQL query that can result in a dataframe or in a modification
}
\examples{
\dontrun{
oracleSendQuery("SELECT * FROM DUAL")
}
}
