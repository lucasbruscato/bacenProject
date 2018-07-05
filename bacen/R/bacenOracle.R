#' Function that executes a SQL query that can result in a dataframe or in a modification
#' @param query query to be executed in database
#' @param hostName hostname - settable via options("bacen.oracle.hostName")
#' @param port port number - settable via options("bacen.oracle.port")
#' @param sid name - settable via options("bacen.oracle.sid")
#' @param user username - settable via options("bacen.oracle.user")
#' @param password password - settable via options("bacen.oracle.password")
#'
#' @return dataframe with resultset
#'
#' @examples
#'\dontrun{
#' oracleSendQuery("SELECT * FROM DUAL")
#'}
oracleSendQuery <- function(query,
                            hostName = getOption("bacen.oracle.hostName"),
                            port = getOption("bacen.oracle.port"),
                            sid = getOption("bacen.oracle.sid"),
                            user = getOption("bacen.oracle.user"),
                            password = getOption("bacen.oracle.password")) {

  .iniParametersOracle(hostName = hostName,
                       port = port,
                       sid = sid,
                       user = user,
                       password = password)

  if(.isDropTableAndTableDoesntExists(query = query,
                                     hostName = hostName,
                                     port = port,
                                     sid = sid,
                                     user = user,
                                     password = password)) {
    message("Table doesn't exists")
    return (NULL)
  } else {
    oracleConn <- DBI::dbConnect(bacen_driver, bacen_user, bacen_password, dbname = bacen_connStr)
    result <- ROracle::dbGetQuery(oracleConn, query)
    ROracle::dbDisconnect(oracleConn)
    return (result)
  }
}

#' Function that copies a dataframe from R to Oracle, creating SQL structure
#' @param df dataframe object to be transfered
#' @param tableName tableName to be created in database
#' @param schemaName schemaName - settable via options("bacen.oracle.schemaName") - default: manut_shinny
#' @param hostName hostname - settable via options("bacen.oracle.hostName")
#' @param port port number - settable via options("bacen.oracle.port")
#' @param sid sid - settable via options("bacen.oracle.sid")
#' @param user username - settable via options("bacen.oracle.user")
#' @param password password - settable via options("bacen.oracle.password")
#' @param useSQLldr useSQLldr - indicate if the user prefer to use SQLldr method
#'
#' @examples
#'\dontrun{
#' df <- data.frame(coluna1 = c(1,2,3), coluna2 = c("a","b","c"))
#' roracle(df, "tabela", "schema")
#'}
roracle <- function(dataframe,
                    tableName,
                    schemaName = getOption("bacen.oracle.schemaName"),
                    hostName = getOption("bacen.oracle.hostName"),
                    port = getOption("bacen.oracle.port"),
                    sid = getOption("bacen.oracle.sid"),
                    user = getOption("bacen.oracle.user"),
                    password = getOption("bacen.oracle.password"),
                    useSQLldr = TRUE) {

  if(is.null(dataframe)) {
    stop("Dataframe must not be NULL")
  }

  if(nrow(dataframe) == 0) {
    stop("Dataframe must have rows")
  }

  tableName <- toupper(tableName)
  schemaName <- toupper(schemaName)

  message("Be aware that accentuation is messed up saving on Oracle")
  message("Be aware that timezones applies when saving on Oracle")

  .iniParametersOracle(hostName = hostName, port = port, sid = sid, user = user, password = password)

  if(!useSQLldr || !file.exists(getOption("bacen.oracle.sqlldr"))) {
    message('SQLldr not found. Verify installation and path in getOption("bacen.oracle.sqlldr"), the transfer will proceed with "ROracle::dbWriteTable"')
    oracleConn <- DBI::dbConnect(bacen_driver, bacen_user, bacen_password, dbname = bacen_connStr)
    ROracle::dbWriteTable(conn = oracleConn, name = tableName, value = dataframe)
    ROracle::dbDisconnect(oracleConn)
  } else {
    pathToTempFile <- paste0(tempdir(), "\\bacenTransferTemporaryTable.csv")
    pathToTempFileCtl <- paste0(tempdir(), "\\bacenTransferTemporaryFile.ctl")
    pathToTempFileLog <- paste0(tempdir(), "\\bacenTransferTemporaryFile.log")
    pathToTempFileBad <- paste0(tempdir(), "\\bacenTransferTemporaryFile.bad")

    .exportDataframeToCSV(df = dataframe, path = pathToTempFile, col.names = FALSE)
    .createTableOracle(df = dataframe, schemaName = schemaName, tableName = tableName)
    .transferTempFileToOracle(hostName = hostName,
                              port = port,
                              sid = sid,
                              schemaName = schemaName,
                              tableName = tableName,
                              user = user,
                              password = password,
                              pathToTempFile = pathToTempFile,
                              pathToTempFileCtl = pathToTempFileCtl,
                              pathToTempFileLog = pathToTempFileLog,
                              pathToTempFileBad = pathToTempFileBad,
                              columnsTypes <- sapply(dataframe, class))

    .rmTempFile(pathToTempFile)
    .rmTempFile(pathToTempFileCtl)
    .rmTempFile(pathToTempFileLog)
    .rmTempFile(pathToTempFileBad)
  }

  dfNumLines <- nrow(dataframe)
  tableNumLines <- oracleSendQuery(paste0("SELECT count(1) FROM ", schemaName, ".", tableName),
                                  hostName,
                                  port,
                                  sid,
                                  user,
                                  password)

  if(dfNumLines != tableNumLines) {
    warning(paste0("Not all lines were imported. Dataframe has ", dfNumLines, " rows and table has ", tableNumLines, " rows"))
  }

  .rmParameters()
}
