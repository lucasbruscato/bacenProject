#' Function that executes a SQL query that result in a dataframe (select registers from a table)
#' @param query query to be executed
#' @param hostName hostname - settable via options("bacen.vertica.hostName")
#' @param port port number - settable via options("bacen.vertica.port")
#' @param databaseName name - settable via options("bacen.vertica.databaseName")
#' @param user username - settable via options("bacen.vertica.user")
#' @param password password - settable via options("bacen.vertica.password")
#'
#' @return dataframe with resultset
#'
#' @examples
#'\dontrun{
#' verticaGetQuery("SELECT 1")
#'}
verticaGetQuery <- function(query,
                            hostName = getOption("bacen.vertica.hostName"),
                            port = getOption("bacen.vertica.port"),
                            databaseName = getOption("bacen.vertica.databaseName"),
                            user = getOption("bacen.vertica.user"),
                            password = getOption("bacen.vertica.password")) {

  .iniParametersVertica(hostName = hostName,
                        port = port,
                        databaseName = databaseName,
                        user = user,
                        password = password)

  verticaConn <- RJDBC::dbConnect(bacen_driver, bacen_connStr, bacen_user, bacen_password)
  result <- RJDBC::dbGetQuery(verticaConn, query)
  RJDBC::dbDisconnect(verticaConn)

  return (result)
}

#' Function that executes a SQL query that result in a modification (update, alter, drop, etc)
#' @param query query to be executed
#' @param hostName hostname - settable via options("bacen.vertica.hostName")
#' @param port port number - settable via options("bacen.vertica.port")
#' @param databaseName name - settable via options("bacen.vertica.databaseName")
#' @param user username - settable via options("bacen.vertica.user")
#' @param password password - settable via options("bacen.vertica.password")
#'
#' @examples
#'\dontrun{
#' verticaGetQuery("UPDATE tabela SET campo = 'valor'")
#'}
verticaSendUpdate <- function(query,
                              hostName = getOption("bacen.vertica.hostName"),
                              port = getOption("bacen.vertica.port"),
                              databaseName = getOption("bacen.vertica.databaseName"),
                              user = getOption("bacen.vertica.user"),
                              password = getOption("bacen.vertica.password")) {

  .iniParametersVertica(hostName = hostName,
                        port = port,
                        databaseName = databaseName,
                        user = user,
                        password = password)

  verticaConn <- RJDBC::dbConnect(bacen_driver, bacen_connStr, bacen_user, bacen_password)
  RJDBC::dbSendUpdate(verticaConn, query)
  RJDBC::dbDisconnect(verticaConn)
}

#' Function that copies a dataframe from R to Vertica, creating SQL structure
#' @param df dataframe object to be transfered
#' @param tableName tableName to be created in database
#' @param schemaName schemaName - settable via options("bacen.vertica.schemaName") - default: public
#' @param hostName hostname - settable via options("bacen.vertica.hostName")
#' @param port port number - settable via options("bacen.vertica.port")
#' @param databaseName name - settable via options("bacen.vertica.databaseName")
#' @param user username - settable via options("bacen.vertica.user")
#' @param password password - settable via options("bacen.vertica.password")
#' @param abort determines if the copy aborts if any row have errors
#'
#' @examples
#'\dontrun{
#' df <- data.frame(coluna1 = c(1,2,3), coluna2 = c("a","b","c"))
#' rertica(df, "tabela", "schema")
#'}
rertica <- function(dataframe,
                    tableName,
                    schemaName = getOption("bacen.vertica.schemaName"),
                    hostName = getOption("bacen.vertica.hostName"),
                    port = getOption("bacen.vertica.port"),
                    databaseName = getOption("bacen.vertica.databaseName"),
                    user = getOption("bacen.vertica.user"),
                    password = getOption("bacen.vertica.password"),
                    abort = FALSE) {

  if(is.null(dataframe)) {
    stop("Dataframe must not be NULL")
  }

  if(nrow(dataframe) == 0) {
    stop("Dataframe must have rows")
  }

  if(is.null(schemaName)) {
    schemaName <- "public"
  }

  pathToTempFile <- paste0(tempdir(), "/bacenTransferTemporaryTable.csv")

  .iniParametersVertica(hostName = hostName,
                        port = port,
                        databaseName = databaseName,
                        user = user,
                        password = password)

  .exportDataframeToCSV(df = dataframe, path = pathToTempFile)
  .dropTableIfExists(schemaName = schemaName, tableName = tableName)
  .createTableVertica(df = dataframe, schemaName = schemaName, tableName = tableName)
  .transferTempFileToVertica(schemaName = schemaName, tableName = tableName, path = pathToTempFile, abort = abort)

  dfNumLines <- nrow(dataframe)
  tableNumLines <- verticaGetQuery(paste0("SELECT count(1) FROM ", schemaName, ".", tableName))

  if(dfNumLines != tableNumLines) {
    warning(paste0("Not all lines were imported. Dataframe has ", dfNumLines, " rows and table has ", tableNumLines, " rows"))
  }

  .rmTempFile(pathToTempFile)
  .rmParameters()
}

#' Function that transfer table from Oracle to Vertica
#'
#' @param oracleTableName name of existing table in oracle
#' @param oracleTableSchema schema of existing table in oracle
#' @param verticaTableName name of table to be created in vertica
#' @param verticaTableSchema schema of table to be created in vertica
#' @param hostNameOracle hostname to oracle
#' @param portOracle port to oracle
#' @param sidOracle sid to oracle
#' @param userOracle user to oracle
#' @param passwordOracle password to oracle
#' @param hostNameVertica hostname to vertica
#' @param portVertica port to vertica
#' @param databaseNameVertica databasename to vertica
#' @param userVertica user to vertica
#' @param passwordVertica password to vertica
#' @param abort flag to determine if abort if any line present errors on load to vertica
#'
#' @examples
#'\dontrun{
#' oracleToVertica("PRODUCT_CATEGORY", "SAFEPAY_ADM, "PRODUCT_CATEGORY", "FSIOLA")
#'}
oracleToVertica <- function(oracleTableName,
                            oracleTableSchema,
                            verticaTableName,
                            verticaTableSchema,
                            hostNameOracle = getOption("bacen.oracle.hostName"),
                            portOracle = getOption("bacen.oracle.port"),
                            sidOracle = getOption("bacen.oracle.sid"),
                            userOracle = getOption("bacen.oracle.user"),
                            passwordOracle = getOption("bacen.oracle.password"),
                            hostNameVertica = getOption("bacen.vertica.hostName"),
                            portVertica = getOption("bacen.vertica.port"),
                            databaseNameVertica = getOption("bacen.vertica.databaseName"),
                            userVertica = getOption("bacen.vertica.user"),
                            passwordVertica = getOption("bacen.vertica.password"),
                            abort = FALSE) {

  df <- oracleSendQuery(paste0("SELECT * FROM ",oracleTableSchema,".",oracleTableName),
                        hostName = hostNameOracle,
                        port = portOracle,
                        sid = sidOracle,
                        user = userOracle,
                        password = passwordOracle)

  rertica(df,
          verticaTableName,
          verticaTableSchema,
          hostName = hostNameVertica,
          port = portVertica,
          databaseName = databaseNameVertica,
          user = userVertica,
          password = passwordVertica,
          abort)
}
