
# Private function that exports dataframes to csv on a temporary directory
.exportDataframe2CSV <- function(df, path, sep = ";", dec = ".") {
  write.table(df, file = path, row.names = FALSE, sep = sep, dec = dec)
}

# Private function that initialize connection parameters
.iniParameters <- function(host, user, password) {
  library(RJDBC)

  bacen_jdbcClassPath <<- gsub("/", "\\\\", system.file("java", "vertica-jdbc-7.2.1-0.jar", package = "bacen"))
  # bacen_jdbcClassPath <<- "./inst/java/vertica-jdbc-7.2.1-0.jar"
  bacen_jdbcConnStr <<- paste0("jdbc:vertica:", host)
  bacen_jdbcUsr <<- user
  bacen_jdbcPwd <<- password
  bacen_driver <<- JDBC(driverClass="com.vertica.jdbc.Driver", bacen_jdbcClassPath)
}

# Private function that removes connection parameters
.rmParameters <- function() {
  objs <- ls(pos = ".GlobalEnv")
  rm(list = c("bacen_jdbcClassPath", "bacen_jdbcConnStr",
              "bacen_jdbcUsr", "bacen_jdbcPwd", "bacen_driver"),
     pos = ".GlobalEnv")
}

# Private function that drop table
.dropTableIfExists <- function(schemaName, tableName) {
  query <- paste0("DROP TABLE IF EXISTS ", schemaName, ".", tableName, " ; ")
  verticaConn <- dbConnect(bacen_driver, bacen_jdbcConnStr, bacen_jdbcUsr, bacen_jdbcPwd)
  dbSendUpdate(verticaConn, query)
  dbDisconnect(verticaConn)
}

# Private function that creates table with dataframe structure
.createTable <- function(schemaName, df, tableName) {
  # identify columns names and types
  if (is.null(ncol(df))) { # one column dataframe
    columnsTypes <- class(df)
    columnsNames <- "x"
  } else {
    columnsTypes <- sapply(df, class)
    columnsNames <- colnames(df)
  }

  query <- paste0("CREATE TABLE ", schemaName, ".", tableName, " ( ")
  for (i in 1:length(columnsTypes)) {
    if (columnsTypes[i] == "numeric" | columnsTypes[i] == "integer")
      query <- paste0(query, " ", columnsNames[i], if (i == length(columnsTypes)) " NUMERIC(37,12)" else " NUMERIC(37,12),")
    else if (columnsTypes[i] == "character" | columnsTypes[i] == "factor")
      query <- paste0(query, " ", columnsNames[i], if (i == length(columnsTypes)) " varchar(255)" else " varchar(255),")
    else if (columnsTypes[i] == "logical")
      query <- paste0(query, " ", columnsNames[i], if (i == length(columnsTypes)) " boolean" else " boolean,")
  }
  query <- paste0(query, ");")
  verticaConn <- dbConnect(bacen_driver, bacen_jdbcConnStr, bacen_jdbcUsr, bacen_jdbcPwd)
  dbSendUpdate(verticaConn, query)
  dbDisconnect(verticaConn)
}

# Private function that removes temporary file
.rmTempFile <- function(path) {
  if (file.exists(path)) file.remove(path)
}

# Private function that transfers file to vertica server
.transferTempFile2Vertica <- function(schemaName, tableName, path) {
  query <- paste0("COPY ", schemaName, ".", tableName, " FROM LOCAL '", path, "' DELIMITER AS ';' NULL 'NA' ENCLOSED BY '\"' NO ESCAPE SKIP 1 DIRECT;")
  verticaConn <- dbConnect(bacen_driver, bacen_jdbcConnStr, bacen_jdbcUsr, bacen_jdbcPwd)
  dbSendUpdate(verticaConn, query)
  dbDisconnect(verticaConn)
}

#' Function to exhibit all public functions of the package
#' @return list with all public functions of bacen library
#' @export
listBacenFunctions <- function() {
  lsf.str("package:bacen")
}

#' Function that copy a dataframe from R to Vertica, creating SQL structure
#' @param host hostname of database or ip from database
#' @param user username to login in database
#' @param password password to login in database
#' @param schemaName schemaname in database to transfer dataframe in
#' @param df dataframe object to be transfered
#' @param tableName tablename to be created in database
#' @export
rertica <- function(host, user, password, schemaName, df, tableName) {
  path2TempFile <- paste0(tempdir(), "/bacenTransferTemporaryTable.csv")

  .iniParameters(host = host, user = user, password = password)
  .exportDataframe2CSV(df = df, path = path2TempFile)
  .dropTableIfExists(schemaName = schemaName, tableName = tableName)
  .createTable(schemaName = schemaName, df = df, tableName = tableName)
  .transferTempFile2Vertica(schemaName = schemaName, tableName = tableName, path = path2TempFile)
  .rmTempFile(path2TempFile)
  .rmParameters()
}
