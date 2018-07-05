# global variables
## oracle
options("bacen.oracle.sqlldr" = "C:/oracleClient/instantclient_12_2/sqlldr.exe")
options("bacen.oracle.hostName" = "rpp.bd.intranet")
options("bacen.oracle.port" = 1521)
options("bacen.oracle.sid" = "rpp.pagseguro")
options("bacen.oracle.user" = "manut_shinny")
options("bacen.vertica.password" = "shinnymanut")
options("bacen.oracle.schemaName" = "manut_shinny")

## vertica
options("bacen.vertica.hostName" = "a1-zueira1")
options("bacen.vertica.port" = 5433)
options("bacen.vertica.databaseName" = "okane")
options("bacen.vertica.user" = Sys.getenv("USERNAME"))
options("bacen.vertica.password" = "a")

# Private function that exports dataframes to csv on a temporary directory
.exportDataframeToCSV <- function(df, path, col.names = TRUE, sep = ";", dec = ".", quote = FALSE) {
  utils::write.table(df,
              file = path,
              col.names = col.names,
              row.names = FALSE,
              sep = sep,
              dec = dec,
              quote = quote,
              fileEncoding = 'UTF-8')
}

# Private function that initializes connection parameters for Vertica
.iniParametersVertica <- function(hostName, port, databaseName, user, password) {

  if(is.null(hostName) | is.null(port) | is.null(databaseName) | is.null(user) | is.null(password)) {
    stop('Set the parameters to Vertica
      options("bacen.vertica.hostName" = "hostname"),
      options("bacen.vertica.port" = porta),
      options("bacen.vertica.databaseName" = "databaseName"),
      options("bacen.vertica.user" = "usuario"),
      options("bacen.vertica.password" = "senha")')
  }

  requireNamespace("RJDBC")

  bacen_classPath <<- gsub("/", "\\\\", system.file("java", "vertica-jdbc-7.2.1-0.jar", package = "bacen"))
  bacen_connStr <<- paste0("jdbc:vertica://", hostName, ":", port, "/", databaseName)
  bacen_user <<- user
  bacen_password <<- password
  bacen_driver <<- RJDBC::JDBC(driverClass="com.vertica.jdbc.Driver", bacen_classPath)
}

# Private function that initializes connection parameters for Oracle
.iniParametersOracle <- function(hostName, port, sid, user, password) {

  if(is.null(hostName) | is.null(port) | is.null(sid) | is.null(user) | is.null(password)) {
    stop('Set the parameters to Oracle
      options("bacen.oracle.hostName" = "hostname"),
      options("bacen.oracle.port" = porta),
      options("bacen.oracle.sid" = "sid"),
      options("bacen.oracle.user" = "usuario"),
      options("bacen.oracle.password" = "senha")')
  }

  requireNamespace("ROracle")
  requireNamespace("infuser")

  bacen_connStr <<- paste0(
    "(DESCRIPTION=",
    "(ADDRESS=(PROTOCOL=tcp)(HOST=", hostName, ")(PORT=", port, "))",
    "(CONNECT_DATA=(SERVICE_NAME=", sid, ")))")
  bacen_user <<- user
  bacen_password <<- password
  bacen_driver <<- DBI::dbDriver("Oracle")
}

# Private function that removes connection parameters
.rmParameters <- function() {
  suppressWarnings({
    rm(list = c("bacen_classPath",
                "bacen_connStr",
                "bacen_user",
                "bacen_password",
                "bacen_driver"),
       pos = ".GlobalEnv"
    )
  })
}

# Private function that drops table
.dropTableIfExists <- function(schemaName, tableName) {
  query <- paste0("DROP TABLE IF EXISTS ", schemaName, ".", tableName, " ; ")
  verticaConn <- DBI::dbConnect(bacen_driver, bacen_connStr, bacen_user, bacen_password)
  RJDBC::dbSendUpdate(verticaConn, query)
  DBI::dbDisconnect(verticaConn)
}

# Private function that creates table with dataframe structure for Vertica
.createTableVertica <- function(df, schemaName, tableName) {
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
    else if (columnsTypes[i] == "Date")
      query <- paste0(query, " ", columnsNames[i], if (i == length(columnsTypes)) " date" else " date,")
    else if (columnsTypes[i][[1]][1] == "POSIXct")
      query <- paste0(query, " ", columnsNames[i], if (i == length(columnsTypes)) " timestamp" else " timestamp,")
  }

  query <- paste0(query, ");")
  verticaConn <- DBI::dbConnect(bacen_driver, bacen_connStr, bacen_user, bacen_password)
  RJDBC::dbSendUpdate(verticaConn, query)
  DBI::dbDisconnect(verticaConn)
}

# Private function that creates table with dataframe structure for Oracle
.createTableOracle <- function(df, schemaName, tableName) {
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
      query <- paste0(query, " ", columnsNames[i], if (i == length(columnsTypes)) " VARCHAR(5)" else " VARCHAR(5),")
    else if (columnsTypes[i] == "Date")
      query <- paste0(query, " ", columnsNames[i], if (i == length(columnsTypes)) " date" else " date,")
    else if (columnsTypes[i][[1]][1] == "POSIXct")
      query <- paste0(query, " ", columnsNames[i], if (i == length(columnsTypes)) " date" else " date,")
  }

  query <- paste0(query, ")")
  oracleConn <- DBI::dbConnect(bacen_driver, bacen_user, bacen_password, dbname = bacen_connStr)

  tabela <- ROracle::dbGetQuery(oracleConn, paste0("SELECT table_name FROM all_tables WHERE table_name = '",tableName,"' AND owner = '",toupper(schemaName),"'"))
  if(nrow(tabela) > 0) {
    ROracle::dbSendQuery(oracleConn, paste0("DROP TABLE ", schemaName, ".", tableName))
  }

  ROracle::dbSendQuery(oracleConn, query)
  ROracle::dbDisconnect(oracleConn)
}

# Private function that removes temporary file
.rmTempFile <- function(path) {
  if (file.exists(path)) {
    file.remove(path)
  }
}

# Private function that creates column names string with separator
.getColNamesString <- function(colTypes) {

  fullString <- NULL
  for (i in 1:length(colTypes)) {
    if (is.null(fullString)) {
      fullString <- names(colTypes)[i]

      if(colTypes[[i]][1] == 'Date') {
        fullString <- paste0(fullString, " ", "\"to_date(trim(:",names(colTypes)[i],"),'YYYY-MM-DD')\"")
      }

      if(colTypes[[i]][1] == 'POSIXct') {
        fullString <- paste0(fullString, " ", "\"to_date(trim(:",names(colTypes)[i],"),'YYYY-MM-DD HH24:MI:SS')\"")
      }
    } else {
      fullString <- paste0(fullString, " , ", names(colTypes)[i])

      if(colTypes[[i]][1] == 'Date') {
        fullString <- paste0(fullString, " ", "\"to_date(trim(:",names(colTypes)[i],"),'YYYY-MM-DD')\"")
      }

      if(colTypes[[i]][1] == 'POSIXct') {
        fullString <- paste0(fullString, " ", "\"to_date(trim(:",names(colTypes)[i],"),'YYYY-MM-DD HH24:MI:SS')\"")
      }
    }
  }

  return(fullString)
}

# Private function that transfers file to vertica server
.transferTempFileToVertica <- function(schemaName, tableName, path, abort) {
  query <- paste0("COPY ", schemaName, ".", tableName, " FROM LOCAL '", path, "' DELIMITER AS ';' NULL 'NA' ENCLOSED BY '\"' NO ESCAPE SKIP 1 DIRECT")

  if(abort) {
    query <- paste0(query, " ENFORCELENGTH ABORT ON ERROR")
  }

  verticaConn <- DBI::dbConnect(bacen_driver, bacen_connStr, bacen_user, bacen_password)
  RJDBC::dbSendUpdate(verticaConn, query)
  DBI::dbDisconnect(verticaConn)
}

# Private function that transfers file to oracle server
.transferTempFileToOracle <- function(hostName,
                                      port,
                                      sid,
                                      schemaName,
                                      tableName,
                                      user,
                                      password,
                                      pathToTempFile,
                                      pathToTempFileCtl,
                                      pathToTempFileLog,
                                      pathToTempFileBad,
                                      listColTypes,
                                      pathToSqlldr = getOption("bacen.oracle.sqlldr")) {

  fileTxt <- paste0(
              "load data ",
              "infile '", pathToTempFile , "' ",
              "truncate ",
              "into table ", schemaName, ".", tableName, " ",
              "fields terminated by ';' ",
              "( ", .getColNamesString(listColTypes), " )")

  writeLines(text = fileTxt, con = pathToTempFileCtl)

  shell(cmd = paste0(pathToSqlldr, " ",
                     user, "/",
                     password, "@",
                     hostName, ":", port, "/", sid,
                     " control='", pathToTempFileCtl,
                     "' log='", pathToTempFileLog,
                     "' bad='", pathToTempFileBad, "'"))
}

# Private function that returns if query is a drop table and the table doesnt exists in database
.isDropTableAndTableDoesntExists <- function(query, hostName, port, sid, user, password) {
  if (.isDropTable(query) && .tableDoesntExists(query, hostName, port, sid, user, password))
    return (TRUE)
  else
    return (FALSE)
}

# Private function that returns if query is a drop table
.isDropTable <- function(query) {
  if (toupper(substr(query, 0, 10)) == 'DROP TABLE')
    return (TRUE)
  else
    return (FALSE)
}

# Private function that returns if table doesnt exists in database
.tableDoesntExists <- function(query, hostName, port, sid, user, password) {
  queryWithoutSpace <- trimws(query)
  queryWithoutDropStatement <- trimws(gsub('\\;', '', substr(queryWithoutSpace, 12, nchar(queryWithoutSpace))))

  schemaName <- sub('\\..*', '', queryWithoutDropStatement)
  tableName <- sub('.*\\.', '', queryWithoutDropStatement)

  .iniParametersOracle(hostName, port, sid, user, password)
  oracleConn <- DBI::dbConnect(bacen_driver, bacen_user, bacen_password, dbname = bacen_connStr)
  numberOfTables <- ROracle::dbGetQuery(oracleConn,
                                        infuser::infuse(
                                          "SELECT count(*) FROM all_tab_columns WHERE OWNER = '{{schemaName}}' and TABLE_NAME = '{{tableName}}'",
                                          schemaName = schemaName,
                                          tableName = tableName)
                                       )
  ROracle::dbDisconnect(oracleConn)

  return (ifelse(numberOfTables[[1]] == 0, TRUE, FALSE))
}

#' Function to exhibit all public functions of the package
#' @return list with all public functions of bacen library
listBacenFunctions <- function() {
  utils::lsf.str("package:bacen")
}
