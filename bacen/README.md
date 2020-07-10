#Instalação

```

devtools::install_git(
  "repositorio aqui",
  subdir = "bacen"
)

library(bacen)

# Oracle Okane
setOracleOkaneParameters <- function() {
  options("bacen.oracle.sqlldr" = "C:/oracleClient/instantclient_12_2/sqlldr.exe")
  options("bacen.oracle.hostName" = "hostname")
  options("bacen.oracle.port" = port_int)
  options("bacen.oracle.sid" = "sid")
  options("bacen.oracle.user" = "user")
  options("bacen.oracle.password" = "password")
  options("bacen.oracle.schemaName" = "schema")
}

# Oracle Lake
setOracleLakeParameters <- function() {
  options("bacen.oracle.sqlldr" = "C:/oracleClient/instantclient_12_2/sqlldr.exe")
  options("bacen.oracle.hostName" = "hostname")
  options("bacen.oracle.port" = port_int)
  options("bacen.oracle.sid" = "sid")
  options("bacen.oracle.user" = "user")
  options("bacen.oracle.password" = "password")
  options("bacen.oracle.schemaName" = "schema")
}

# Vertica
setVerticaParameters <- function() {
  options("bacen.vertica.hostName" = "a1-zueira1")
  options("bacen.vertica.port" = 5433)
  options("bacen.vertica.databaseName" = "okane")
  options("bacen.vertica.user" = Sys.getenv("USERNAME"))
  options("bacen.vertica.password" = "a")
}
s
```

