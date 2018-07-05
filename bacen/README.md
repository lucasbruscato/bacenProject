#Instalação

```

devtools::install_git(
  "https://stash.uol.intranet/scm/pds/pergamo.git",
  subdir = "bacen"
)

library(bacen)

# Oracle Okane
setOracleOkaneParameters <- function() {
  options("bacen.oracle.sqlldr" = "C:/oracleClient/instantclient_12_2/sqlldr.exe")
  options("bacen.oracle.hostName" = "rpp.bd.intranet")
  options("bacen.oracle.port" = 1521)
  options("bacen.oracle.sid" = "rpp.pagseguro")
  options("bacen.oracle.user" = "manut_shinny")
  options("bacen.oracle.password" = "shinnymanut")
  options("bacen.oracle.schemaName" = "manut_shinny")
}

# Oracle Lake
setOracleLakeParameters <- function() {
  options("bacen.oracle.sqlldr" = "C:/oracleClient/instantclient_12_2/sqlldr.exe")
  options("bacen.oracle.hostName" = "lnm.bd.intranet")
  options("bacen.oracle.port" = 1521)
  options("bacen.oracle.sid" = "lnm.datascience.pagseguro")
  options("bacen.oracle.user" = "lbruscato")
  options("bacen.oracle.password" = "a")
  options("bacen.oracle.schemaName" = "lbruscato")
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

