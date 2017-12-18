
install.packages("../bacen_0.1.0.tar.gz", repos = NULL, type="source")

library(bacen)

# Temporary directory and name of temp file
print(paste0(tempdir(), "/bacenTransferTemporaryTable.csv"))

# Test 1
host <- "//a1-zueira1:5433/okane"
user <- Sys.info()[["user"]]
password <- "a"
schemaName <- "LBRUSCATO"
test1_1 <- read.csv2("../tests_bacen/test1_1.csv", header = TRUE, sep = ";")
tableName <- "test1_1"

rertica(host = host,
        user = user,
        password = password,
        schemaName = schemaName,
        df = test1_1,
        tableName = tableName)

library(RJDBC)

bacen_jdbcClassPath <- "./inst/java/vertica-jdbc-7.2.1-0.jar"
bacen_jdbcConnStr <- paste0("jdbc:vertica:", host)
bacen_jdbcUsr <- user
bacen_jdbcPwd <- password
bacen_driver <- JDBC(driverClass="com.vertica.jdbc.Driver", bacen_jdbcClassPath)
verticaConn <- dbConnect(bacen_driver, bacen_jdbcConnStr, bacen_jdbcUsr, bacen_jdbcPwd)
test1_2 <- dbGetQuery(conn = verticaConn, "select * from lbruscato.test1_1")
dbDisconnect(verticaConn)

write.table(test1_2, "../tests_bacen/test1_2.csv", sep = ";")
test1_2 <- read.csv2("../tests_bacen/test1_2.csv", header = TRUE, sep = ";")

all.equal(test1_1, test1_2)

# Test 2
host <- "//a1-zueira1:5433/okane"
user <- Sys.info()[["user"]]
password <- "a"
schemaName <- "LBRUSCATO"
test2_1 <- read.csv2("../tests_bacen/test2_1.csv", header = TRUE, sep = ";")
tableName <- "test2_1"

rertica(host = host,
        user = user,
        password = password,
        schemaName = schemaName,
        df = test2_1,
        tableName = tableName)

library(RJDBC)

bacen_jdbcClassPath <- "./inst/java/vertica-jdbc-7.2.1-0.jar"
bacen_jdbcConnStr <- paste0("jdbc:vertica:", host)
bacen_jdbcUsr <- user
bacen_jdbcPwd <- password
bacen_driver <- JDBC(driverClass="com.vertica.jdbc.Driver", bacen_jdbcClassPath)
verticaConn <- dbConnect(bacen_driver, bacen_jdbcConnStr, bacen_jdbcUsr, bacen_jdbcPwd)
test2_2 <- dbGetQuery(conn = verticaConn, "select * from lbruscato.test2_1")
dbDisconnect(verticaConn)

write.table(test2_2, "../tests_bacen/test2_2.csv", sep = ";")
test2_2 <- read.csv2("../tests_bacen/test2_2.csv", header = TRUE, sep = ";")

all.equal(test2_1, test2_2)
