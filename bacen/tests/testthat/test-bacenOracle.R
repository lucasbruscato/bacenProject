context("oracle - oracleSendQuery")

test_that("oracleSendQuery fails when parameters are not setted", {
  expect_error(
    oracleSendQuery("SELECT 1 FROM DUAL", hostName = NULL)
  )
  expect_error(
    oracleSendQuery("SELECT 1 FROM DUAL", port = NULL)
  )
  expect_error(
    oracleSendQuery("SELECT 1 FROM DUAL", sid = NULL)
  )
  expect_error(
    oracleSendQuery("SELECT 1 FROM DUAL", user = NULL)
  )
  expect_error(
    oracleSendQuery("SELECT 1 FROM DUAL", password = NULL)
  )
})

test_that("oracleSendQuery returns values when everything is fine", {
  expect_equal(
    oracleSendQuery("SELECT 1 FROM DUAL", user = 'fsiola', password = "mudar123")$`1`,
    1
  )
})

test_that("oracleSendQuery returns values when everything is fine", {
  expect_message(
    oracleSendQuery("DROP TABLE INEXISTENT_TABLE_TEST_BACEN", user = 'fsiola', password = "mudar123")$`1`,
    "Table doesn't exists"
  )
  expect_equal(
    oracleSendQuery("DROP TABLE INEXISTENT_TABLE_TEST_BACEN", user = 'fsiola', password = "mudar123")$`1`,
    NULL
  )
})

########################################

context("Oracle - roracle")

test_that("roracle fails when parameters are not setted", {
  expect_error(
    roracle(data.frame(), "teste", hostName = NULL)
  )
  expect_error(
    roracle(data.frame(), "teste", port = NULL)
  )
  expect_error(
    roracle(data.frame(), "teste", sid = NULL)
  )
  expect_error(
    roracle(data.frame(), "teste", user = NULL)
  )
  expect_error(
    roracle(data.frame(), "teste", password = NULL)
  )
})

test_that("roracle fails when df is NULL or has no rows", {
  expect_error(
    roracle(NULL, "teste", user = 'fsiola', password = "mudar123")
  )
  expect_error(
    roracle(data.frame(), "teste", user = 'fsiola', password = "mudar123")
  )
})

# test_that("roracle corrects load table to oracle", {
#   options("bacen.oracle.sqlldr" = "C:/oracleClient/instantclient_12_2/sqlldr.exe")
#
#   currentDate <- Sys.Date()
#   currentDateDP1 <- Sys.Date() + 1
#   currentDateDN1 <- Sys.Date() - 1
#   currentTime <- Sys.time()
#   currentTimeDP1 <- Sys.time() + 1
#   currentTimeDN1 <- Sys.time() - 1
#
#   df <- data.frame(colunaChar = c("a","b","c"),
#                    colunaInt = c(1,2,3),
#                    colunaFloat = c(1.1,2.2,3.3),
#                    colunaBoolean = c(T,F,T),
#                    colunaDate = c(currentDate, currentDateDP1, currentDateDN1),
#                    colunaTime = c(currentTime, currentTimeDP1, currentTimeDN1),
#                    acentuacao = c("à", "â", 'ã'))
#
#   roracle(df, "TABELATESTTHAT", schemaName='fsiola', user = 'fsiola', password = "mudar123")
#
#   result <- oracleSendQuery("SELECT * FROM fsiola.TABELATESTTHAT", user = 'fsiola', password = "mudar123")
#
#   expect_equal(nrow(result), 3)
#   expect_equal(colnames(result), c("COLUNACHAR", "COLUNAINT", "COLUNAFLOAT", "COLUNABOOLEAN", "COLUNADATE", "COLUNATIME", "ACENTUACAO"))
#   expect_equal(result[,1], c("a","b","c"))
#   expect_equal(result[,2], c(1,2,3))
#   expect_equal(result[,3], c(1.1,2.2,3.3))
#   expect_equal(result[,4], c("TRUE","FALSE","TRUE")) # saving boolean as char
#   #expect_equal(result[,5], as.POSIXct(c(currentDate, currentDateDP1, currentDateDN1))) # timestamp diff
#   #expect_equal(result[,6], as.POSIXct(c(currentTime, currentTimeDP1, currentTimeDN1))) # seconds diff
#   #expect_equal(result[,7], c("à", "â", 'ã')) # accentuation messes up because of roracle
# })

test_that("roracle warning when file not fully loaded", {
  options("bacen.oracle.sqlldr" = "C:/oracleClient/instantclient_12_2/sqlldr.exe")

  df <- data.frame(erro = c(Inf))

  expect_warning(
    roracle(df, "TABELATESTTHAT", schemaName='fsiola', user = 'fsiola', password = "mudar123")
  )
})
