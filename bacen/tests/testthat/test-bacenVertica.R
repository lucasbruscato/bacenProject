context("Vertica - verticaGetQuery")

test_that("verticaGetQuery fails when parameters are not setted", {
  expect_error(
    verticaGetQuery("SELECT 1", hostName = NULL)
  )
  expect_error(
    verticaGetQuery("SELECT 1", port = NULL)
  )
  expect_error(
    verticaGetQuery("SELECT 1", databaseName = NULL)
  )
  expect_error(
    verticaGetQuery("SELECT 1", user = NULL)
  )
  expect_error(
    verticaGetQuery("SELECT 1", password = NULL)
  )
})

test_that("verticaGetQuery returns values when everthing is fine", {
  expect_equal(
    verticaGetQuery("SELECT 1")$`?column?`,
    1
  )
})

########################################

context("Vertica - verticaSendUpdate")

test_that("verticaSendUpdate fails when parameters are not setted", {
  expect_error(
    verticaSendUpdate("SELECT 1", hostName = NULL)
  )
  expect_error(
    verticaSendUpdate("SELECT 1", port = NULL)
  )
  expect_error(
    verticaSendUpdate("SELECT 1", databaseName = NULL)
  )
  expect_error(
    verticaSendUpdate("SELECT 1", user = NULL)
  )
  expect_error(
    verticaSendUpdate("SELECT 1", password = NULL)
  )
})

test_that("verticaSendUpdate drops table when everthing is fine", {
  expect_true(verticaSendUpdate("DROP TABLE IF EXISTS public.TABELAINCRIVELCOMNOMESUPREENDENTE"))
})

########################################

context("Vertica - rertica")

test_that("rertica fails when parameters are not setted", {
  expect_error(
    rertica(data.frame(), "teste", hostName = NULL)
  )
  expect_error(
    rertica(data.frame(), "teste", port = NULL)
  )
  expect_error(
    rertica(data.frame(), "teste", databaseName = NULL)
  )
  expect_error(
    rertica(data.frame(), "teste", user = NULL)
  )
  expect_error(
    rertica(data.frame(), "teste", password = NULL)
  )
})

test_that("rertica fails when df is NULL or has no rows", {
  expect_error(
    rertica(NULL, "teste")
  )
  expect_error(
    rertica(data.frame(), "teste")
  )
})

test_that("rertica corrects load table to vertica", {
  currentDate <- Sys.Date()
  currentDateDP1 <- Sys.Date() + 1
  currentDateDN1 <- Sys.Date() - 1
  currentTime <- Sys.time()
  currentTimeDP1 <- Sys.time() + 1
  currentTimeDN1 <- Sys.time() - 1

  df <- data.frame(colunaChar = c("a","b","c"),
                   colunaInt = c(1,2,3),
                   colunaFloat = c(1.1,2.2,3.3),
                   colunaBoolean = c(T,F,T),
                   colunaDate = c(currentDate, currentDateDP1, currentDateDN1),
                   colunaTime = c(currentTime, currentTimeDP1, currentTimeDN1),
                   acentuacao = c("â", "à", 'ã'))

  rertica(df, "TABELATESTTHAT")

  result <- verticaGetQuery("SELECT * FROM public.TABELATESTTHAT")

  expect_equal(nrow(result), 3)
  expect_equal(colnames(result), c("colunaChar", "colunaInt", "colunaFloat", "colunaBoolean", "colunaDate", "colunaTime", "acentuacao"))
  expect_equal(result[,1], c("a","b","c"))
  expect_equal(result[,2], c(1,2,3))
  expect_equal(result[,3], c(1.1,2.2,3.3))
  expect_equal(result[,4], c("t","f","t")) #problems with RJDBC
  expect_equal(result[,5], as.character(c(currentDate, currentDateDP1, currentDateDN1)))
  expect_equal(result[,6], as.character(c(currentTime, currentTimeDP1, currentTimeDN1)))
  expect_equal(result[,7], c("â", "à", 'ã'))
})

test_that("rertica fails when file not correct loaded with abort = T", {
  df <- data.frame(erro = c("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))

  expect_error(
    rertica(df, "TABELATESTTHAT", abort=T)
  )
})

test_that("rertica warning when file not fully loaded", {
  df <- data.frame(erro = c(Inf))

  expect_warning(
    rertica(df, "TABELATESTTHAT")
  )
})

########################################

context("Vertica - oracleToVertica")

test_that("oracleToVertica fails when table doesnt exists on oracle", {
  expect_error(
    oracleToVertica("NULL", "teste","TESTE","TESTE")
  )
})

test_that("oracleToVertica corrects load from oracle to vertica", {
  oracleToVertica("A","FSIOLA","A","FSIOLA", userOracle = 'fsiola', passwordOracle = "mudar123")

  result <- verticaGetQuery("SELECT * FROM FSIOLA.A")

  expect_equal(nrow(result), 1)
  expect_equal(colnames(result), c("A"))
  expect_equal(result[,1], c(1))
})
