context("match_fun")

## This is partly taken from rrqueue.  It's by far the nastiest bit of
## the process - trying to work out if we can run something.  I might
## use a simpler version of it here though to allow anon functions
## through.

## The testing is still really nasty here because I don't know that I
## can assume that contexts always have a global root.  But here I'll
## assume that.

test_that("match_fun", {
  ctx <- context::context_save(tempfile(), sources = "scope.R")
  context::context_load(ctx)

  cmp <- list(namespace = NULL, name = "f1", envir = .GlobalEnv, value = f1)
  res <- local({
    e <- environment()
    match_fun(quote(f1), e)
  })
  expect_equal(res, cmp)

  res <- local({
    e <- environment()
    match_fun("f1", e)
  })
  expect_equal(res, cmp)

  res <- local({
    e <- environment()
    match_fun(f1, e)
  })
  expect_equal(res, cmp)

  res <- local({
    e <- environment()
    f <- function(x) x + 2
    match_fun(f, e)
  })
  expect_equal(res[c("namespace", "name")], list(namespace = NULL, name = "f"))
  expect_is(res$envir, "environment")
  expect_is(res$value, "function")
  expect_identical(res$envir, environment(res$value))
  expect_equal(res$value, function(x) x + 2)

  res <- local({
    e <- environment()
    match_fun(function(x) x + 2, e)
  })
  expect_equal(res[c("namespace", "name")], list(namespace = NULL, name = NULL))
  expect_is(res$envir, "environment")
  expect_is(res$value, "function")
  expect_identical(res$envir, environment(res$value))
  expect_equal(res$value, function(x) x + 2)

  cmp <- list(namespace = "stats", name = "dnorm",
              envir = asNamespace("stats"), value = dnorm)

  res <- local({
    e <- environment()
    match_fun(dnorm, e)
  })
  expect_identical(res, cmp)

  res <- local({
    e <- environment()
    match_fun(stats::dnorm, e)
  })
  expect_identical(res, cmp)

  res <- local({
    e <- environment()
    match_fun("dnorm", e)
  })
  expect_identical(res, cmp)

  res <- local({
    e <- environment()
    match_fun("stats::dnorm", e)
  })
  expect_identical(res, cmp)

  res <- local({
    e <- environment()
    match_fun(quote(dnorm), e)
  })
  expect_identical(res, cmp)

  res <- local({
    e <- environment()
    match_fun(quote(stats::dnorm), e)
  })
  expect_identical(res, cmp)

  res <- local({
    e <- environment()
    f <- dnorm
    match_fun(f, e)
  })
  expect_identical(res, cmp)
})

## Now, the fun starts.
test_that("match_fun_queue", {
  res <- local({
    e <- environment()
    f <- function(x) x + 1
    match_fun_queue(f, e, .GlobalEnv)
  })
  expect_null(res$name)
})

test_that("primative", {
  e <- environment()
  res <- match_fun(list, e)
  expect_equal(res$name, "list")
})

test_that("primative, 2", {
  res <- match_fun(list, .GlobalEnv)
  expect_equal(res$name, "list")
})
