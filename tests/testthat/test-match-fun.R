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
  ctx <- context::context_load(ctx, .GlobalEnv)

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
  expect_equal(res$value(4), 6)

  res <- local({
    e <- environment()
    match_fun(function(x) x + 2, e)
  })
  expect_equal(res[c("namespace", "name")], list(namespace = NULL, name = NULL))
  expect_is(res$envir, "environment")
  expect_is(res$value, "function")
  expect_identical(res$envir, environment(res$value))
  expect_equal(res$value(4), 6)

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

test_that("symbol", {
  expect_identical(match_fun(quote(sin), .GlobalEnv),
                   match_fun("sin", .GlobalEnv))
})

test_that("error case", {
  expect_error(match_fun(1, .GlobalEnv), "Invalid input")
})

test_that("find primitive by value", {
  res <- match_fun(sin, .GlobalEnv)
  expect_equal(res$namespace, "base")
  expect_equal(res$name, "sin")
  expect_identical(res$envir, baseenv())
  expect_equal(res$value, sin)
})

test_that("missing function by name in namespace", {
  expect_equal(match_fun_name("stats::df", .GlobalEnv)$name,
               "df")
  expect_error(
    match_fun_name("stats::no_such_function", .GlobalEnv),
    "Did not find function 'no_such_function' in namespace 'stats'")
})

test_that("missing function by name", {
  expect_error(match_fun_name("no_such_function", .GlobalEnv),
               "Did not find function 'no_such_function' in environment")
})

test_that("missing function by value", {
  expect_error(match_fun_value(function(x) my_thing(x), .GlobalEnv),
               "Did not find function")
})

test_that("invalid namespace", {
  expect_error(match_fun_name("stats::df::foo", .GlobalEnv),
               "Invalid namespace-qualified name 'stats::df::foo'")
})

test_that("function in non-loaded namespace", {
  expect_error(match_fun_name("nonamespace::myfunction", .GlobalEnv),
               "Did not find function 'myfunction' in namespace 'nonamespace'")
})

test_that("nested environments", {
  local({
    fun <- local(local(local(function(x) x + 1)))
    e1 <- environment()
    e2 <- new.env(parent = e1)
    e3 <- new.env(parent = e2)
    expect_identical(match_fun_value(fun, e1)$envir, e1)
    expect_identical(match_fun_value(fun, e2)$envir, e1)
    expect_identical(match_fun_value(fun, e3)$envir, e1)
  })
})
