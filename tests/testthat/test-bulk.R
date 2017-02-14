context("bulk")

test_that("submit", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_base(ctx)

  x <- 1:30
  fun <- quote(sin)
  res <- obj$lapply(x, fun)

  expect_equal(res$X, x)
  expect_equal(length(res$ids), length(x))
  expect_null(res$names)
  expect_is(res$name, "character")

  expect_is(res$tasks, "list")
  expect_is(res$tasks[[1]], "queuer_task")
  expect_equal(length(res$tasks), length(x))

  ## Not amazing, but it'll do:
  expect_equal(res$tasks[[14]]$expr(), quote(base::sin(14L)))

  expect_error(res$results(), "Tasks not yet completed")
  expect_equal(res$results(TRUE), rep(list(NULL), length(x)))

  expect_equal(res$status(), setNames(rep("PENDING", length(x)), res$ids))
  expect_equal(res$status(FALSE), rep("PENDING", length(x)))
})

test_that("named", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  obj <- queue_base(ctx)

  x <- setNames(runif(10), ids::adjective_animal(10))
  fun <- quote(sin)

  res <- obj$lapply(x, fun)
  expect_equal(res$names, names(x))

  expect_equal(res$results(TRUE),
               setNames(rep(list(NULL), length(x)), names(x)))

  tmp <- lapply(res$ids, context::task_run, ctx)
  expect_equal(res$results(TRUE), as.list(sin(x)))
})

test_that("named group", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_base(ctx)
  nm <- ids::sentence()
  res <- obj$lapply(1:30, quote(sin), name = nm)
  expect_equal(res$name, nm)
})

test_that("named lapply", {
  ctx <- context::context_save(tempfile())
  obj <- queue_local(ctx)
  bundle <- obj$lapply(setNames(as.list(1:3), letters[1:3]), I)

  expect_equal(bundle$tasks[[1]]$expr(),
               quote(base::I(1L)))
  res <- obj$run_next()
  expect_equal(res$value, I(1L))
  expect_equal(bundle$names, letters[1:3])

  obj$run_all()

  ## Check names are returned:
  expect_equal(bundle$results(), setNames(lapply(bundle$X, I), letters[1:3]))
})

test_that("$enqueue_bulk", {
  ctx <- context::context_save(tempfile())
  obj <- queue_local(ctx)
  bundle <- obj$enqueue_bulk(1:3, quote(I))
  expect_is(bundle, "task_bundle")
  expect_equal(bundle$function_name(), "base::I")
})

test_that("exotic functions", {
  Sys.setenv(R_TESTS = "")

  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_local(ctx)

  x <- 1:5
  res <- local({
    f_local <- function(x) {
      x + 2
    }
    obj$lapply(x, quote(f_local), progress = FALSE, timeout = 0)
  })

  expect_equal(length(obj$task_list()), length(x))
  done <- obj$run_all()
  expect_equal(done, res$ids)
  expect_equal(res$results(), as.list(x + 2))

  res <- local({
    obj$lapply(x, function(x) x + 3, progress = FALSE, timeout = 0)
  })
  expect_equal(obj$run_all(), res$ids)
  expect_equal(res$results(), as.list(x + 3))
})
