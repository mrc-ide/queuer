context("queue_local")

test_that("empty queue", {
  ctx <- context::context_save(tempfile())
  on.exit(unlink(ctx$db$destroy()))
  obj <- queue_local(ctx)
  expect_equal(obj$task_list(), character(0))
  expect_equal(obj$queue_list(), character(0))
  expect_equal(obj$run_next(), list(task_id = NULL, value = NULL))
  expect_equal(obj$run_all(), character(0))

  tt <- obj$task_times()
  expect_is(tt, "data.frame")
  expect_equal(nrow(tt), 0L)
})

test_that("enqueue", {
  ctx <- context::context_save(tempfile())
  on.exit(unlink(ctx$db$destroy()))
  log_path <- "logs"
  obj <- queue_local(ctx, log = TRUE)

  expect_is(obj$log_path, "character")
  expect_true(file.exists(obj$log_path))

  if (context::context_log_start()) {
    on.exit(context::context_log_stop(), add = TRUE)
  }
  ## For some reason, this will not record the log correctly on the
  ## *first* task here.  Not sure why
  t <- obj$enqueue_(quote(sin(1)))

  expect_is(t, "queuer_task")
  expect_equal(obj$queue_list(), t$id)
  expect_equal(t$status(), "PENDING")

  ## Can't test here that we print the "Running" message because
  ## testthat clobbers all future messages :(
  ##
  ## https://github.com/hadley/testthat/issues/460
  res <- obj$run_next()
  expect_equal(res, list(task_id = t$id, value = t$result()))
  expect_silent(res <- obj$run_next())
  expect_equal(res, list(task_id = NULL, value = NULL))

  expect_equal(obj$task_list(), t$id)
  expect_equal(obj$queue_list(), character(0))

  ## TODO: not working:
  expect_true(file.exists(file.path(obj$log_path, t$id)))
  readLines(file.path(obj$log_path, t$id))
  expect_is(t$log(), "context_log")

  ## Better to do this with bulk, but that falls out of scope of what
  ## I want to test here, really.
  for (i in seq_len(10)) {
    obj$enqueue(sin(i))
  }
  expect_equal(length(obj$task_list()), 11L)
  expect_equal(length(obj$queue_list()), 10L)
  ## The first task really is the one with i = 1:
  t1 <- obj$task_get(obj$queue_list()[[1]])
  expect_equal(t1$expr(TRUE),
               structure(quote(sin(i)), locals = list(i = 1L)))

  res <- obj$run_next()
  expect_equal(res, list(task_id = t1$id, value = sin(1)))

  ord <- obj$queue_list()

  res <- obj$run_all()
  expect_equal(res, ord)
  expect_equal(obj$queue_list(), character(0))

  tt <- obj$task_times()
  expect_is(tt, "data.frame")
  expect_equal(nrow(tt), 11)
  ## This is too strict on windows because timing is not sensitive
  ## enough.
  if (!is_windows()) {
    expect_equal(tt$task_id, c(t$id, t1$id, ord))
  }
})

test_that("environment storage", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  on.exit(unlink(ctx$db$destroy()))

  obj <- queue_local(ctx)
  t <- obj$enqueue(sin(1))

  expect_is(t, "queuer_task")
  expect_equal(t$status(), "PENDING")

  expect_equal(obj$run_next(),
               list(task_id = t$id, value = sin(1)))
  expect_equal(t$status(), "COMPLETE")

  for (i in seq_len(10)) {
    obj$enqueue(sin(i))
  }
  res <- obj$run_all()
  expect_equal(lapply(res, function(x) obj$task_get(x)$result()),
               as.list(sin(1:10)))
})

test_that("initialise later", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  on.exit(unlink(ctx$db$destroy()))
  obj <- queue_local(ctx, initialize = FALSE)
  expect_null(obj$context$envir)

  expect_message(t <- obj$enqueue(sin(1)), "Loading context")
  expect_is(obj$context$envir, "environment")
  expect_is(t, "queuer_task")
  res <- obj$run_next()
  expect_equal(res, list(task_id = t$id, value = t$result()))
})

test_that("unsubmit", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  on.exit(unlink(ctx$db$destroy()))

  obj <- queue_local(ctx)
  for (i in seq_len(10)) {
    obj$enqueue(sin(i))
  }

  ids <- obj$queue_list()
  rem <- sample(ids, 4)
  expect_equal(obj$unsubmit(rem), rep(TRUE, length(rem)))
  expect_equal(obj$queue_list(), setdiff(ids, rem))

  res <- obj$task_delete(ids)
  expect_equal(res, rep(TRUE, length(ids)))
  expect_equal(obj$queue_list(), character(0))
})

test_that("submit_or_delete", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  on.exit(unlink(ctx$db$destroy()))

  obj <- queue_local(ctx)
  ## Then we sabotage the queue to test that submit_or_delete works:
  obj$fifo <- NULL
  expect_message(try(obj$enqueue(sin(1)), silent = TRUE), "Deleting task")
})
