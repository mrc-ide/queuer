context("queuer_tasks")

test_that("basic", {
  path <- tempfile("queuer_")
  ctx <- context::context_save(root=path)
  th <- context::task_save(quote(sin(1)), ctx)

  t <- task(ctx, th$id)

  expect_is(t, "task")
  expect_equal(t$status(), "PENDING")
  expect_equal(t$expr(), quote(sin(1)))
  expect_equal(t$context_id(), ctx$id)
  expect_error(t$result(), "is unfetchable")
  ## expect_error(t$log(), "No log for")
  expect_error(t$wait(0), "task not returned in time")
  expect_error(t$wait(0.01), "task not returned in time")
  expect_true(system.time(try(t$wait(0.01), silent=TRUE))[["elapsed"]] < 0.5)

  tt <- t$times()
  expect_is(tt, "data.frame")
  expect_is(tt$submitted, "POSIXt")
  expect_equal(tt$started, as.POSIXct(NA))
  expect_equal(tt$finished, as.POSIXct(NA))

  ## file <- path_log(t$root, t$id)
  file <- NULL
  context::context_log_start()
  res <- context::task_run(th, filename=file)
  ## readLines(file)
  context::context_log_stop()
  ## t$log()

  expect_equal(t$status(), "COMPLETE")
  expect_equal(t$result(), sin(1))
  expect_equal(t$wait(0), sin(1))
  expect_equal(t$wait(0.1), sin(1))

  ## t$times()
  ## tasks_list(t)
})
