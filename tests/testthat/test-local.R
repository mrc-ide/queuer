context("queue_local")

test_that("empty queue", {
  ctx <- context::context_save(root=tempfile())
  on.exit(unlink(ctx$db$destroy()))
  obj <- queue_local(ctx)
  expect_equal(obj$tasks_list(), character(0))
  expect_equal(obj$queue_list(), character(0))
  expect_equal(obj$run_next(), list(task_id=NULL, value=NULL))
  expect_equal(obj$run_all(), character(0))

  tt <- obj$tasks_times()
  expect_is(tt, "data.frame")
  expect_equal(nrow(tt), 0L)
})

test_that("enqueue", {
  ctx <- context::context_save(root=tempfile())
  on.exit(unlink(ctx$db$destroy()))
  obj <- queue_local(ctx)

  t <- obj$enqueue_(quote(sin(1)))

  expect_is(t, "task")
  expect_equal(obj$queue_list(), t$id)
  expect_equal(t$status(), "PENDING")

  res <- obj$run_next()
  expect_equal(res, list(task_id=t$id, value=t$result()))
  expect_equal(obj$run_next(), list(task_id=NULL, value=NULL))

  expect_equal(obj$tasks_list(), t$id)
  expect_equal(obj$queue_list(), character(0))

  for (i in seq_len(10)) {
    obj$enqueue(sin(i))
  }
  expect_equal(length(obj$tasks_list()), 11L)
  expect_equal(length(obj$queue_list()), 10L)
  ## The first task really is the one with i=1:
  t1 <- obj$task_get(obj$queue_list()[[1]])
  expect_equal(t1$expr(TRUE),
               structure(quote(sin(i)), locals=list(i=1L)))

  res <- obj$run_next()
  expect_equal(res, list(task_id=t1$id, value=sin(1)))

  ord <- obj$queue_list()

  res <- obj$run_all()
  expect_equal(res, ord)
  expect_equal(obj$queue_list(), character(0))

  tt <- obj$tasks_times()
  expect_is(tt, "data.frame")
  expect_equal(nrow(tt), 11)
  expect_equal(tt$task_id, c(t$id, t1$id, ord))
})
