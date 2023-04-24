context("queue_base")

test_that("empty", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_base$new(ctx, initialize = FALSE)

  expect_equal(obj$task_list(), character(0))
  expect_equal(obj$task_status(), set_names(character(0), character(0)))
  expect_equal(obj$task_status(named = FALSE), character(0))

  tt <- obj$task_times()
  expect_is(tt, "data.frame")
  expect_equal(nrow(tt), 0)

  expect_error(obj$task_get(ids::random_id()),
               "Task does not exist")
  ## Behaviour of missing tasks is tested elsewhere
  expect_is(obj$task_get(ids::random_id(), FALSE), "queuer_task")

  expect_error(obj$task_result(ids::random_id()), "unfetchable: MISSING",
               class = "UnfetchableTask")
  expect_false(obj$task_delete(ids::random_id()))

  expect_equal(obj$task_bundle_list(), character(0))
  info <- obj$task_bundle_info()
  expect_is(info, "data.frame")
  expect_equal(nrow(info), 0)

  expect_error(obj$task_bundle_get(ids::adjective_animal()),
               "not found", class = "KeyError")
})

test_that("enqueue", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  obj <- queue_base$new(ctx)
  t <- obj$enqueue(sin(1))
  expect_equal(t$status(), "PENDING")

  context::task_run(t$id, ctx)

  expect_equal(t$status(), "COMPLETE")
  expect_equal(t$result(), sin(1))
})

test_that("enqueue with dependencies", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  obj <- queue_base$new(ctx)
  t <- obj$enqueue(sin(1))

  expect_error(obj$enqueue(sin(1), depends_on = "123"),
               "Failed to save as dependency 123 does not exist")

  t2 <- obj$enqueue(sin(1), depends_on = t$id)
  t3 <- obj$enqueue(sin(1), depends_on = c(t$id, t2$id))
  expect_equal(t$status(), "PENDING")
  expect_equal(t2$status(), "PENDING")
  expect_equal(t3$status(), "PENDING")
})

test_that("create by id", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_base$new(ctx$id, ctx$root)
  expect_equal(obj$context$id, ctx$id)
})

test_that("invalid creation", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  expect_error(queue_base$new(ctx, ctx$root),
               "'root' must be NULL")
})

test_that("retry tasks", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  obj <- queue_base$new(ctx)

  t <- obj$enqueue(stop("fail"))
  t2 <- obj$enqueue(sin(1))

  expect_equal(t$status(), "PENDING")
  expect_equal(t2$status(), "PENDING")

  context::task_run(t$id, ctx)
  context::task_run(t2$id, ctx)

  expect_equal(t$status(), "ERROR")
  expect_equal(t2$status(), "COMPLETE")

  now <- Sys.time()

  expect_lt(t$times()$submitted, now)
  expect_lt(t2$times()$submitted, now)

  obj$task_retry_failed(c(t$id, t2$id))

  expect_gt(t$times()$submitted, now)
  expect_lt(t2$times()$submitted, now)

  expect_equal(t$status(), "PENDING")
  expect_equal(t2$status(), "COMPLETE")
})
