context("bulk")

test_that("single process", {
  Sys.setenv(R_TESTS="")
  ctx <- context::context_save(root=tempfile(), storage_type="environment")
  obj <- queue_local(ctx)
  res <- enqueue_bulk_submit(obj, 1:10, quote(sin))

  expect_is(res, "task_bundle")
  expect_equal(length(res$ids), 10L)
  expect_equal(res$done, setNames(rep(FALSE, 10L), res$ids))

  expect_error(res$results(), "Tasks not yet completed")
  expect_error(res$wait(0.1, 0.01, progress_bar=interactive()),
               "Exceeded maximum time")
  if (interactive()) {
    cat("\n")
  }

  expect_is(res$to_handle(), "task_handle")
  expect_is(res$times(), "data.frame")

  done <- obj$run_all()
  dat <- res$wait(0)
  expect_equal(dat, as.list(sin(1:10)))
  expect_equal(done, res$ids)
})

test_that("worker", {
  ctx <- context::context_save(root=tempfile(), sources="functions.R")
  obj <- queue_local(ctx)
  x <- runif(4, max=0.1)
  res <- enqueue_bulk_submit(obj, x, "slow_double")

  (log <- tempfile())
  cl <- start_cluster(ctx$root, ctx$id, outfile=log)
  on.exit(cl$stop())

  expect_true(cl$exists())
  cl$send_call(queuer:::queue_local_worker, list(ctx$root, ctx$id, FALSE))

  ans <- res$wait(100, time_poll=0.02, progress_bar=interactive())
  expect_equal(ans, as.list(x * 2))

  ok <- cl$can_return_result()
  expect_true(ok)
  if (ok) {
    expect_equal(cl$recieve()[[1]], res$ids)
  }
})

test_that("task_bundles", {
  context::context_log_start()
  ctx <- context::context_save(root=tempfile(), sources="functions.R")
  obj <- queue_local(ctx)

  expect_equal(obj$task_bundles_list(), character(0))
  info <- obj$task_bundles_info()
  expect_is(info, "data.frame")
  expect_equal(nrow(info), 0L)
  expect_equal(names(info), c("name", "function", "length", "created"))

  x <- setNames(runif(4, max=0.1), letters[1:4])

  res1 <- enqueue_bulk_submit(obj, x, "slow_double")
  expect_equal(obj$task_bundles_list(), res1$name)
  expect_equal(res1$names, names(x))
  expect_equal(res1$X, x)

  info <- obj$task_bundles_info()
  expect_equal(nrow(info), 1L)
  expect_equal(info$name, res1$name)
  expect_equal(info$length, length(res1$ids))
  expect_equal(info$"function", "slow_double")

  res2 <- enqueue_bulk_submit(obj, unname(x), "slow_double")
  expect_equal(sort(obj$task_bundles_list()),
               sort(c(res1$name, res2$name)))
  expect_null(res2$names)
  expect_equal(res2$X, unname(x))

  x <- expand.grid(a=1:2, b=letters[1:3], stringsAsFactors=FALSE)
  rownames(x) <- LETTERS[seq_len(nrow(x))]
  res3 <- enqueue_bulk_submit(obj, x, "list", do.call=TRUE)
  expect_equal(res3$names, rownames(x))
  expect_equal(res3$X, x)
  expect_equal(as.list(res3$tasks[[1]]$expr())[-1],
               df_to_list(x, TRUE)[[1]])

  res4 <- enqueue_bulk_submit(obj, x, "list", do.call=TRUE, use_names=FALSE)
  expect_equal(res4$names, rownames(x))
  expect_equal(res4$X, x)
  expect_equal(as.list(res4$tasks[[1]]$expr())[-1],
               df_to_list(x, FALSE)[[1]])

  res5 <- enqueue_bulk_submit(obj, x, "list", name="mygroup")
  expect_equal(res5$name, "mygroup")

  t <- obj$tasks_list()
  expect_error(enqueue_bulk_submit(obj, x, "list", name="mygroup"),
               "Task bundle already exists")
  ## No tasks were actually added here:
  expect_equal(obj$tasks_list(), t)

  res6 <- enqueue_bulk_submit(obj, x, "list", name="mygroup", overwrite=TRUE)
  expect_equal(res6$name, "mygroup")
  expect_equal(sort(c(t, res6$ids)), sort(obj$tasks_list()))
  expect_equal(intersect(res5$ids, res6$ids), character(0))

  cmp <- c(res1$name, res2$name, res3$name, res4$name, res6$name)

  expect_equal(obj$task_bundles_list(), sort(cmp))

  info <- obj$task_bundles_info()
  expect_equal(info$name, cmp)
  expect_equal(info$length,
               c(length(res1$ids), length(res2$ids), length(res3$ids),
                 length(res4$ids), length(res6$ids)))
  expect_equal(info$"function",
               rep(c("slow_double", "base::list"), c(2, 3)))
  expect_gte(min(as.numeric(diff(info$created))), 0)
})

test_that("task_bundles", {
  ctx <- context::context_save(root=tempfile(), storage_type="environment")
  obj <- queue_local(ctx)
  expect_error(task_bundle_create(obj, character(0)),
               "task_ids must be nonempty")
})
