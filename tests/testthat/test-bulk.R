context("bulk")

test_that("single process", {
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

  done <- obj$run_all()
  dat <- res$wait(0)
  expect_equal(dat, as.list(sin(1:10)))
})

## OK, this _was_ working and now the worker is not seeing any tasks.
## So something terrible has happened!
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

  x <- setNames(runif(4, max=0.1), letters[1:4])

  res1 <- enqueue_bulk_submit(obj, x, "slow_double")
  expect_equal(obj$task_bundles_list(), res1$name)
  expect_equal(res1$names, names(x))
  expect_equal(res1$X, x)

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
})
