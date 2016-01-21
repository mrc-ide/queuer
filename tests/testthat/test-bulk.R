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
