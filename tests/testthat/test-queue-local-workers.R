context("queue_local workers")

test_that("runner", {
  ctx <- context::context_save(tempfile(), sources = "functions.R")
  obj <- queue_local(ctx)
  x <- runif(4, max = 0.1)
  res <- obj$lapply(x, "slow_double")

  path_worker <- file.path(ctx$root$path, "bin", "queue_local_worker")
  px <- processx::process$new(path_worker, c(ctx$root$path, ctx$id))

  on.exit({
    if (px$is_alive()) px$kill(0)
    unlink(ctx$root$path)
  })

  ans <- res$wait(100, time_poll = 0.02, progress_bar = interactive())
  expect_equal(ans, as.list(x * 2))
})
