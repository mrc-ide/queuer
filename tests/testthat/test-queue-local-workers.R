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

  ans <- res$wait(100, time_poll = 0.02, progress = FALSE)
  expect_equal(ans, as.list(x * 2))
})

## From command line:
##
## writeLines(paste(c(path_worker, args), collapse = " "))
## cat(sprintf('queue_local_worker("%s", "%s", TRUE)\n', ctx$root$path, ctx$id))
test_that("runner loop", {
  ctx <- context::context_save(tempfile(), sources = "functions.R")
  obj <- queue_local(ctx)

  path_worker <- file.path(ctx$root$path, "bin", "queue_local_worker")
  args <- c(ctx$root$path, ctx$id, TRUE)

  px <- processx::process$new(path_worker, args)
  on.exit({
    if (px$is_alive()) px$kill(0)
    unlink(ctx$root$path)
  })

  x <- runif(4, max = 0.1)
  id <- ids::sentence()
  res <- obj$lapply(x, "slow_double", timeout = 100, time_poll = 0.02,
                    progress = FALSE, name = id)
  expect_equal(res, as.list(x * 2))

  ## Can also get things this way:
  grp <- obj$task_bundle_get(id)
  expect_is(grp, "task_bundle")
  expect_equal(grp$results(), as.list(x * 2))

  pid <- obj$enqueue(Sys.getpid())$wait(Inf, 0.02, FALSE)

  tools::pskill(pid, tools::SIGINT)
  times_up <- time_checker(1)
  while (px$is_alive() && !times_up()) {
    Sys.sleep(0.02)
  }
  expect_false(px$is_alive())
})
