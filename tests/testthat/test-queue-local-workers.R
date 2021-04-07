context("queue_local workers")

test_that("in process", {
  ctx <- context::context_save(tempfile(), sources = "functions.R")
  obj <- queue_local(ctx)
  x <- runif(4, max = 0.1)
  res <- obj$lapply(x, "slow_double")

  expect_message(queue_local_worker(ctx$root$path, ctx$id, FALSE),
                 "worker")

  queue_local_worker(ctx$root$path, ctx$id, FALSE)

  expect_equal(res$results(), as.list(x * 2))
})


test_that("runner", {
  skip_if_not_installed("callr")
  skip_on_os("windows")
  ctx <- context::context_save(tempfile(), sources = "functions.R")
  obj <- queue_local(ctx)
  x <- runif(4, max = 0.1)
  res <- obj$lapply(x, "slow_double")

  path_worker <- file.path(ctx$root$path, "bin", "queue_local_worker")
  expect_true(file.exists(path_worker))

  ans <- callr::rscript(path_worker, c(ctx$root$path, ctx$id), show = FALSE)
  expect_equal(ans$status, 0)

  ans <- res$results()
  expect_equal(ans, as.list(x * 2))
})


## From command line:
##
## writeLines(paste(c(path_worker, args), collapse = " "))
## cat(sprintf('queue_local_worker("%s", "%s", TRUE)\n', ctx$root$path, ctx$id))
test_that("runner loop", {
  skip_on_os("windows") # requires interrupt support
  skip_if_not_installed("callr")
  ctx <- context::context_save(tempfile(), sources = "functions.R")
  obj <- queue_local(ctx)

  px <- callr::r_bg(function(args) queuer:::queue_local_worker_main(args),
                    args = list(c(ctx$root$path, ctx$id, TRUE)))
  on.exit(if (px$is_alive()) px$kill())

  x <- runif(4, max = 0.1)
  id <- ids::sentence()
  res <- obj$lapply(x, "slow_double", timeout = 100, time_poll = 0.02,
                    progress = FALSE, name = id)
  expect_equal(res, as.list(x * 2))

  ## Can also get things this way:
  grp <- obj$task_bundle_get(id)
  expect_is(grp, "task_bundle")
  expect_equal(grp$wait(5), as.list(x * 2))

  pid <- obj$enqueue(Sys.getpid())$wait(Inf, 0.1, FALSE)

  tools::pskill(pid, tools::SIGINT)
  times_up <- time_checker(1)
  while (px$is_alive() && !times_up()) {
    Sys.sleep(0.02)
  }
  expect_false(px$is_alive())
})


test_that("Argument parsing", {
  expect_error(queue_local_worker_main_args(character(0)), "Usage")
  expect_error(queue_local_worker_main_args("a"), "Usage")
  expect_error(queue_local_worker_main_args(c("a", "b", "c", "d")), "Usage")

  expect_equal(
    queue_local_worker_main_args(c("root", "id")),
    list(root = "root", context_id = "id", loop = FALSE))
  expect_equal(
    queue_local_worker_main_args(c("root", "id", "TRUE")),
    list(root = "root", context_id = "id", loop = TRUE))
})
