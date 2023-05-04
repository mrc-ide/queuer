context("bulk")

test_that("submit", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_base$new(ctx)

  x <- 1:30
  fun <- quote(sin)
  res <- obj$lapply(x, fun)

  expect_equal(res$X, x)
  expect_equal(length(res$ids), length(x))
  expect_null(res$names)
  expect_is(res$name, "character")

  expect_is(res$tasks, "list")
  expect_is(res$tasks[[1]], "queuer_task")
  expect_equal(length(res$tasks), length(x))

  ## Not amazing, but it'll do:
  expect_equal(res$tasks[[14]]$expr(), quote(base::sin(14L)))

  expect_error(res$results(), "Tasks not yet completed")
  expect_equal(res$results(TRUE), rep(list(NULL), length(x)))

  expect_equal(res$status(), set_names(rep("PENDING", length(x)), res$ids))
  expect_equal(res$status(FALSE), rep("PENDING", length(x)))
})

test_that("named", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  obj <- queue_base$new(ctx)

  x <- set_names(runif(10), ids::adjective_animal(10))
  fun <- quote(sin)

  res <- obj$lapply(x, fun)
  expect_equal(res$names, names(x))

  expect_equal(res$results(TRUE),
               set_names(rep(list(NULL), length(x)), names(x)))

  tmp <- lapply(res$ids, context::task_run, ctx)
  expect_equal(res$results(TRUE), as.list(sin(x)))
})

test_that("named group", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_base$new(ctx)
  nm <- ids::sentence()
  res <- obj$lapply(1:30, quote(sin), name = nm)
  expect_equal(res$name, nm)
})

test_that("named lapply", {
  skip_if_not_using_local_queue()
  ctx <- context::context_save(tempfile())
  obj <- queue_local$new(ctx)
  bundle <- obj$lapply(set_names(as.list(1:3), letters[1:3]), I)

  expect_equal(bundle$tasks[[1]]$expr(),
               quote(base::I(1L)))
  res <- obj$run_next()
  expect_equal(res$value, I(1L))
  expect_equal(bundle$names, letters[1:3])

  obj$run_all()

  ## Check names are returned:
  expect_equal(bundle$results(), set_names(lapply(bundle$X, I), letters[1:3]))
})

test_that("$enqueue_bulk", {
  skip_if_not_using_local_queue()
  ctx <- context::context_save(tempfile())
  obj <- queue_local$new(ctx)
  bundle <- obj$enqueue_bulk(1:3, quote(I))
  expect_is(bundle, "task_bundle")
  expect_equal(bundle$function_name(), "base::I")
})

test_that("enqueue_bulk with dependencies", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  obj <- queue_base$new(ctx)
  t <- obj$enqueue(sin(1))

  expect_error(obj$enqueue_bulk(1:3, quote(I), depends_on = "123"),
               "Failed to save as dependency 123 does not exist")

  t2 <- obj$enqueue_bulk(1:3, quote(I), depends_on = t$id)
  t3 <- obj$enqueue_bulk(1:3, quote(I), depends_on = c(t$id, t2$id))

  expect_equal(context::task_deps(t2$ids, ctx), rep(list(t$id), 3))
  expect_equal(context::task_deps(t3$ids, ctx), rep(list(c(t$id, t2$id)), 3))
})

test_that("exotic functions", {
  skip_if_not_using_local_queue()
  Sys.setenv(R_TESTS = "")

  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_local$new(ctx)

  x <- 1:5
  res <- local({
    f_local <- function(x) {
      x + 2
    }
    obj$lapply(x, quote(f_local), progress = FALSE, timeout = 0)
  })

  expect_equal(length(obj$task_list()), length(x))
  done <- obj$run_all()
  expect_equal(done, res$ids)
  expect_equal(res$results(), as.list(x + 2))

  res <- local({
    obj$lapply(x, function(x) x + 3, progress = FALSE, timeout = 0)
  })
  expect_equal(obj$run_all(), res$ids)
  expect_equal(res$results(), as.list(x + 3))
})

test_that("sanity checking", {
  expect_error(qlapply(1:4, sin, NULL), "must be a queue object")
  expect_error(enqueue_bulk(NULL, 1:4, sin), "must be a queue object")
})

test_that("mapply", {
  skip_if_not_using_local_queue()
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_local$new(ctx)

  grp <- obj$mapply(rep, 1:4, 4:1)
  expect_equal(length(grp$tasks), 4)
  expect_equal(grp$tasks[[1]]$expr(), quote(base::rep(1L, 4L)))
  expect_equal(grp$tasks[[4]]$expr(), quote(base::rep(4L, 1L)))

  expect_equal(obj$run_all(), grp$ids)
  expect_equal(grp$results(), mapply(rep, 1:4, 4:1, SIMPLIFY = FALSE))

  grp <- obj$mapply(rep, times = 1:4, x = 4:1)
  expect_equal(length(grp$tasks), 4)
  expect_equal(grp$tasks[[1]]$expr(), quote(base::rep(times = 1L, x = 4L)))
  expect_equal(grp$tasks[[4]]$expr(), quote(base::rep(times = 4L, x = 1L)))
  expect_equal(obj$run_all(), grp$ids)
  expect_equal(grp$results(),
               mapply(rep, times = 1:4, x = 4:1, SIMPLIFY = FALSE))

  grp <- obj$mapply(rep, times = 1:4, MoreArgs = list(x = 42))
  expect_equal(length(grp$tasks), 4)
  expect_equal(grp$tasks[[1]]$expr(), quote(base::rep(times = 1L, x = 42)))
  expect_equal(grp$tasks[[4]]$expr(), quote(base::rep(times = 4L, x = 42)))
  expect_equal(obj$run_all(), grp$ids)
  expect_equal(grp$results(),
               mapply(rep, times = 1:4, MoreArgs = list(x = 42),
                      SIMPLIFY = FALSE))
})

test_that("mapply - recycle", {
  skip_if_not_using_local_queue()
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_local$new(ctx)
  grp <- obj$mapply(rep, 1:4, 1)

  expect_equal(length(grp$tasks), 4)
  expect_equal(grp$tasks[[1]]$expr(), quote(base::rep(1L, 1)))
  expect_equal(grp$tasks[[4]]$expr(), quote(base::rep(4L, 1)))
  expect_equal(obj$run_all(), grp$ids)
  expect_equal(grp$results(), mapply(rep, 1:4, 1, SIMPLIFY = FALSE))

  expect_error(obj$mapply(rep, 1:4, 1:3),
               "Every element of '...' must have the same length (or 1)",
               fixed = TRUE)
})
