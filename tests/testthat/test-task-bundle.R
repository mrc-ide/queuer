context("task bundle")

test_that("task_bundle_combine -- none", {
  expect_error(task_bundle_combine(), "Provide at least one task bundle")
  expect_error(task_bundle_combine(bundles=list()),
               "Provide at least one task bundle")
})

test_that("task_bundle_combine -- single", {
  ctx <- context::context_save(root=tempfile(), storage_type="environment")
  obj <- queue_local(ctx)

  x <- qlapply(1:5, list, obj)
  xx <- task_bundle_combine(x)

  expect_identical(x$ids, xx$ids)
  expect_identical(x$X, xx$X)
  expect_identical(x$names, xx$names)
})

test_that("task_bundle_combine -- single, named", {
  ctx <- context::context_save(root=tempfile(), storage_type="environment")
  obj <- queue_local(ctx)

  els <- setNames(1:5, letters[1:5])
  x <- qlapply(els, list, obj)
  xx <- task_bundle_combine(x)

  expect_is(xx$names, "character")
  expect_identical(x$ids, xx$ids)
  expect_identical(x$X, xx$X)
  expect_identical(x$names, xx$names)
})

test_that("task_bundle_combine, two", {
  ctx <- context::context_save(root=tempfile(), storage_type="environment")
  obj <- queue_local(ctx)

  x <- qlapply(1:5, list, obj)
  y <- qlapply(6:10, list, obj)

  z <- task_bundle_combine(x, y)

  expect_equal(z$ids, c(x$ids, y$ids))
  expect_equal(z$X, c(x$X, y$X))
})

test_that("task_bundle_combine, two, named", {
  ctx <- context::context_save(root=tempfile(), storage_type="environment")
  obj <- queue_local(ctx)

  X1 <- setNames(1:5, letters[1:5])
  X2 <- setNames(6:10, letters[6:10])
  x <- qlapply(X1, list, obj)
  y <- qlapply(X2, list, obj)

  z <- task_bundle_combine(x, y)

  expect_equal(z$ids, c(x$ids, y$ids))
  expect_equal(z$X, c(x$X, y$X))
})

test_that("task bundle, dfs", {
  ctx <- context::context_save(root=tempfile(), storage_type="environment")
  obj <- queue_local(ctx)
  X1 <- data.frame(a=1:2, b=runif(2))
  X2 <- data.frame(a=3:6, b=runif(4))

  x <- enqueue_bulk(obj, X1, list)
  y <- enqueue_bulk(obj, X2, list)

  z <- task_bundle_combine(x, y)

  expect_equal(z$ids, c(x$ids, y$ids))
  expect_equal(z$X, rbind(x$X, y$X))
})

test_that("task_bundle_combine, incompatible functions", {
  ctx <- context::context_save(root=tempfile(), storage_type="environment")
  obj <- queue_local(ctx)

  x <- qlapply(1:5, list, obj)
  y <- qlapply(6:10, head, obj)

  expect_error(task_bundle_combine(x, y), "must have same function")
})
