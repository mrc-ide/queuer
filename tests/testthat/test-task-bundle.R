context("task bundle")

## This tests the lifecycle of a pair of tasks in a bundle:
test_that("task_bundle", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  on.exit(unlink(ctx$root$path, recursive = TRUE))

  id1 <- context::task_save(quote(sin(1)), ctx)
  id2 <- context::task_save(quote(sin(2)), ctx)
  ids <- c(id1, id2)

  grp <- task_bundle_create(ids, ctx)

  expect_is(grp, "task_bundle")

  expect_equal(grp$done, setNames(c(FALSE, FALSE), ids))
  expect_true(grp$homogeneous)

  expect_equal(grp$expr(),
               setNames(list(quote(sin(1)), quote(sin(2))), ids))
  expect_equal(grp$function_name(), "sin")

  tt <- grp$times()
  expect_equal(tt[-5], context::task_times(ids, ctx)[-5])

  expect_error(grp$results(FALSE), "Tasks not yet completed")
  expect_equal(grp$results(TRUE), list(NULL, NULL))

  expect_error(grp$wait(0), "Tasks not yet completed")

  t0 <- Sys.time()
  expect_error(grp$wait(0.1, progress = FALSE), "Exceeded maximum time")
  t1 <- Sys.time()
  expect_lt(as.numeric(t1 - t0, "secs"), 1)

  context::task_run(id1, ctx)
  expect_equal(grp$results(TRUE), list(sin(1), NULL))
  expect_error(grp$results(FALSE), "Tasks not yet completed")

  context::task_run(id2, ctx)
  expect_equal(grp$results(), list(sin(1), sin(2)))
  t0 <- Sys.time()
  expect_equal(grp$wait(10, progress = FALSE), list(sin(1), sin(2)))
  t1 <- Sys.time()
  expect_lt(as.numeric(t1 - t0, "secs"), 1)
})

test_that("bundle name", {
  db <- storr::storr_environment()

  ## Avoid collisions:
  set.seed(1)
  nm <- create_bundle_name(NULL, TRUE, db)
  db$set(nm, NULL, "task_bundles")
  set.seed(1)
  expect_equal(ids::adjective_animal(), nm)
  set.seed(1)
  expect_true(create_bundle_name(NULL, TRUE, db) != nm)

  ## Don't overwrite:
  expect_error(create_bundle_name(nm, FALSE, db),
               "Task bundle already exists")

  nm <- ids::adjective_animal(n_adjectives = 2)
  expect_equal(create_bundle_name(nm, TRUE, db), nm)
  ## No database access in this situation:
  expect_silent(create_bundle_name(nm, TRUE, NULL))
})

test_that("empty bundle", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  obj <- queue_local(ctx)
  expect_error(task_bundle_create(character(0), obj),
               "task_ids must be nonempty")
})

test_that("nonhomogeneous bundle", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  on.exit(unlink(ctx$root$path, recursive = TRUE))

  id1 <- context::task_save(quote(sin(1)), ctx)
  id2 <- context::task_save(quote(cos(2)), ctx)
  ids <- c(id1, id2)

  grp <- task_bundle_create(ids, ctx)
  expect_false(grp$homogeneous)
  expect_equal(grp$function_name(), NA_character_)

  expect_equal(grp$expr(),
               setNames(list(quote(sin(1)), quote(cos(2))), ids))
})

test_that("delete", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  on.exit(unlink(ctx$root$path, recursive = TRUE))

  id1 <- context::task_save(quote(sin(1)), ctx)
  id2 <- context::task_save(quote(cos(2)), ctx)
  ids <- c(id1, id2)

  grp <- task_bundle_create(ids, ctx)
  expect_true(grp$name %in% task_bundle_list(ctx$db))
  task_bundle_delete(grp$name, ctx$db)
  expect_false(grp$name %in% task_bundle_list(ctx$db))
  expect_true(all(context::task_exists(ids, ctx)))
})

test_that("info", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  on.exit(unlink(ctx$root$path, recursive = TRUE))

  obj <- queue_local(ctx)
  grp1 <- obj$lapply(runif(4), quote(sin))
  grp2 <- obj$lapply(runif(10), quote(cos))

  expect_equal(sort(obj$task_bundle_list()),
               sort(c(grp1$name, grp2$name)))

  dat <- obj$task_bundle_info()
  expect_equal(dat$name, c(grp1$name, grp2$name))
  expect_equal(dat[["function"]], c("base::sin", "base::cos"))
  expect_equal(dat$length, c(4L, 10L))
  expect_is(dat$created, "POSIXt")
  expect_gte(as.numeric(diff(dat$created)), 0)
})

test_that("create data.frame group", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  on.exit(unlink(ctx$root$path, recursive = TRUE))
  obj <- queue_local(ctx)
  df <- data.frame(a = 1:5, x = runif(5))
  grp <- obj$enqueue_bulk(df, list)
  expect_null(grp$names)
  rownames(df) <- letters[seq_len(nrow(df))]
  grp <- obj$enqueue_bulk(df, list)
  expect_equal(grp$names, letters[seq_len(nrow(df))])
})

test_that("combine", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
  on.exit(unlink(ctx$root$path, recursive = TRUE))

  obj <- queue_local(ctx)
  grp1_sin <- obj$lapply(runif(4), quote(sin))
  grp2_sin <- obj$lapply(runif(10), quote(sin))
  grp3_sin <- obj$lapply(setNames(runif(5), letters[1:5]), quote(sin))
  grp4_sin <- obj$lapply(setNames(runif(4), letters[6:9]), quote(sin))
  grp_cos <- obj$lapply(runif(10), quote(cos))
  grp1_df <- obj$enqueue_bulk(data.frame(x = runif(10)), quote(sin),
                              use_names = FALSE)
  grp2_df <- obj$enqueue_bulk(data.frame(x = runif(4)), quote(sin),
                              use_names = FALSE)
  grp3_df <- obj$enqueue_bulk(data.frame(y = runif(4)), quote(sin),
                              use_names = FALSE)
  grp4_df <- obj$enqueue_bulk(data.frame(a = 1:3, y = runif(3)), quote(sin),
                              use_names = FALSE)

  res <- task_bundle_combine(grp1_sin, grp2_sin)
  expect_is(res, "task_bundle")
  expect_equal(res$ids, c(grp1_sin$ids, grp2_sin$ids))
  expect_equal(res$X, c(grp1_sin$X, grp2_sin$X))
  expect_null(res$names)

  res <- task_bundle_combine(grp3_sin, grp4_sin)
  expect_is(res, "task_bundle")
  expect_equal(res$ids, c(grp3_sin$ids, grp4_sin$ids))
  expect_equal(res$names, c(grp3_sin$names, grp4_sin$names))
  expect_equal(res$X, c(grp3_sin$X, grp4_sin$X))

  res <- task_bundle_combine(grp1_sin, grp4_sin)
  expect_is(res, "task_bundle")
  expect_equal(res$ids, c(grp1_sin$ids, grp4_sin$ids))
  expect_equal(res$names, c(rep("", length(grp1_sin$ids)), grp4_sin$names))
  expect_equal(res$X, c(grp1_sin$X, grp4_sin$X))

  res <- task_bundle_combine(grp1_df, grp2_df)
  expect_is(res, "task_bundle")
  expect_equal(res$ids, c(grp1_df$ids, grp2_df$ids))
  expect_null(res$names)
  expect_equal(res$X, rbind(grp1_df$X, grp2_df$X))

  expect_error(task_bundle_combine(),
               "Provide at least two task bundles")
  expect_error(task_bundle_combine(bundles = list()),
               "Provide at least two task bundles")
  expect_error(task_bundle_combine(grp1_sin),
               "Provide at least two task bundles")
  expect_error(task_bundle_combine(bundles = list(grp1_sin)),
               "Provide at least two task bundles")
  expect_error(task_bundle_combine(grp1_sin, grp_cos),
               "task bundles must have same function to combine")
  expect_error(task_bundle_combine("a", "b"),
               "All elements of ... or bundles must be task_bundle objects")
  expect_error(task_bundle_combine(grp1_df, grp3_df),
               "All bundle data.frames must have the same column names")
  expect_error(task_bundle_combine(grp1_df, grp4_df),
               "All bundle data.frames must have the same column names")
  ## Yeah, so this is not a great error message :-/
  expect_error(task_bundle_combine(grp1_sin, grp1_df),
               "Can't combine these task bundles")

})
