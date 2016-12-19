context("task bundle")

## This tests the lifecycle of a pair of tasks in a bundle:
test_that("task_bundle", {
  ctx <- context::context_save(tempfile(), storage_type = "environment")
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
  expect_error(grp$wait(0.1, progress_bar = FALSE), "Exceeded maximum time")
  t1 <- Sys.time()
  expect_lt(as.numeric(t1 - t0, "secs"), 1)

  context::task_run(id1, ctx)
  expect_equal(grp$results(TRUE), list(sin(1), NULL))
  expect_error(grp$results(FALSE), "Tasks not yet completed")

  context::task_run(id2, ctx)
  expect_equal(grp$results(), list(sin(1), sin(2)))
  t0 <- Sys.time()
  expect_equal(grp$wait(10, progress_bar = FALSE), list(sin(1), sin(2)))
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
  expect_true(grp$name %in% task_bundle_list(ctx))
  task_bundle_delete(grp$name, ctx$db)
  expect_false(grp$name %in% task_bundle_list(ctx))
  expect_true(all(context::task_exists(ids, ctx)))
})
