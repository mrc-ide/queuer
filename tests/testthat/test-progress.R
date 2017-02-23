context("progress")

test_that("options", {
  with_options <- function(opts, code) {
    oo <- options(opts)
    on.exit(options(oo))
    force(code)
  }

  with_options(
    list(queuer.progress_show = TRUE,
         queuer.progress_suppress = FALSE), {
           expect_true(show_progress(NULL))
           expect_true(show_progress(TRUE))
           expect_false(show_progress(FALSE))
         })

  with_options(
    list(queuer.progress_show = FALSE,
         queuer.progress_suppress = FALSE), {
           expect_false(show_progress(NULL))
           expect_true(show_progress(TRUE))
           expect_false(show_progress(FALSE))
         })

  with_options(
    list(queuer.progress_show = TRUE,
         queuer.progress_suppress = TRUE), {
           expect_false(show_progress(NULL))
           expect_false(show_progress(TRUE))
           expect_false(show_progress(FALSE))
         })

  with_options(
    list(queuer.progress_show = FALSE,
         queuer.progress_suppress = TRUE), {
           expect_false(show_progress(NULL))
           expect_false(show_progress(TRUE))
           expect_false(show_progress(FALSE))
         })

  with_options(
    list(queuer.progress_show = NULL,
         queuer.progress_suppress = NULL), {
           expect_true(show_progress(NULL))
           expect_true(show_progress(TRUE))
           expect_false(show_progress(FALSE))
         })
})

test_that("progress_timeout", {
  msg <- get_output(ans <- run_progress_timeout(3, 100.1, 0.1, digits = 1))
  expected <- win_newline(
    "\r(-) [------]   0% | giving up in 100.1 s",
    "\r(\\) [==----]  33% | giving up in 100.0 s",
    "\r(|) [====--]  67% | giving up in  99.9 s",
    "\r(/) [======] 100% | giving up in  99.8 s",
    "\r                                        ",
    "\r"
  )
  expect_equal(ans, list(done = TRUE, expired = FALSE))
  expect_equal(msg, expected)
})

test_that("progress_timeout; label", {
  msg <- get_output(ans <- run_progress_timeout(3, 100.1, 0.1, digits = 1,
                                                label = "foo: ", width = 44))
  expected <- win_newline(
    "\rfoo: (-) [-----]   0% | giving up in 100.1 s",
    "\rfoo: (\\) [==---]  33% | giving up in 100.0 s",
    "\rfoo: (|) [===--]  67% | giving up in  99.9 s",
    "\rfoo: (/) [=====] 100% | giving up in  99.8 s",
    "\r                                            ",
    "\r"
  )
  expect_equal(ans, list(done = TRUE, expired = FALSE))
  expect_equal(msg, expected)
})

test_that("progress_timeout; infinite time", {
  msg <- get_output(ans <- run_progress_timeout(3, Inf, 0.2))
  expected <- win_newline(
    "\r(-) [------------]   0% | waited for  0s",
    "\r(\\) [====--------]  33% | waited for  0s",
    "\r(|) [========----]  67% | waited for  0s",
    "\r(/) [============] 100% | waited for  1s",
    "\r                                        ",
    "\r"
  )
  expect_equal(ans, list(done = TRUE, expired = FALSE))
  expect_equal(msg, expected)
})

test_that("progress_timeout; expires", {
  msg <- get_output(ans <- run_progress_timeout(5, 0.3, 0.1, digits = 1))
  expected <- win_newline(
    "\r(-) [--------]   0% | giving up in 0.3 s",
    "\r(\\) [==------]  20% | giving up in 0.2 s",
    "\r(|) [===-----]  40% | giving up in 0.1 s",
    "\r(/) [========] 100% | giving up in 0.0 s",
    "\r                                        ",
    "\r"
  )
  expect_equal(ans, list(done = FALSE, expired = TRUE))
  expect_equal(msg, expected)
})

test_that("remaining", {
  ## NOTE: There is some extra output here when clearing; the last set
  ## of lines all come together from the final p(clear = TRUE) call
  ## and seem to come from progress itself.  A better option would
  ## probably be to just do a newline and deal with leaving the
  ## progress bar up on the screen.
  msg <- get_output(ans <- run_progress_timeout_single(100.1, 0.1, 3,
                                                       digits = 1))
  expected <- win_newline(
    "\r(-) waiting for task, giving up in 100.1 s",
    "\r(\\) waiting for task, giving up in 100.0 s",
    "\r(|) waiting for task, giving up in  99.9 s",
    "\r(/) waiting for task, giving up in  99.8 s",
    "\r(-) waiting for task, giving up in  99.8 s",
    "\r                                        ",
    "\r"
  )
  expect_equal(ans, list(done = TRUE, expired = FALSE))
  expect_equal(msg, expected)
})

test_that("remaining; infinite", {
  ## NOTE: There is some extra output here when clearing; the last set
  ## of lines all come together from the final p(clear = TRUE) call
  ## and seem to come from progress itself.  A better option would
  ## probably be to just do a newline and deal with leaving the
  ## progress bar up on the screen.
  msg <- get_output(ans <- run_progress_timeout_single(Inf, 0.2, 3))
  expected <- win_newline(
    "\r(-) waiting for task, waited for  0s",
    "\r(\\) waiting for task, waited for  0s",
    "\r(|) waiting for task, waited for  0s",
    "\r(/) waiting for task, waited for  1s",
    "\r(-) waiting for task, waited for  1s",
    "\r                                        ",
    "\r"
  )
  expect_equal(ans, list(done = TRUE, expired = FALSE))
  expect_equal(msg, expected)
})
