context("progress")

test_that("progress_timeout", {
  run <- function(total, timeout, every, n = total, ...) {
    p <- progress_timeout(total, timeout, ...,
                          show_after = 0, stream = stdout(),
                          force = TRUE, width = 40)
    expired <- FALSE
    done <- FALSE
    p(0)
    for (i in seq_len(n)) {
      Sys.sleep(every)
      if (p()) {
        expired <- TRUE
        break
      }
    }
    done <- i == n
    list(done = done, expired = expired)
  }

  prepare_expected <- function(x) {
    x <- gsub("\\", "\\\\", x, fixed = TRUE)
    x <- paste0("\\r", c(strsplit(x, "\r")[[1]][-1], ""))
    writeLines(
      sprintf("win_newline(\n%s\n)",
              paste(sprintf('  "%s"', x), collapse = ",\n")))
  }

  msg <- get_output(ans <- run(3, 100.1, 0.1, digits = 1))
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
