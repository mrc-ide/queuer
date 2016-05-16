context("utils")

test_that("capture_log", {
  f <- function(n) {
    ret <- ifelse(runif(n) < 0.5, "stdout", "stderr")
    for (i in seq_len(n)) {
      if (ret[[i]] == "stdout") {
        cat(i, ":stdout\n", sep="")
      } else {
        message(i, ":stderr")
      }
    }
    ret
  }

  dest <- tempfile()
  on.exit(file.remove(dest))

  set.seed(1)
  expect_message(res <- capture_log(f(10), dest), "3:stderr")
  expect_equal(readLines(dest),
               paste(seq_along(res), res, sep=":"))

  expect_silent(res2 <- capture_log(f(10), dest, TRUE))
  expect_equal(readLines(dest),
               paste(seq_along(res2), res2, sep=":"))
})
