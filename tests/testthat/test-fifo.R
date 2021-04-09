context("fifo")

test_that("fifo_thor", {
  skip_if_not_installed("thor")
  q <- fifo_thor$new(tempfile())
  expect_equal(q$read(), character(0))
  expect_null(q$pop())

  expect_equal(q$push(c("a", "b", "c")), 3)
  expect_equal(q$read(), c("a", "b", "c"))
  expect_equal(q$pop(), "a")
  expect_equal(q$pop(), "b")
  expect_equal(q$pop(), "c")
  expect_null(q$pop())

  expect_equal(q$push(c("a", "b", "c")), 3)
  expect_equal(q$drop(c("b", "d", "f")), c(TRUE, FALSE, FALSE))
  expect_equal(q$read(), c("a", "c"))
})
