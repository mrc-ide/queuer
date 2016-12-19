context("fifo")

test_that("seagull", {
  q <- fifo_seagull(storr::storr_environment(), "queue", "objects",
                    tempfile(), 1)
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
