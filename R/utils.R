## Base imports:
##' @importFrom stats setNames
NULL

## Short-circuit apply; returns the index of the first element of x
## for which cond(x[[i]]) holds true.
scapply <- function(x, cond, no_match = NA_integer_) {
  for (i in seq_along(x)) {
    if (isTRUE(cond(x[[i]]))) {
      return(i)
    }
  }
  no_match
}

trim_id <- function(x, head = 7, tail = 0) {
  n <- nchar(x)
  i <- (head + tail) < (n - 3)
  if (any(i)) {
    x[i] <- sprintf("%s...%s",
                    substr(x[i], 1, head),
                    substr(x[i], n - tail + 1, n))
  }
  x
}

vlapply <- function(X, FUN, ...) {
  vapply(X, FUN, logical(1), ...)
}
vcapply <- function(X, FUN, ...) {
  vapply(X, FUN, character(1), ...)
}

capture_log <- function(expr, filename, suppress_messages = FALSE) {
  con <- file(filename, "w")
  sink(con, split = FALSE)
  on.exit({
    sink(NULL)
    close(con)
  })
  handle_message <- function(e) cat(e$message, file = stdout())
  if (suppress_messages) {
    suppressMessages(withCallingHandlers(expr, message = handle_message))
  } else {
    withCallingHandlers(expr, message = handle_message)
  }
}

`%||%` <- function(a, b) {
  if (is.null(a)) b else a
}
