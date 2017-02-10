## Base imports:
##' @importFrom stats setNames
##' @importFrom utils packageVersion
time_checker <- function(timeout, remaining = FALSE) {
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units = "secs")
  if (is.finite(timeout)) {
    if (remaining) {
      function() {
        as.double(timeout - (Sys.time() - t0), "secs")
      }
    } else {
      function() {
        Sys.time() - t0 > timeout
      }
    }
  } else {
    if (remaining) {
      function() Inf
    } else {
      function() FALSE
    }
  }
}

progress <- function(total, ..., show = TRUE, prefix = "", fmt = NULL) {
  if (show) {
    if (is.null(fmt)) {
      fmt <- paste0(prefix, "(:spin) [:bar] :percent")
    }
    pb <- progress::progress_bar$new(fmt, total = total)
    pb_private <- environment(pb$tick)$private
    function(len = 1, ..., clear = FALSE) {
      if (clear) {
        len <- pb_private$total - pb_private$current
      }
      invisible(pb$tick(len, ...))
    }
  } else {
    function(...) {}
  }
}

remaining <- function(timeout, what, digits = 0, show = TRUE) {
  if (show && timeout > 0) {
    total <- 1e8 # arbitrarily large number :(
    if (is.finite(timeout)) {
      fmt <- sprintf("(:spin) waiting for %s, giving up in :remaining s",
                     what)
    } else {
      fmt <- sprintf("(:spin) waiting for %s, waited for :elapsed", what)
    }

    ## digits <- if (every < 1) abs(floor(log10(every))) else 0
    p <- progress(total, fmt = fmt)
    t <- time_checker(timeout, TRUE)
    function(..., clear = FALSE) {
      rem <- if (clear) 0 else t()
      if (rem <= 0) {
        p(clear = TRUE, tokens = list(remaining = "0s"))
      } else {
        remaining <- formatC(rem, digits = digits, format = "f")
        p(tokens = list(remaining = remaining))
      }
      rem <= 0
    }
  } else {
    t <- time_checker(timeout, FALSE)
    function(..., clear = FALSE) {
      if (clear) 0 else t()
    }
  }
}

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
