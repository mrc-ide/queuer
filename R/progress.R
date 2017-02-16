progress <- function(total, ..., show = TRUE, prefix = "", fmt = NULL) {
  if (show) {
    if (is.null(fmt)) {
      fmt <- paste0(prefix, "(:spin) [:bar] :percent")
    }
    pb <- progress::progress_bar$new(fmt, total = total, ...)
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

progress_timeout <- function(total, timeout, ..., show = TRUE, prefix = "",
                             fmt = NULL, digits = 0) {
  if (show) {
    if (is.null(fmt)) {
      if (is.finite(timeout)) {
        fmt <- "(:spin) [:bar] :percent | giving up in :remaining s"
      } else {
        fmt <- "(:spin) [:bar] :percent | waited for :elapsed"
      }
    }
    if (nzchar(prefix)) {
      fmt <- paste0(prefix, fmt)
    }
    p <- progress(total, fmt = fmt, ...)
    time_left <- time_checker(timeout, TRUE)
    width <- (if (digits > 0) digits + 1 else 0) +
      abs(floor(log10(timeout))) + 1

    function(len = 1, ..., clear = FALSE) {
      rem <- if (clear) 0 else time_left()
      if (rem <= 0) {
        p(len, clear = TRUE, tokens = list(remaining = "0s"))
      } else {
        remaining <- formatC(rem, digits = digits, width = width, format = "f")
        p(len, tokens = list(remaining = remaining))
      }
      rem <= 0
    }
  } else {
    times_up <- time_checker(timeout, FALSE)
    function(..., clear = FALSE) {
      if (clear) 0 else times_up()
    }
  }
}

remaining <- function(timeout, what, digits = 0, show = TRUE, ...) {
  if (show && timeout > 0) {
    total <- 1e8 # arbitrarily large number :(
    if (is.finite(timeout)) {
      fmt <- sprintf("(:spin) waiting for %s, giving up in :remaining s",
                     what)
    } else {
      fmt <- sprintf("(:spin) waiting for %s, waited for :elapsed", what)
    }

    ## digits <- if (every < 1) abs(floor(log10(every))) else 0
    p <- progress(total, fmt = fmt, ...)
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
