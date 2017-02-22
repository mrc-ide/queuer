progress <- function(total, fmt, ..., show = TRUE, prefix = "") {
  if (show) {
    pb <- progress::progress_bar$new(fmt, total = total, ...)
    function(len = 1, ...) {
      invisible(pb$tick(len, ...))
    }
  } else {
    function(...) {}
  }
}

progress_timeout <- function(total, timeout, ..., show = TRUE, prefix = "",
                             fmt = NULL, digits = 0) {
  if (show) {
    forever <- !is.finite(timeout)
    if (is.null(fmt)) {
      if (forever) {
        fmt <- "(:spin) [:bar] :percent | waited for :elapsed"
      } else {
        fmt <- "(:spin) [:bar] :percent | giving up in :remaining s"
      }
    }
    if (nzchar(prefix)) {
      fmt <- paste0(prefix, fmt)
    }
    p <- progress(total, fmt, ...)
    time_left <- time_checker(timeout, TRUE)
    width <- (if (digits > 0) digits + 1 else 0) +
      max(0, floor(log10(timeout))) + 1

    function(len = 1, ..., clear = FALSE) {
      rem <- max(0, time_left())
      move <- if (clear || rem == 0) total else len
      if (forever) {
        p(move)
      } else {
        remaining <- formatC(rem, digits = digits, width = width, format = "f")
        p(move, tokens = list(remaining = remaining))
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

progress_remaining <- function(timeout, what, digits = 0, show = TRUE, ...) {
  if (show && timeout > 0) {
    total <- 1
    forever <- !is.finite(timeout)
    if (forever) {
      fmt <- sprintf("(:spin) waiting for %s, waited for :elapsed", what)
    } else {
      fmt <- sprintf("(:spin) waiting for %s, giving up in :remaining s",
                     what)
    }

    ## digits <- if (time_poll < 1) abs(floor(log10(time_poll))) else 0
    p <- progress(total, fmt, ...)
    time_left <- time_checker(timeout, TRUE)
    width <- (if (digits > 0) digits + 1 else 0) +
      abs(floor(log10(timeout))) + 1

    function(..., clear = FALSE) {
      rem <- max(0, time_left())
      move <- if (clear || rem == 0) 1L else 0L
      if (forever) {
        p(move)
      } else {
        remaining <- formatC(rem, digits = digits, width = width, format = "f")
        p(move, tokens = list(remaining = remaining))
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
