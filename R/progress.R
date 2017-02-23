##' Progress bar with timeout
##'
##' @title Progress bar with timeout
##' @param total Total number of expected things.  Use \code{NULL} if
##'   you want to wait on a single thing
##' @param timeout The number of seconds to wait
##' @param ... Additional arguments to
##'   \code{progress::progress_bar$new}
##' @param show Flag to indicate if the bar should be displayed.  If
##'   \code{NULL} then the global options \code{queuer.progress_show}
##'   and \code{queuer.progress_suppress} are used to determine if the
##'   bar is shown (with suppress overriding show).  This will
##'   evaluate to \code{TRUE} by default.  A logical flag overrides
##'   \code{queuer.progress_show} but not
##'   \code{queuer.progress_suppress}.
##' @param label An optional label to prefix the timeout bar with
##'   (will not be padded with space), or in the case of \code{total =
##'   NULL} to indicate what is being waited on.
##' @param digits The number of digits of accuracy to display the
##'   remaining time in
##' @export
progress_timeout <- function(total, timeout, ..., show = NULL, label = NULL,
                             digits = 0) {
  show <- show_progress(show)
  if (show) {
    single <- is.null(total)
    if (single) {
      total <- 1L
    }
    forever <- !is.finite(timeout)
    width <- (if (digits > 0) digits + 1 else 0) +
      max(0, floor(log10(timeout))) + 1

    if (single) {
      if (is.null(label)) {
        label <- "task"
      }
      if (forever) {
        fmt <- sprintf("(:spin) waiting for %s, waited for :elapsed", label)
      } else {
        fmt <- sprintf("(:spin) waiting for %s, giving up in :remaining s",
                       label)
      }
    } else {
      if (forever) {
        fmt <- "(:spin) [:bar] :percent | waited for :elapsed"
      } else {
        fmt <- "(:spin) [:bar] :percent | giving up in :remaining s"
      }
      if (!is.null(label)) {
        fmt <- paste0(label, fmt)
      }
    }

    p <- progress::progress_bar$new(fmt, total = total, ...)$tick
    time_left <- time_checker(timeout, TRUE)

    function(len = 1, ..., clear = FALSE) {
      rem <- max(0, time_left())
      move <- if (clear || rem == 0) total else if (single) 0L else len
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

##' A simple timer
##' @title Simple timer
##' @param timeout The length of time to set the timer for (can be
##'   infinite)
##' @param remaining Logical, indicating if the timer should return
##'   the time remaining (as the number of seconds).  If \code{FALSE}
##'   (the default) then the timer returns \code{FALSE} until the time
##'   has completed, then returns \code{TRUE}.
##' @export
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

show_progress <- function(show) {
  !getOption("queuer.progress_suppress", FALSE) &&
    (show %||% getOption("queuer.progress_show", TRUE))
}
