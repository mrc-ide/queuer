queuer_task <- function(id, root, check_exists = TRUE) {
  R6_queuer_task$new(id, root, check_exists)
}

R6_queuer_task <- R6::R6Class(
  "queuer_task",
  public = list(
    root = NULL,
    id = NULL,

    initialize = function(id, root, check_exists = TRUE) {
      self$root <- context::context_root_get(root)
      self$id <- id
      if (check_exists && !context::task_exists(id, self$root$db)) {
        stop("Task does not exist: ", id, call. = FALSE)
      }
    },

    status = function() {
      context::task_status(self$id, self$root$db)
    },

    result = function(sanitise = FALSE) {
      context::task_result(self$id, self$root$db, sanitise)
    },

    expr = function(locals = FALSE) {
      context::task_expr(self$id, self$root$db, locals)
    },

    context_id = function() {
      context::task_read(self$id, self$root$db)$context_id
    },

    times = function(unit_elapsed = "secs") {
      context::task_times(self$id, self$root$db, unit_elapsed)
    },

    log = function() {
      context::task_log(self$id, self$root)
    },

    wait = function(timeout, every = 0.5, progress = TRUE) {
      task_wait(self$root$db, self$id, timeout, every, progress)
    }
  ))

## TODO: we want to override this (probably) where a Redis based
## interface is available, because then we can do an optimal wait.
## See rrq for the approach there.
task_wait <- function(db, task_id, timeout, every = 0.5, progress = TRUE) {
  t <- time_checker(timeout, TRUE)
  every <- min(every, timeout)

  ## TODO: This does not always do a great job of reprinting
  if (is.finite(timeout)) {
    fmt <- sprintf("(:spin) waiting for %s, giving up in :remaining s",
                   trim_id(task_id, 7, 3))
    total <- ceiling(timeout / every) + 1
  } else {
    fmt <- sprintf("(:spin) waiting for %s, waited for :elapsed",
                   trim_id(task_id, 7, 3))
    total <- 1e8 # arbitrarily large number :(
  }

  digits <- if (every < 1) abs(floor(log10(every))) else 0
  p <- progress(total, show = progress && timeout > 0, fmt = fmt)
  repeat {
    res <- context::task_result(task_id, db, sanitise = TRUE)
    if (!inherits(res, "UnfetchableTask")) {
      p(clear = TRUE, tokens = list(remaining = "0s"))
      return(res)
    } else {
      rem <- t()
      if (rem < 0) {
        p(clear = TRUE, tokens = list(remaining = "0s"))
        stop("task not returned in time")
      } else {
        remaining <- formatC(rem, digits = digits, format = "f")
        p(tokens = list(remaining = remaining))
        Sys.sleep(every)
      }
    }
  }
}
