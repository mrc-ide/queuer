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

    result = function(allow_incomplete = FALSE) {
      context::task_result(self$id, self$root$db, allow_incomplete)
    },

    expr = function(locals = FALSE) {
      context::task_expr(self$id, self$root$db, locals)
    },

    context_id = function() {
      context::task_context_id(self$id, self$root$db)
    },

    times = function(unit_elapsed = "secs") {
      context::task_times(self$id, self$root$db, unit_elapsed)
    },

    log = function(parse = TRUE) {
      context::task_log(self$id, self$root, parse)
    },

    wait = function(timeout, time_poll = 0.5, progress = TRUE) {
      task_wait(self$root$db, self$id, timeout, time_poll, progress)
    }
  ))

## TODO: we want to override this (probably) where a Redis based
## interface is available, because then we can do an optimal wait.
## See rrq for the approach there.
task_wait <- function(db, task_id, timeout, time_poll = 0.5, progress = TRUE) {
  time_poll <- min(time_poll, timeout)
  digits <- if (time_poll < 1) abs(floor(log10(time_poll))) else 0
  p <- remaining(timeout, trim_id(task_id, 7, 3), digits, progress)
  repeat {
    res <- context::task_result(task_id, db, allow_incomplete = TRUE)
    if (!inherits(res, "UnfetchableTask")) {
      p(clear = TRUE)
      return(res)
    } else {
      if (p()) {
        stop("task not returned in time")
      } else {
        Sys.sleep(time_poll)
      }
    }
  }
}
