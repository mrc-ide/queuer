## TODO: There is a little too much overlap here with context itself.
## It might be worth moving context over to R6 and implementing this
## there?  Then task_handle / task goes away.  But task_handle there
## does deal with _vectors_ of tasks (not sure where that is used
## though).
##
##    UPDATE (2016-01-20): The more I think about this the more I like
##    the idea; it seems that this is going to be needed to be dealt
##    with quite soon.  Same with the context handle itself I suspect.
##    For now continue as-is, though it will be a breaking change when
##    it's done.
task <- function(obj, id) {
  .R6_task$new(obj, id, TRUE)
}

.R6_task <-
  R6::R6Class(
    "task",
    public=
      list(
        id=NULL,
        handle=NULL,
        db=NULL,
        ## root=NULL,

        initialize=function(obj, id, check_exists=TRUE) {
          self$id <- id
          self$handle <- context::task_handle(obj, id, check_exists)
          self$db <- context::context_db(self$handle)
          ## self$root <- context::context_root(self$handle)
        },
        status=function() {
          context::task_status(self$handle)
        },
        result=function(sanitise=FALSE) {
          context::task_result(self$handle, sanitise)
        },
        expr=function(locals=FALSE) {
          context::task_expr(self$handle, locals)
        },
        context_id=function() {
          context::task_read(self$handle)$context_id
        },
        ## TODO: add context_handle() perhaps?
        ## NOTE: no run() method; should that be added?
        times=function(unit_elapsed="secs") {
          context::tasks_times(self$handle, unit_elapsed)
        },
        log=function() {
          context::task_log(self$handle, self$id)
        },
        wait=function(timeout, every=0.5, progress=TRUE) {
          task_wait(self$handle, self$id, timeout, every, progress)
        }))

make_task_handle <- function(obj, task_ids, check_exists=TRUE) {
  if (is.null(task_ids)) {
    task_ids <- context::tasks_list(obj)
  }
  context::task_handle(obj, task_ids, check_exists)
}

task_wait <- function(handle, task_id, timeout, every=0.5, progress=TRUE) {
  t <- time_checker(timeout, TRUE)
  every <- min(every, timeout)

  if (is.finite(timeout)) {
    fmt <- sprintf("waiting for %s, giving up in :remaining s",
                   trim_id(task_id, 7, 3))
    total <- 1e8 # arbitrarily large number :(
  } else {
    fmt <- sprintf("waiting for %s, waited for :elapsed",
                   trim_id(task_id, 7, 3))
    total <- ceiling(timeout / every) + 1
  }
  if (progress_has_spin()) {
    fmt <- paste("(:spin)", fmt)
  }

  digits <- if (every < 1) abs(floor(log10(every))) else 0
  p <- progress(total, show=progress && timeout > 0, fmt=fmt)
  repeat {
    res <- context::task_result(handle, sanitise=TRUE)
    if (!inherits(res, "UnfetchableTask")) {
      p(1, update=TRUE, tokens=list(remaining="0s"))
      return(res)
    } else {
      rem <- t()
      if (rem < 0) {
        p(1, update=TRUE, tokens=list(remaining="0s"))
        stop("task not returned in time")
      } else {
        p(tokens=list(remaining=formatC(rem, digits=digits, format="f")))
        Sys.sleep(every)
      }
    }
  }
}
