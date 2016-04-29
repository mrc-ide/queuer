## Right.  The deal with rrqueue was that we could pass around
## {con,keys} as a way of getting in and extracting information about
## what jobs are where.  Here we'll pass 'obj' as the object and see
## what happens.  `obj` can be anything that satisfies having elements:
##
##    * config (was used in didewin for tasks_ours which is ignored at present, but most things will want to have I suspect).
##    * db -- gettable with context::context_db I hope.
##
## Better would be to stuff the config into the db for everything
## other than the actual database.  Using etcd to help with discovery
## would be interesting.

## The things in here attempt to be generic and make no claims about
## how the tasks actually run.

## I want to allow different underlying objects to be able to override
## some of these methods easily, but not sure how that will play out.
##
## For example, it might be nice if the rrqueue version, which is
## likely to run on a non-shared filesystem, could add the log into
## the database on completion.

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

        initialize=function(obj, id, check_exists=TRUE) {
          self$id <- id
          self$handle <- context::task_handle(obj, id, check_exists)
          self$db <- context::context_db(self$handle)
        },
        status=function(follow_redirect=FALSE, named=FALSE) {
          tasks_status(self, self$id, follow_redirect, named)
        },
        result=function(follow_redirect=FALSE, sanitise=FALSE) {
          task_result(self, self$id, follow_redirect, sanitise)
        },
        expr=function(locals=FALSE) {
          task_expr(self, self$id, locals)
        },
        context_id=function() {
          context::task_read(self$handle)$context_id
        },
        ## TODO: add context_handle() perhaps?
        ## NOTE: no run() method; should that be added?
        times=function(unit_elapsed="secs") {
          tasks_times(self, self$id, unit_elapsed)
        },
        wait=function(timeout, every=0.5, progress=TRUE) {
          task_wait(self, self$id, timeout, every, progress)
        }))

task_result <- function(obj, task_id, follow_redirect=FALSE, sanitise=FALSE) {
  if (follow_redirect) {
    .NotYetUsed("follow_redirect")
  }
  status <- tasks_status(obj, task_id)
  ## TODO: organise for these to be correctly exported from context as
  ## TASK (so we have TASK$COMPLETE, TASK$ERROR, etc.
  if (status == "COMPLETE" || status == "ERROR") {
    context::task_result(context::task_handle(obj, task_id, FALSE))
  } else if (sanitise) {
    UnfetchableTask(task_id, status)
  } else {
    stop(sprintf("task %s is unfetchable: %s", task_id, status))
  }
}

task_expr <- function(obj, task_id, locals) {
  t <- context::task_read(context::task_handle(obj, task_id))
  ret <- t$expr
  if (locals) {
    attr(ret, "locals") <- t$objects
  }
  ret
}

tasks_list <- function(obj) {
  context::tasks_list(obj)
}

tasks_status <- function(obj, task_ids, follow_redirect=FALSE, named=TRUE) {
  if (follow_redirect) {
    .NotYetUsed("follow_redirect")
  }
  if (is.null(task_ids)) {
    task_ids <- tasks_list(obj)
  }
  task_handle <- context::task_handle(obj, task_ids, TRUE)
  context::task_status(task_handle, named=named)
}

## TODO: It might be useful to have a "sorted" option here, because
## it's a bit confusing that when a specific list of tasks is given
## the output is a different order.  However, because the task_ids
## might be generated at any point above this it's hard to tell when a
## sorted/unsorted list is wanted.
tasks_times <- function(obj, task_ids, unit_elapsed="secs") {
  if (is.null(task_ids)) {
    task_ids <- tasks_list(obj)
  }
  db <- context::context_db(obj)
  if (length(task_ids) == 0L) {
    empty_time <- Sys.time()[-1]
    ret <- data.frame(task_id=character(0),
                      submitted=empty_time,
                      started=empty_time,
                      finished=empty_time,
                      waiting=numeric(0),
                      running=numeric(0),
                      idle=numeric(0),
                      stringsAsFactors=FALSE)
  } else {
    f <- function(ids, type) {
      NA_POSIXct <- as.POSIXct(NA)
      gett <- function(id) {
        tryCatch(db$get(id, type),
                 KeyError=function(e) NA_POSIXct)
      }
      res <- lapply(ids, gett)
      ret <- unlist(res)
      class(ret) <- c("POSIXct", "POSIXt")
      ret
    }
    ret <- data.frame(task_id   = task_ids,
                      submitted = f(task_ids, "task_time_sub"),
                      started   = f(task_ids, "task_time_beg"),
                      finished  = f(task_ids, "task_time_end"),
                      stringsAsFactors=FALSE)
    ret <- ret[order(ret$submitted), ]
    rownames(ret) <- NULL
    started2  <- ret$started
    finished2 <- ret$finished
    now <- Sys.time()
    finished2[is.na(finished2)] <- started2[is.na(started2)] <- now
    ret$waiting <- as.numeric(started2  - ret$submitted, unit_elapsed)
    ret$running <- as.numeric(finished2 - ret$started,   unit_elapsed)
    ret$idle    <- as.numeric(now       - ret$finished,  unit_elapsed)
  }
  ret
}

task_wait <- function(obj, task_id, timeout, every=0.5, progress=TRUE) {
  t <- time_checker(timeout, TRUE)
  every <- min(every, timeout)

  fmt <- sprintf("waiting for %s, giving up in :remaining s",
                 trim_id(task_id, 7, 3), timeout)
  if (progress && progress_has_spin()) {
    fmt <- paste("(:spin)", fmt)
  }

  digits <- if (every < 1) abs(floor(log10(every))) else 0
  total <- timeout / every + 1
  p <- progress(total, show=progress, fmt=fmt)

  repeat {
    res <- task_result(obj, task_id, sanitise=TRUE)
    if (!inherits(res, "UnfetchableTask")) {
      p(total, update=TRUE)
      return(res)
    } else {
      rem <- t()
      if (rem < 0) {
        p(total, update=TRUE)
        stop("task not returned in time")
      } else {
        p(tokens=list(remaining=formatC(rem, digits=digits, format="f")))
        Sys.sleep(every)
      }
    }
  }
}

tasks_drop <- function(obj, task_ids) {
  ## TODO: This should also cancel *running* tasks, and that sort of
  ## thing, but that's going to require support from different
  ## underlying drivers.
  context::task_delete(context::task_handle(obj, task_ids, FALSE))
  ## task_unschedule(obj, task_ids)
}

pretty_context_log <- function(x) {
  yellow <- crayon::make_style("yellow")$bold
  green <- crayon::make_style("blue")$bold
  x$str <- green(x$str)
  i <- vapply(x$body, length, integer(1)) > 0L
  x$body[i] <- lapply(x$body[i], yellow)
  x
}
