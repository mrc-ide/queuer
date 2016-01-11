## A local dummy queue used for testing.  Eventually this could be
## spun out into its own thing and used with the future package to
## schedule future execution.  For now it will be focussed on serial
## execution I think.

## The aim of this is to implement enough so that it's obvious what
## this package needs to implement to support the various queuing
## approaches in rrqueue and the windows cluster (and eventually for
## the imperial cluster but it sounds like that's hard).
queue_base <- function(context, config=NULL) {
  .R6_queue$new(context, config)
}

.R6_queue_base <- R6::R6Class(
  "queue_base",
  public=
    list(
      config=NULL,
      context=NULL,
      root=NULL,
      db=NULL,
      workdir=NULL,
      initialize=function(context, config=NULL) {
        if (!inherits(context, "context_handle")) {
          stop("Expected a context object")
        }
        self$config <- config
        self$context <- context
        ## NOTE: The root is needed so that tasks can run correctly;
        ## we need this to set the local library.
        self$root <- context$root
        ## NOTE: We need a copy of the db within the object for
        ## context::context_db() elsewhere to work correctly.
        self$db <- context::context_db(context)
        self$workdir <- getwd()
      },

      tasks_list=function() {
        tasks_list(self)
      },
      tasks_status=function(task_ids=NULL, follow_redirect=FALSE) {
        tasks_status(self, task_ids, follow_redirect)
      },
      tasks_times=function(task_ids=NULL, unit_elapsed="secs") {
        tasks_times(self, task_ids, unit_elapsed)
      },
      task_get=function(task_id) {
        task(self, task_id)
      },
      task_result=function(task_id, follow_redirect=FALSE) {
        task_result(self, task_id, follow_redirect)
      },
      tasks_drop=function(task_ids) {
        tasks_drop(self, task_ids)
        unsubmit(self, task_ids)
      },

      enqueue=function(expr, ..., envir=parent.frame(), submit=TRUE) {
        self$enqueue_(substitute(expr), ..., envir=envir, submit=submit)
      },
      ## I don't know that these always want to be submitted.
      enqueue_=function(expr, ..., envir=parent.frame(), submit=TRUE) {
        task <- context::task_save(expr, self$context, envir)
        if (submit) {
          withCallingHandlers(self$submit(task$id),
                              error=function(e) {
                                message("Deleting task as submission failed")
                                context::task_delete(task)
                              })
        }
        invisible(task(self, task$id))
      },

      ## These exist only as a stub for now, for other classes to
      ## override.
      submit=function(task_ids) {},
      unsubmit=function(task_ids) {}
    ))

queue_local <- function(...) {
  .R6_queue_local$new(...)
}

.R6_queue_local <- R6::R6Class(
  "queue_local",
  inherit=.R6_queue_base,

  public=list(
    lockfile=NULL,
    timeout=NULL,
    initialize=function(...) {
      super$initialize(...)
      ## This can probably be relaxed to allow environment storage
      ## actually, though that would not survive a fork meaningfully.
      ## Redis storage can't be assumed to have the same filesystem,
      ## though rrlite can.
      ##
      ## What would be nicer is if storr allowed some way of generally
      ## locking but that's getting a bit far out and would induce a
      ## seagull dependency in storr.
      db <- context::context_db(self$context)

      if (db$driver$type() == "rds") {
        ## NOTE: So far, this is the only part that makes an explicit
        ## reference to the root, so that's nice.
        self$lockfile <- file.path(self$context$root, "lockfile")
      } else {
        ## No lockfile for environment storage.
        warning("Some code may assume rds storage, or shared filesystems")
      }
      self$timeout <- 10.0
    },

    ## This is the running half of the system.
    run_task=function(task_id, ...) {
      context::task_run(self$task_get(task_id)$handle, ...)
    },
    run_next=function() {
      ## TODO: It is possible for the task to be lost here if
      ## context::task_run throws an error.  We should be atomically
      ## doing something within the queue code to indicate where the
      ## task has gone.  Otherwise task stays as PENDING (rather than
      ## RUNNING) but is not in the queue.
      task_id <- self$queue_op(local_queue_pop)
      if (is.null(task_id)) {
        value <- NULL
      } else {
        value <- self$run_task(task_id)
      }
      invisible(list(task_id=task_id, value=value))
    },
    run_all=function() {
      res <- character(0)
      repeat {
        task_id <- self$run_next()$task_id
        if (is.null(task_id)) {
          break
        } else {
          res <- c(res, task_id)
        }
      }
      res
    },

    ## Ordinarily there would be two objects created; one worker and
    ## one queue.  but for the local queue these are the same.
    submit=function(task_ids) {
      self$queue_op(local_queue_push, task_ids)
    },
    unsubmit=function(task_ids) {
      self$queue_op(local_queue_del, task_ids)
    },
    ## NOTE: not protected by lock, and duplication of key/ns names
    queue_list=function() {
      local_queue_read(self$context$db, "queue", "queue_local")
    },

    queue_op=function(f, ...) {
      f(self$lockfile, self$context$db, "queue", "queue_local",
        ..., timeout=self$timeout)
    }
))

## Run queue operations through a locked queue; these all do a read
## and and a write, though the write is conditional.  The lockfile
## ensures that the read/write avoids a race condition.  Because the
## read/write is likely to be quite quick this should be fairly free
## of nasty deadlocks; I would expect everything resolved in well
## under a second as we're just storing a vector of small strings.
local_queue_push <- function(lockfile, db, key, namespace, ids, ...) {
  seagull::with_flock(lockfile, {
    queue <- local_queue_read(db, key, namespace)
    db$set(key, c(queue, ids), namespace)
  }, ...)
}

local_queue_pop <- function(lockfile, db, key, namespace, ...) {
  seagull::with_flock(lockfile, {
    queue <- local_queue_read(db, key, namespace)
    if (length(queue) > 0L) {
      db$set(key, queue[-1], namespace)
      queue[[1]]
    } else {
      NULL
    }
  }, ...)
}

local_queue_del <- function(lockfile, db, key, namespace, ids, ...) {
  seagull::with_flock(lockfile, {
    queue <- local_queue_read(db, key, namespace)
    if (length(queue) > 0L) {
      db$set(key, setdiff(queue, ids), namespace)
    }
    invisible(ids %in% queue)
  }, ...)
}

local_queue_read <- function(db, key, namespace) {
  tryCatch(db$get(key, namespace),
           KeyError=function(e) character(0))
}
