## A local dummy queue used for testing.  Eventually this could be
## spun out into its own thing and used with the future package to
## schedule future execution.  For now it will be focussed on serial
## execution I think.

## The aim of this is to implement enough so that it's obvious what
## this package needs to implement to support the various queuing
## approaches in rrqueue and the windows cluster (and eventually for
## the imperial cluster but it sounds like that's hard).

## There are some fairly insurmountable issues with using a local
## queue here unfortunately; there's no real way of using the blocking
## wait because we need two event loops.  Might have to switch locally
## there.

queue_local <- function(context) {
  .R6_queue_local$new(context)
}

.R6_queue_local <- R6::R6Class(
  "queue_local",
  inherit=.R6_queue_base,

  public=list(
    lockfile=NULL,
    timeout=NULL,
    initialize=function(context) {
      loadNamespace("seagull")
      super$initialize(context)
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
        ## NOTE: No lockfile for environment storage, won't be
        ## possible for redis, which we can easily lock with SETX, but
        ## will need a general interface for the lockfile.
      }
      self$timeout <- 10.0
    },

    ## This is the running half of the system; these will shortly move
    ## into workers, but the queue_local case might be weird enough to
    ## warrant keeping them here too.
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
    run_loop=function(interval=0.5) {
      ## TODO: Until messaging is implemented, this will need to run
      ## until interrupted.
      ##
      ## TODO: get a growing timeout in here too (abstract away the
      ## one I have in seagull:R/util.R:retry()
      repeat {
        res <- self$run_next()
        if (is.null(res$task_id)) {
          Sys.sleep(interval)
        }
      }
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
      local_queue_read(context::context_db(self), "queue", "queue_local")
    },

    queue_op=function(f, ...) {
      f(self$lockfile, context::context_db(self), "queue", "queue_local",
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
    tot <- c(queue, ids)
    db$set(key, tot, namespace)
    length(tot)
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
