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

## NOTE: No lockfile for environment storage, won't be possible for
## redis, which we can easily lock with SETX, but will need a general
## interface for the lockfile.

queue_local <- function(context, log_path=NULL, initialise=TRUE) {
  .R6_queue_local$new(context, log_path, initialise)
}

.R6_queue_local <- R6::R6Class(
  "queue_local",
  inherit=.R6_queue_base,

  public=list(
    log_path=NULL,
    timeout=NULL,
    initialize=function(context, log_path, initialise) {
      loadNamespace("seagull")
      super$initialize(context, initialise)
      ## This can probably be relaxed to allow environment storage
      ## actually, though that would not survive a fork meaningfully.
      ## Redis storage can't be assumed to have the same filesystem,
      ## though rrlite can.
      ##
      ## What would be nicer is if storr allowed some way of generally
      ## locking but that's getting a bit far out and would induce a
      ## seagull dependency in storr.
      db <- context::context_db(self$context)

      self$timeout <- 10.0 # Eh. needs dealing with
      if (!is.null(log_path)) {
        if (context::is_absolute_path(log_path)) {
          dir.create(log_path, FALSE, TRUE)
        } else {
          root <- context::context_root(self)
          dir.create(file.path(root, log_path), FALSE, TRUE)
        }
        self$log_path <- log_path
      }
    },

    ## This is the running half of the system; these will shortly move
    ## into workers, but the queue_local case might be weird enough to
    ## warrant keeping them here too.
    run_task=function(task_id, ...) {
      h <- self$task_get(task_id)$handle
      if (is.null(self$log_path)) {
        ## there's plenty of printing here without printing any extra
        context::task_run(h, ...)
      } else {
        log <- file.path(context::context_root(self), self$log_path, task_id)
        context::context_db(self)$set(task_id, self$log_path, "log_path")
        message(sprintf("*** Running %s -> %s", task_id, log))
        ## Suppress messages I think.
        capture_log(context::task_run(h, ...), log, TRUE)
      }
    },
    run_next=function() {
      ## TODO: It is possible for the task to be lost here if
      ## context::task_run throws an error.  We should be atomically
      ## doing something within the queue code to indicate where the
      ## task has gone.  Otherwise task stays as PENDING (rather than
      ## RUNNING) but is not in the queue.
      task_id <- queue_local_pop(self)
      if (is.null(task_id)) {
        value <- NULL
      } else {
        value <- self$run_task(task_id)
      }
      invisible(list(task_id=task_id, value=value))
    },
    run_all=function() {
      ## TODO: run_all and run_loop together perhaps
      res <- character(0)
      repeat {
        task_id <- self$run_next()$task_id
        if (is.null(task_id)) {
          message("All tasks complete")
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
    submit=function(task_ids, names=NULL) {
      ## NOTE: Need to set the log_path first or risk a race condition.
      if (!is.null(self$log_path)) {
        db <- context::context_db(self)
        for (id in task_ids) {
          db$set(id, self$log_path, "log_path")
        }
      }
      queue_local_submit(self, task_ids)
    },
    unsubmit=function(task_ids) {
      queue_local_unsubmit(self, task_ids)
    },
    queue_list=function() {
      queue_local_read(self)
    }
))

## Constants.  Can probably be made variables by stuffing them inside
## the context db but it's not very obvious why that's a good thing.
## An alternative (probably better) is to use the context id as the
## QUEUE_NAME leaving only the namespace one hanging.
QUEUE_NAME <- "queue"
QUEUE_NAMESPACE <- "queue_local"

queue_local_submit <- function(obj, task_ids) {
  ## TODO: This is not ideal, but the timeout does need to be
  ## specified.  I don't see that this should ever take two minutes so
  ## it's OK here.
  timeout <- obj$timeout %||% 120
  db <- context::context_db(obj)
  context_id <- obj$context$id
  lockfile <- path_lockfile(context::context_root(obj), context_id)

  seagull::with_flock(lockfile, {
    queue <- queue_local_read(obj)
    tot <- c(queue, task_ids)
    db$set(context_id, tot, QUEUE_NAMESPACE)
    invisible(length(tot))
  }, timeout=timeout)
}

queue_local_pop <- function(obj) {
  timeout <- obj$timeout %||% 120
  db <- context::context_db(obj)
  context_id <- obj$context$id
  lockfile <- path_lockfile(context::context_root(obj), context_id)

  seagull::with_flock(lockfile, {
    queue <- queue_local_read(obj)
    if (length(queue) > 0L) {
      db$set(context_id, queue[-1L], QUEUE_NAMESPACE)
      queue[[1]]
    } else {
      NULL
    }
  }, timeout=timeout)
}

queue_local_unsubmit <- function(obj, task_ids) {
  ## TODO: This is not ideal, but the timeout does need to be
  ## specified.  I don't see that this should ever take two minutes so
  ## it's OK here.
  timeout <- obj$timeout %||% 120
  db <- context::context_db(obj)
  context_id <- obj$context$id
  lockfile <- path_lockfile(context::context_root(obj), context_id)

  seagull::with_flock(lockfile, {
    queue <- queue_local_read(obj)
    if (length(queue) > 0L) {
      db$set(context_id, setdiff(queue, task_ids), QUEUE_NAMESPACE)
    }
    invisible(task_ids %in% queue)
  }, timeout=timeout)
}

## NOTE: not protected by file lock
queue_local_read <- function(obj) {
  db <- context::context_db(obj)
  context_id <- obj$context$id
  tryCatch(db$get(context_id, QUEUE_NAMESPACE),
           KeyError=function(e) character(0))
}

path_lockfile <- function(root, id) {
  file.path(root, "lockfiles", id)
}
