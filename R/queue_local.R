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

queue_local <- function(context_id, root = NULL, initialize = TRUE,
                        log = FALSE) {
  .R6_queue_local$new(context_id, root, initialize, log)
}

.R6_queue_local <- R6::R6Class(
  "queue_local",
  inherit = .R6_queue_base,

  public = list(
    log_path = NULL,
    timeout = 10.0, # TODO: configurable...
    fifo = NULL,

    initialize = function(context_id, root, initialize, log) {
      super$initialize(context_id, root, initialize)

      lockfile <- path_lockfile(self$root$path, self$context$id)
      dir.create(dirname(lockfile), FALSE, TRUE)
      self$fifo <- fifo_seagull(self$db, self$context$id, "queue_local",
                                lockfile, self$timeout)

      if (isTRUE(log)) {
        self$log_path <- file.path(self$root$path, "logs")
        dir.create(self$log_path, FALSE, TRUE)
      }

      worker_runner <- file.path(self$root$path, "bin", "worker_runner")
      if (!file.exists(worker_runner)) {
        file.copy(system.file("bin/worker_runner", package = "queuer"),
                  worker_runner)
      }
    },

    ## This is the running half of the system; these will shortly move
    ## into workers, but the queue_local case might be weird enough to
    ## warrant keeping them here too.
    run_task = function(task_id, ...) {
      if (is.null(self$log_path)) {
        ## there's plenty of printing here without printing any extra
        context::task_run(task_id, self$context, self$context_envir, ...)
      } else {
        log <- file.path(self$log_path, task_id)
        message(sprintf("*** Running %s -> %s", task_id, log))
        context::task_run(task_id, self$context, self$context_envir, log)
      }
    },

    run_next = function() {
      ## TODO: It is possible for the task to be lost here if
      ## context::task_run throws an error.  We should be atomically
      ## doing something within the queue code to indicate where the
      ## task has gone.  Otherwise task stays as PENDING (rather than
      ## RUNNING) but is not in the queue.
      task_id <- self$fifo$pop()
      if (is.null(task_id)) {
        value <- NULL
      } else {
        value <- self$run_task(task_id)
      }
      invisible(list(task_id = task_id, value = value))
    },

    run_all = function() {
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

    run_loop = function(interval = 0.5) {
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
    submit = function(task_ids, names = NULL) {
      if (!is.null(self$log_path)) {
        self$db$mset(task_ids, self$log_path, "log_path")
      }
      self$fifo$push(task_ids)
    },

    unsubmit = function(task_ids) {
      self$fifo$drop(task_ids)
    },

    queue_list = function() {
      self$fifo$read()
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
  db <- obj$db
  context_id <- obj$context$id
  lockfile <- path_lockfile(context::context_root(obj), context_id)

  seagull::with_flock(lockfile, {
    queue <- queue_local_read(obj)
    tot <- c(queue, task_ids)
    db$set(context_id, tot, QUEUE_NAMESPACE)
    invisible(length(tot))
  }, timeout = timeout)
}

## NOTE: not protected by file lock
queue_local_read <- function(obj) {
  db <- obj$db
  context_id <- obj$context$id
  tryCatch(db$get(context_id, QUEUE_NAMESPACE),
           KeyError = function(e) character(0))
}

path_lockfile <- function(root, id) {
  file.path(root, "lockfiles", id)
}
