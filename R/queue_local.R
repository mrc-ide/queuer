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
queue_local <- R6::R6Class(
  "queue_local",
  inherit = queue_base,

  public = list(
    log_path = NULL,
    timeout = 10.0, # TODO: configurable...
    fifo = NULL,

    initialize = function(context_id, root = NULL, initialize = TRUE,
                          log = FALSE) {
      super$initialize(context_id, root, initialize)

      path_fifo <- file.path(private$root$path, "fifo", self$context$id)
      dir.create(dirname(path_fifo), FALSE, TRUE)
      self$fifo <- fifo_thor$new(path_fifo)

      if (isTRUE(log)) {
        self$log_path <- file.path(private$root$path, "logs")
        dir.create(self$log_path, FALSE, TRUE)
      }

      write_queue_local_worker(private$root)
    },

    ## This is the running half of the system; these will shortly move
    ## into workers, but the queue_local case might be weird enough to
    ## warrant keeping them here too.
    run_task = function(task_id) {
      ## There is an issue here (and in the other workers, though
      ## potentially a bit less of an issue) where we need to check
      ## that the context that is loaded is the appropriate one.  This
      ## probably means that task_run should get a 'context_id'
      ## argument that we can pass to it so that it will throw an
      ## error if a context mismatch is detected.  Practically, this
      ## is not something I want to be too concerned about though as
      ## it will just lead to lots and lots of stray 'PENDING' jobs.
      if (is.null(self$log_path)) {
        logfile <- NULL
      } else {
        logfile <- file.path(self$log_path, task_id)
        context::context_log("running", task_id)
      }
      context::task_run(task_id, self$context, logfile)
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
          context::context_log("done", "All tasks complete")
          break
        } else {
          res <- c(res, task_id)
        }
      }
      res
    },

    run_loop = function(interval = 0.5) {
      ## TODO: Until messaging is implemented, this will need to run
      ## until interrupted.  Messaging should be fairly easy to
      ## implement because we can just poll a key with exists().
      tryCatch(
        repeat {
          res <- self$run_next()
          if (is.null(res$task_id)) {
            Sys.sleep(interval)
          }
        },
        interrupt = function(e) {
          context::context_log("interrupt", Sys.time())
        })
      context::context_log("done", "interrupted")
    },

    ## Ordinarily there would be two objects created; one worker and
    ## one queue.  but for the local queue these are the same.
    submit = function(task_ids, names = NULL, depends_on = NULL) {
      if (!is.null(self$log_path)) {
        private$db$mset(task_ids, self$log_path, "log_path")
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
