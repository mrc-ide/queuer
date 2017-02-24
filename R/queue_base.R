##' @importFrom R6 R6Class
queue_base <- function(context_id, root = NULL, initialize = TRUE) {
  R6_queue_base$new(context_id, root, initialize)
}

R6_queue_base <- R6::R6Class(
  "queue_base",
  public =
    list(
      context = NULL,
      root = NULL,
      db = NULL,
      workdir = NULL,

      initialize = function(context_id, root, initialize = TRUE) {
        if (inherits(context_id, "context")) {
          if (!is.null(root)) {
            stop("'root' must be NULL if 'context_id' is a context object")
          }
          self$context <- context_id
          self$root <- self$context$root
        } else {
          self$root <- context::context_root_get(root)
          self$context <- context::context_read(context_id, self$root)
        }
        self$db <- self$root$db
        self$workdir <- getwd()

        if (initialize) {
          self$initialize_context()
        }
      },

      initialize_context = function() {
        ## TODO: it might be useful to allow a different environment
        ## here, rather than requiring the global environment?  The
        ## interface for doing that is tricky though because we need
        ## to pass the environment around from the beginning and pass
        ## it in whenever this is called.
        if (is.null(self$context$envir)) {
          message("Loading context ", self$context$id)
          self$context <- context::context_load(self$context)
       }
      },

      ## Some pluralisations:
      ## TODO: enable these
      ## tasks_list = function(...) with_deprecated(self, "task_list", ...),
      ## tasks_status = function(...) with_deprecated(self, "task_status", ...),
      ## tasks_times = function(...) with_deprecated(self, "task_times", ...),

      task_list = function() {
        context::task_list(self$db)
      },

      task_status = function(task_ids = NULL, named = TRUE) {
        if (is.null(task_ids)) {
          task_ids <- context::task_list(self$db)
        }
        context::task_status(task_ids, self$db, named)
      },

      task_times = function(task_ids = NULL, unit_elapsed = "secs",
                             sorted = TRUE) {
        if (is.null(task_ids)) {
          task_ids <- context::task_list(self$db)
        }
        context::task_times(task_ids, self$db, unit_elapsed, sorted)
      },

      task_get = function(task_id, check_exists = TRUE) {
        queuer_task(task_id, self$root, check_exists)
      },

      task_result = function(task_id) {
        context::task_result(task_id, self$db)
      },

      task_delete = function(task_ids) {
        self$unsubmit(task_ids)
        context::task_delete(task_ids, self$root)
      },

      task_bundle_list = function() {
        task_bundle_list(self$db)
      },

      task_bundle_info = function() {
        task_bundle_info(self)
      },

      task_bundle_get = function(name) {
        task_bundle_get(name, self$root)
      },

      enqueue = function(expr, envir = parent.frame(), submit = TRUE,
                         name = NULL) {
        self$enqueue_(substitute(expr), envir, submit, name)
      },

      enqueue_ = function(expr, envir = parent.frame(), submit = TRUE,
                          name = NULL) {
        self$initialize_context()
        task_id <- context::task_save(expr, self$context, envir)
        if (submit) {
          self$submit_or_delete(task_id, name)
        }
        invisible(queuer_task(task_id, self$root))
      },

      enqueue_bulk = function(X, FUN, ..., do_call = FALSE,
                              envir = parent.frame(),
                              timeout = 0, time_poll = 1, progress = NULL,
                              name = NULL, overwrite = FALSE) {
        enqueue_bulk(self, X, FUN, ..., do_call = do_call, envir = envir,
                     timeout = timeout, time_poll = time_poll,
                     progress = progress, name = name, overwrite = overwrite)
      },

      lapply = function(X, FUN, ..., envir = parent.frame(),
                        timeout = 0, time_poll = 1, progress = NULL,
                        name = NULL, overwrite = FALSE) {
        qlapply(X, FUN, self, ..., envir = envir,
                timeout = timeout, time_poll = time_poll,
                progress = progress, name = name, overwrite = overwrite)
      },

      ## These exist only as a stub for now, for other classes to
      ## override.
      submit = function(task_ids, names = NULL) {},
      unsubmit = function(task_ids) {},

      ## Internal wrapper
      submit_or_delete = function(task_ids, name = NULL) {
        delete_these_tasks <- function(e) {
          message("Deleting task as submission failed")
          context::task_delete(task_ids, self$root)
        }
        withCallingHandlers(self$submit(task_ids, name),
                            error = delete_these_tasks)
      }
    ))
