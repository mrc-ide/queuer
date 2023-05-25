##' @title Base class for queues
##'
##' @description A base class, on top of which queues can be
##'   developed. This includes all methods except for support for
##'   actually submitting tasks.
##'
##' @export
queue_base <- R6::R6Class(
  "queue_base",
  cloneable = FALSE,
  public = list(
    ##' @field context The context object
    context = NULL,

    ##' @description Constructor
    ##'
    ##' @param context_id A context identifier; either a context
    ##'   object or an name/id of a saved context (see
    ##'   [context::context_save()]).
    ##'
    ##' @param root Root path to load contexts from, if using a string
    ##'   identifier for `context_id`. If `context_id` is a `context`
    ##'   object then `root` must be `NULL`.
    ##'
    ##' @param initialize Logical, indicating if the context should be
    ##'   loaded immediately. If you want to run tasks this must be
    ##'   `TRUE`, but to query it can be `FALSE`. See
    ##'   [context::context_load()] and the `$initialise_context()` method.
    initialize = function(context_id, root = NULL, initialize = TRUE) {
      if (inherits(context_id, "context")) {
        if (!is.null(root)) {
          stop("'root' must be NULL if 'context_id' is a context object")
        }
        self$context <- context_id
        private$root <- self$context$root
      } else {
        private$root <- context::context_root_get(root)
        self$context <- context::context_read(context_id, private$root)
      }
      private$db <- private$root$db

      if (initialize) {
        self$initialize_context()
      }
    },

    ##' @description Load the context. This causes the packages to be
    ##'   loaded and all script files to be sourced. This is required
    ##'   before any tasks can be queued, because we need to be check
    ##'   against this environment to work out what is available on
    ##'   any workers.
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

    ##' @description List all tasks known to this queue
    task_list = function() {
      context::task_list(private$db)
    },

    ##' @description Return the status of selected tasks
    ##'
    ##' @param task_ids Task identifiers to query. If `NULL`, then all
    ##'   tasks are queried.
    ##'
    ##' @param named Logical, indicating if the status result should be
    ##'   named by by task id.
    task_status = function(task_ids = NULL, named = TRUE) {
      if (is.null(task_ids)) {
        task_ids <- context::task_list(private$db)
      }
      context::task_status(task_ids, private$db, named)
    },

    ##' @description Return the times taken by tasks as a [data.frame]
    ##'
    ##' @param task_ids Task identifiers to query. If `NULL`, then all
    ##'   tasks are queried.
    ##'
    ##' @param unit_elapsed Time unit to use for the elapsed fields
    ##'
    ##' @param sorted Logical indicating of the fields should be sorted
    ##'   by submitted time.
    task_times = function(task_ids = NULL, unit_elapsed = "secs",
                          sorted = TRUE) {
      if (is.null(task_ids)) {
        task_ids <- context::task_list(private$db)
      }
      context::task_times(task_ids, private$db, unit_elapsed, sorted)
    },

    ##' @description Retrieve a task by id
    ##'
    ##' @param task_id A task identifier (hexadecimal string)
    ##'
    ##' @param check_exists Logical, indicating if we should check
    ##'   that the task exists.
    task_get = function(task_id, check_exists = TRUE) {
      queuer_task$new(task_id, private$root, check_exists)
    },

    ##' @description Retrieve a task's result
    ##'
    ##' @param task_id A task identifier (hexadecimal string)
    task_result = function(task_id) {
      context::task_result(task_id, private$db)
    },

    ##' @description Delete tasks
    ##'
    ##' @param task_ids A vector of task identifiers (each a
    ##'   hexadecimal string)
    task_delete = function(task_ids) {
      self$unsubmit(task_ids)
      context::task_delete(task_ids, private$root)
    },

    ##' @description Retry failed tasks
    ##'
    ##' @param task_ids A vector of task identifiers (each a
    ##'   hexadecimal string)
    task_retry_failed = function(task_ids) {
      ret <- context::task_status(task_ids, private$db)
      failed <- set_names(ret == "ERROR", task_ids)
      ids <- names(failed[failed])
      context::task_reset(ids, self$context)
      private$submit_or_delete(ids)
    },

    ##' @description List all known task bundles
    task_bundle_list = function() {
      task_bundle_list(private$db)
    },

    ##' @description List all known task bundles along with information
    ##'   about what was run and when.
    task_bundle_info = function() {
      task_bundle_info(self)
    },

    ##' @description Get a task bundle by name
    ##'
    ##' @param name A task bundle identifier (a string of the form
    ##'   `adjective_anmimal`)
    task_bundle_get = function(name) {
      task_bundle$new(name, private$root)
    },

    ##' @description Retry failed tasks in a bundle
    ##'
    ##' @param name A task bundle identifier (a string of the form
    ##'   `adjective_anmimal`)
    task_bundle_retry_failed = function(name) {
      b <- self$task_bundle_get(name)
      self$task_retry_failed(b$ids)
    },

    ##' @description Queue a task
    ##'
    ##' @param expr An unevaluated expression to put on the queue
    ##'
    ##' @param envir The environment that you would run this expression in
    ##'   locally. This will be used to copy across any dependent variables.
    ##'   For example, if your expression is `sum(1 + a)`, we will also send
    ##'   the value of `a` to the worker along with the expression.
    ##'
    ##' @param submit Logical indicating if the task should be submitted
    ##'
    ##' @param depends_on Optional vector of task ids to depend on
    ##'
    ##' @param name Optional name for the task
    enqueue = function(expr, envir = parent.frame(), submit = TRUE,
                       name = NULL, depends_on = NULL) {
      ## TODO: when is submit = FALSE wanted?
      self$enqueue_(substitute(expr), envir, submit, name, depends_on)
    },

    ##' @description Queue a task
    ##'
    ##' @param expr A quoted expression to put on the queue
    ##'
    ##' @param envir The environment that you would run this expression in
    ##'   locally. This will be used to copy across any dependent variables.
    ##'   For example, if your expression is `sum(1 + a)`, we will also send
    ##'   the value of `a` to the worker along with the expression.
    ##'
    ##' @param submit Logical indicating if the task should be submitted
    ##'
    ##' @param name Optional name for the task
    ##'
    ##' @param depends_on Optional vector of task ids to depend on
    ##'
    enqueue_ = function(expr, envir = parent.frame(), submit = TRUE,
                        name = NULL, depends_on = NULL) {
      self$initialize_context()
      task_id <- context::task_save(expr, self$context, envir, depends_on = depends_on)
      if (submit) {
        private$submit_or_delete(task_id, name)
      }
      invisible(queuer_task$new(task_id, private$root))
    },

    ##' @description Send a bulk set of tasks to your workers.
    ##' This function is a bit like a mash-up of [Map] and [do.call],
    ##' when used with a [data.frame] argument, which is typically what
    ##' is provided. Rather than `$lapply()` which applies `FUN` to each
    ##' element of `X`, `enqueue_bulk will apply over each row of `X`,
    ##' spreading the columms out as arguments. If you have a function
    ##' `f(a, b)` and a [data.frame] with columns `a` and `b` this
    ##' should feel intuitive.
    ##'
    ##' @param X Typically a [data.frame], which you want to apply `FUN`
    ##'   over, row-wise. The names of the `data.frame` must match the
    ##'   arguments of your function.
    ##'
    ##' @param FUN A function
    ##'
    ##' @param ... Additional arguments to add to every call to `FUN`
    ##'
    ##' @param do_call Logical, indicating if each row of `X` should be
    ##'   treated as if it was `do.call(FUN, X[i, ])` - typically this is what
    ##'   you want.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed.
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param name Optional name for a created bundle
    ##'
    ##' @param depends_on Optional task ids to depend on (see
    ##'   [context::bulk_task_save()]).
    ##'
    ##' @param overwrite Logical, indicating if we should overwrite any
    ##'   bundle that exists with name `name`.
    enqueue_bulk = function(X, FUN, ..., do_call = TRUE,
                            envir = parent.frame(),
                            timeout = 0, time_poll = 1, progress = NULL,
                            name = NULL, overwrite = FALSE, depends_on = NULL) {
      enqueue_bulk(self, private, X, FUN, ..., do_call = do_call, envir = envir,
                   timeout = timeout, time_poll = time_poll,
                   progress = progress, name = name, overwrite = overwrite, depends_on = depends_on)
    },

    ##' @description Apply a function over a list of data. This is
    ##' equivalent to using `$enqueue()` over each element in the list.
    ##'
    ##' @param X A list of data to apply our function against
    ##'
    ##' @param FUN A function to be applied to each element of `X`
    ##'
    ##' @param ... Additional arguments to add to every call to `FUN`
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed.
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param name Optional name for a created bundle
    ##'
    ##' @param overwrite Logical, indicating if we should overwrite any
    ##'   bundle that exists with name `name`.
    ##'
    ##' @param depends_on Optional task ids to depend on (see
    ##'   [context::bulk_task_save()]).
    lapply = function(X, FUN, ..., envir = parent.frame(),
                      timeout = 0, time_poll = 1, progress = NULL,
                      name = NULL, overwrite = FALSE, depends_on = NULL) {
      qlapply(self, private, X, FUN, ..., envir = envir,
              timeout = timeout, time_poll = time_poll,
              progress = progress, name = name, overwrite = overwrite, depends_on = depends_on)
    },

    ##' @description A wrapper like `mapply`
    ##' @description Send a bulk set of tasks to your workers.
    ##' This function is a bit like a mash-up of [Map] and [do.call],
    ##' when used with a [data.frame] argument, which is typically what
    ##' is provided. Rather than `$lapply()` which applies `FUN` to each
    ##' element of `X`, `enqueue_bulk will apply over each row of `X`,
    ##' spreading the columms out as arguments. If you have a function
    ##' `f(a, b)` and a [data.frame] with columns `a` and `b` this
    ##' should feel intuitive.
    ##'
    ##' @param X Typically a [data.frame], which you want to apply `FUN`
    ##'   over, row-wise. The names of the `data.frame` must match the
    ##'   arguments of your function.
    ##'
    ##' @param FUN A function
    ##'
    ##' @param ... Additional arguments to add to every call to `FUN`
    ##'
    ##' @param MoreArgs As for [mapply], additional arguments that apply
    ##'   to every function call.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed.
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param name Optional name for a created bundle
    ##'
    ##' @param overwrite Logical, indicating if we should overwrite any
    ##'   bundle that exists with name `name`.
    ##'
    ##' @param use_names Use names
    ##'
    ##' @param depends_on Optional task ids to depend on (see
    ##'   [context::bulk_task_save()]).
    mapply = function(FUN, ..., MoreArgs = NULL,
                      envir = parent.frame(), timeout = 0,
                      time_poll = 1, progress = NULL, name = NULL,
                      use_names = TRUE, overwrite = FALSE, depends_on = NULL) {
      ## TODO: consider deleting
      X <- mapply_X(...)
      self$enqueue_bulk(X, FUN, DOTS = MoreArgs, do_call = TRUE,
                        envir = envir, timeout = timeout,
                        time_poll = time_poll, progress = progress,
                        name = name, use_names = use_names,
                        overwrite = overwrite, depends_on = depends_on)
    },

    ##' @description Submit a task into a queue. This is a stub
    ##'   method and must be overridden by a derived class for the queue
    ##'   to do anything.
    ##'
    ##' @param task_ids Vector of tasks to submit
    ##'
    ##' @param names Optional vector of names of tasks
    ##'
    ##' @param depends_on Optional named list of task ids to vectors of
    ##'    dependencies, e.g. list("t3" = c("t", "t1"), "t4" = "t)
    submit = function(task_ids, names = NULL, depends_on = NULL) {
    },

    ##' @description Unsubmit a task from the queue.  This is a stub
    ##'   method and must be overridden by a derived class for the queue
    ##'   to do anything.
    ##'
    ##' @param task_ids Vector of tasks to submit
    unsubmit = function(task_ids) {
    }
  ),

  private = list(
    root = NULL,
    db = NULL,

    submit_or_delete = function(task_ids, name = NULL) {
      dependencies <- context::task_deps(task_ids, private$db, named = TRUE)
      delete_these_tasks <- function(e) {
        message("Deleting task as submission failed")
        context::task_delete(task_ids, private$root)
      }
      withCallingHandlers(self$submit(task_ids, name, dependencies),
                          error = delete_these_tasks)
    }
  ))
