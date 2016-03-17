queue_base <- function(context) {
  .R6_queue$new(context)
}

.R6_queue_base <- R6::R6Class(
  "queue_base",
  public=
    list(
      context=NULL,
      context_envir=NULL,
      root=NULL,
      db=NULL,
      workdir=NULL,
      initialize=function(context) {
        if (!inherits(context, "context_handle")) {
          stop("Expected a context object")
        }
        ## NOTE: The root is needed so that tasks can run correctly;
        ## we need this to set the local library.
        self$root <- context$root
        ## NOTE: We need a copy of the db within the object for
        ## context::context_db() elsewhere to work correctly.
        self$db <- context::context_db(context)
        self$workdir <- getwd()

        ## Consider making this optional?
        ctx <- context::context_read(context)
        if (ctx$auto) {
          stop("auto environments not yet supported")
        } else {
          message("Loading context ", context$id)
          self$context <- ctx
          self$context_envir <- context::context_load(context)
        }
      },

      tasks_list=function() {
        tasks_list(self)
      },
      tasks_status=function(task_ids=NULL, follow_redirect=FALSE, named=TRUE) {
        tasks_status(self, task_ids, follow_redirect, named)
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

      task_bundles_list=function() {
        tasks_bundles_list(self)
      },
      task_bundle_get=function(id) {
        task_bundle_get(self, id)
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