##' A queued version of \code{\link{lapply}}.  With this, tasks are
##' sent to a queue (specified by \code{obj}) and run in any order,
##' without communication between tasks.  The functions
##' \code{enqueue_bulk} function is a slightly different inteface that
##' allows looping over rows in a data.frame as if they are parameters
##' to a function.
##'
##' If the function is interrupted after all tasks have been submitted
##' (the progress bar will be displayed at this point) then
##' interrupting the process (e.g. with Ctrl-C) will return a
##' \code{task_bundle} object that can be queried.  Otherwise if the
##' timeout is reached an error will be thrown.  In either case the
##' tasks will continue on the cluster.
##'
##' @title Run tasks in a queue
##'
##' @param X A vector (atomic or list) to evaluate \code{FUN} on each
##'   element of.
##'
##' @param FUN A function.  This can be a function specified by value
##'   (e.g. \code{sin}) or by name (e.g. \code{"sin"}).  Some effort
##'   is made to determine that the function can be found in the
##'   environment that the queue itself uses.
##'
##' @param obj The queue object.
##'
##' @param ... Additional arguments to pass through to \code{FUN}
##'   along with each element of \code{X}.
##'
##' @param envir Environment to search for functions in.  This might change.
##'
##' @param timeout Time to wait for tasks to be returned.  The default
##'   is to wait forever.  If \code{0} (or a negative value) is given
##'   then \code{qlapply} will not block but will instead return a
##'   \code{task_bundle} object which can be used to inspect the
##'   task status.
##'
##' @param time_poll How often to check for task completion.  The
##'   default is every second.  This is an \emph{approximate} time and
##'   should be seen as a lower limit.
##'
##' @param progress_bar Display a progress bar as tasks are polled.
##'
##' @export
qlapply <- function(X, FUN, obj, ...,
                    envir=parent.frame(),
                    timeout=Inf, time_poll=1, progress_bar=TRUE) {
  enqueue_bulk(obj, X, FUN, ...,
               do.call=FALSE,
               timeout=timeout, time_poll=time_poll, progress_bar=progress_bar,
               envir=envir)
}

## A downside of the current treatment of dots is there are quite a
## few arguments on the RHS of it; if a function uses any of these
## they're not going to be allowed access to them.  Usually this seems
## solved by something like progress_bar.=TRUE but I think that looks
## horrid.  So for now leave it as-is and we'll see what happens.

##' @export
##' @rdname qlapply
##'
##' @param do.call If \code{TRUE}, rather than evaluating \code{FUN(x,
##'   ...)}, evaluate \code{FUN(x[1], x[2], ..., x[n], ...)} (where
##'   \code{x} is an element of \code{X}).
enqueue_bulk <- function(obj, X, FUN, ..., do.call=FALSE,
                         timeout=Inf, time_poll=1, progress_bar=TRUE,
                         envir=parent.frame()) {
  obj <- enqueue_bulk_submit(obj, X, FUN, ..., do.call=do.call, envir=envir)
  if (timeout > 0) {
    tryCatch(obj$wait(timeout, time_poll, progress_bar),
             interrupt=function(e) obj)
  } else {
    obj
  }
}

enqueue_bulk_submit <- function(obj, X, FUN, ..., do.call=FALSE,
                                envir=parent.frame()) {
  if (is.data.frame(X)) {
    X <- df_to_list(X)
  } else if (is.atomic(X)) {
    X <- as.list(X)
  } else if (!is.list(X)) {
    stop("X must be a data.frame or list")
  }

  fun <- find_fun_queue(FUN, envir, obj$context_envir)
  n <- length(X)
  DOTS <- list(...)

  tasks <- vector("list", n)
  group <- context:::random_id()
  for (i in seq_len(n)) {
    if (do.call) {
      tasks[[i]] <- as.call(c(list(fun), X[[i]], DOTS))
    } else {
      tasks[[i]] <- as.call(c(list(fun), X[i], DOTS))
    }
  }

  context <- obj$context

  res <- context::task_save_list(tasks, context, envir)
  withCallingHandlers({
    for (i in seq_len(n)) {
      obj$submit(res$id[[i]])
    }
  }, error=function(e) {
    message("Deleting task as submission failed")
    context::task_delete(res)
  })

  ## TODO: Getting a list of the task groups out of the db is not
  ## really possible at the moment because it requires having a proper
  ## hash access interface to the database, which with RDS storage we
  ## don't really have?
  context::context_db(obj)$set(group, res$id, "task_groups")
  task_bundle(obj, res$id, group, names(X))
}
