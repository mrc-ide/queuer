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
##'   along with each element of \code{X}.  Unfortunately, because of
##'   the huge number of options that these functions support, dots
##'   are going to be unreliable here (if you match any argument here
##'   the dots won't make it through to your function).  This function
##'   may adopt the (fairly ugly) convention used by \code{mapply} and
##'   support an explicit argument instead.  Extra unfortunately
##'   though, the dots arguments need some care so that they are
##'   evaluated as symbols.  So another option is that all arguments
##'   after \code{...} will acquire a leading dot.
##'
##' @param envir Environment to search for functions in.  This might change.
##'
##' @param timeout Time to wait for tasks to be returned.  The
##'   default, 0, will not block but will instead return a
##'   \code{task_bundle} object which can be used to inspect the task
##'   status.  Give a value greater than 0 (including \code{Inf}) to
##'   wait.  If you do wait, you can interrupt R at any time (with
##'   Ctrl-C or Esc depending on platform) and it will return the
##'   \code{task_bundle}.
##'
##' @param time_poll How often to check for task completion.  The
##'   default is every second.  This is an \emph{approximate} time and
##'   should be seen as a lower limit.
##'
##' @param progress Display a progress bar as tasks are polled.
##'
##' @param name Name for the task bundle.  If not provided a
##'   human-recognisable random name will be generated and printed to
##'   the console.
##'
##' @param overwrite If a task bundle name \code{name} exists already,
##'   should we overwrite it (see \code{\link{task_bundle_create}})?
##'   If \code{FALSE} (the default) we throw an error if it exists.
##'
##' @export
qlapply <- function(X, FUN, obj, ...,
                    envir = parent.frame(),
                    timeout = 0, time_poll = 1, progress = TRUE,
                    name = NULL, overwrite = FALSE) {
  ## TODO: The dots here are going to cause grief at some point.  I
  ## may need a more robust way of passing additional arguments in,
  ## but not sure what that looks like...
  enqueue_bulk(obj, X, FUN, ..., do_call = FALSE,
               timeout = timeout, time_poll = time_poll,
               progress = progress, name = name,
               envir = envir, overwrite = overwrite)
}

## A downside of the current treatment of dots is there are quite a
## few arguments on the RHS of it; if a function uses any of these
## they're not going to be allowed access to them.  Usually this seems
## solved by something like progress. = TRUE but I think that looks
## horrid.  So for now leave it as-is and we'll see what happens.
##
## TODO: Consider allowing DOTS as an argument itself.

##' @export
##' @rdname qlapply
##'
##' @param do_call If \code{TRUE}, rather than evaluating \code{FUN(x,
##'   ...)}, evaluate \code{FUN(x[1], x[2], ..., x[n], ...)} (where
##'   \code{x} is an element of \code{X}).
##'
##' @param use_names Only meaningful when \code{do_call} is
##'   \code{TRUE} and \code{X} is a \code{data.frame}, if
##'   \code{use_names = FALSE}, then names will be stripped off each row
##'   of the data.frame before the function call is composed.
enqueue_bulk <- function(obj, X, FUN, ..., do_call = TRUE,
                         envir = parent.frame(),
                         timeout = 0, time_poll = 1, progress = TRUE,
                         name = NULL, use_names = TRUE,
                         overwrite = FALSE) {
  obj <- enqueue_bulk_submit(obj, X, FUN, ..., do_call = do_call, envir = envir,
                             progress = progress, name = name,
                             use_names = use_names, overwrite = overwrite)
  if (timeout > 0) {
    ## TODO: this is possibly going to change as interrupt changes in
    ## current R-devel (as of 3.3.x)
    tryCatch(obj$wait(timeout, time_poll, progress),
             interrupt = function(e) obj)
  } else {
    obj
  }
}

enqueue_bulk_submit <- function(obj, X, FUN, ..., DOTS = NULL, do_call = FALSE,
                                envir = parent.frame(), progress = TRUE,
                                name = NULL, use_names = TRUE,
                                overwrite = FALSE) {
  ## TODO: If I push this to *only* be a method, then the assertion is
  ## not needed.
  if (!inherits(obj, "queue_base")) {
    stop("'obj' must be a queue object (inheriting from queue_base)")
  }

  name <- create_bundle_name(name, overwrite, obj$db)

  obj$initialize_context()
  fun_dat <- match_fun_queue(FUN, envir, obj$context_envir)
  FUN <- fun_dat$name_symbol %||% fun_dat$value

  ## It is important not to use list(...) here and instead capture the
  ## symbols.  Otherwise later when we print the expression bad things
  ## will happen!
  if (is.null(DOTS)) {
    DOTS <- lapply(lazyeval::lazy_dots(...), "[[", "expr")
  }
  ids <- context::task_bulk_save(X, FUN, obj$context, DOTS,
                                 do_call, use_names, envir)

  message(sprintf("submitting %s tasks", length(ids)))
  obj$submit_or_delete(ids, names(ids))

  task_bundle_create(ids, obj, name, X, overwrite = TRUE, homogeneous = TRUE)
}
