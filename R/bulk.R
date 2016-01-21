## This file contains the "bulk" operations; these are where most
## users will probably directly interact.
qlapply <- function(X, FUN, obj, ...,
                    timeout=60, time_poll=1, progress_bar=TRUE) {
  res <- enqueue_bulk_submit(obj, X, FUN, ...)
  tryCatch(res$wait(timeout, time_poll, progress_bar),
           interrupt=function(e) res)
}

## A downside of the current treatment of dots is there are quite a
## few arguments on the RHS of it; if a function uses any of these
## they're not going to be allowed access to them.  Usually this seems
## solved by something like progress_bar.=TRUE but I think that looks
## horrid.  So for now leave it as-is and we'll see what happens.
enqueue_bulk <- function(obj, X, FUN, ..., do.call=FALSE, group=NULL,
                         timeout=Inf, time_poll=1, progress_bar=TRUE,
                         envir=parent.frame()) {
  obj <- enqueue_bulk_submit(obj, X, FUN, ...,
                             do.call=do.call, group=group, envir=envir)
  tryCatch(obj$wait(timeout, time_poll, progress_bar),
           interrupt=function(e) obj)
}

enqueue_bulk_submit <- function(obj, X, FUN, ..., do.call=FALSE,
                                envir=parent.frame(), verbose=FALSE) {
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
  group <- create_group(NULL, FALSE)
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

create_group <- function(group, verbose) {
  if (is.null(group)) {
    group <- context:::random_id()
    if (verbose) {
      message(sprintf("Creating group: '%s'", group))
    }
  }
  group
}
