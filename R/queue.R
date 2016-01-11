## A downside of the current treatment of dots is there are quite a
## few arguments on the RHS of it; if a function uses any of these
## they're not going to be allowed access to them.  Usually this seems
## solved by something like progress_bar.=TRUE but I think that looks
## horrid.  So for now leave it as-is and we'll see what happens.
enqueue_bulk <- function(obj, X, FUN, ..., do.call=FALSE, group=NULL,
                         timeout=Inf, time_poll=1, progress_bar=TRUE,
                         env=parent.frame()) {
  obj <- enqueue_bulk_submit(obj, X, FUN, ...,
                             do.call=do.call, group=group, env=env)
  tryCatch(obj$wait(timeout, time_poll, progress_bar),
           interrupt=function(e) obj)
}

enqueue_bulk_submit <- function(obj, X, fun, ..., do.call=FALSE, group=NULL,
                                env=parent.frame()) {
  if (is.data.frame(X)) {
    X <- df_to_list(X) # TODO: PORT
  } else if (!is.list(X)) {
    stop("X must be a data.frame or list")
  }

  fun <- find_fun(FUN, env, obj) # TODO: PORT
  n <- length(X)

  tasks <- vector("list", n)
  group <- create_group(group) # TODO: PORT
  for (i in seq_len(n)) {
    if (do.call) {
      expr <- as.call(c(list(fun), X[[i]]))
    } else {
      expr <- as.call(list(fun, X[[i]]))
    }
    tasks[[i]] <- obj$enqueue_(expr, env)
  }

  task_bundle(obj, tasks, group, names(X))
}
