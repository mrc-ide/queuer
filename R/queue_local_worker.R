## TODO (here, and queue_base, queue_local).  The actual queue object
## should take only take "root", rather than context, and we should
## attempt to build all contexts.  This is fairly well developed in
## rrqueue.
queue_local_worker <- function(root, context_id=NULL, loop=TRUE, verbose=TRUE) {
  if (verbose) {
    context::context_log_start()
    on.exit(context::context_log_stop())
  }
  if (!file.exists(root)) {
    stop("Queue does not exist")
  }
  if (is.null(context_id)) {
    db <- context::context_db(root)
    context_id <- context::contexts_list(root)
    if (length(context_id) != 1L) {
      stop("Expected exactly one context")
    }
  } else {
    db <- NULL
  }

  ctx <- context::context_handle(root, context_id, db)
  obj <- queue_local(ctx)

  if (loop) {
    obj$run_loop()
  } else {
    obj$run_all()
  }
}
