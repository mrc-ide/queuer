## TODO: (here, and queue_base, queue_local).  The actual queue object
## should take only take "root", rather than context, and we should
## attempt to build all contexts.  This is fairly well developed in
## rrqueue.

## TODO: Here it might be useful to indicate which workers are working
## on which tasks as they get them.  That requires a worker ID.  This
## probably goes into the queue_local constructor function and gets
## written to just after we pop something from the queue.  This is
## like the BRPOPLPUSH thing that Redis does but we won't be doing it
## atomically.
queue_local_worker <- function(root, context_id, loop) {
  context::context_log("worker", Sys.getpid())
  obj <- queue_local$new(context_id, root, log = TRUE)
  if (loop) {
    obj$run_loop()
  } else {
    obj$run_all()
  }
  context::context_log("quitting", Sys.getpid())
  invisible(NULL)
}


queue_local_worker_main <- function(args = commandArgs(TRUE)) {
  dat <- queue_local_worker_main_args(args)
  queue_local_worker(dat$root, dat$context_id, dat$loop)
}


queue_local_worker_main_args <- function(args = commandArgs(TRUE)) {
  usage <- "Usage:
queue_local_worker <root> <id> [<loop>]"
  nargs <- length(args)
  if (nargs < 2 || nargs > 3) {
    stop(usage, call. = FALSE)
  }
  list(root = args[[1]],
       context_id = args[[2]],
       loop = if (nargs == 3) as.logical(args[[3]]) else FALSE)
}


write_queue_local_worker <- function(root) {
  path <- context::context_root_get(root)$path
  code <- c("#!/usr/bin/env Rscript",
            "queuer:::queue_local_worker_main()")
  context:::write_script_exec(
    code, file.path(path, "bin", "queue_local_worker"))
}
