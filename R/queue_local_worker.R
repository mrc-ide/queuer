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
  if (!context::context_log_start()) {
    on.exit(context::context_log_stop())
  }
  context::context_log("worker", Sys.getpid())
  obj <- queue_local(context_id, root, log = TRUE)
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

queue_local_worker_main_args <- function(args) {
  args <- context::parse_context_args(args, "queue_local_worker", 1:2)
  list(root = args$root,
       context_id = args$args[[1L]],
       loop = if (args$n == 2L) args$args[[2L]] else FALSE)
}

write_queue_local_worker <- function(root) {
  path <- context::context_root_get(root)$path
  context::write_context_script(path, "queue_local_worker",
                                "queuer:::queue_local_worker_main", 1:2)
}
