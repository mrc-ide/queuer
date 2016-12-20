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
  context::context_log_start()
  on.exit(context::context_log_stop())
  context::context_log("worker", Sys.getpid())
  obj <- queue_local(context_id, root, log = TRUE)
  if (loop) {
    obj$run_loop()
  } else {
    obj$run_all()
  }
  context::context_log("quitting", Sys.getpid())
}

write_queue_local_worker <- function(root) {
  queue_local_worker <- file.path(root, "bin", "queue_local_worker")
  if (!file.exists(queue_local_worker)) {
    txt <- c(
      readLines(file.path(root, "bin", "bootstrap")),
      readLines(system.file("bin/queue_local_worker", package = "queuer")))
    writeLines(txt, queue_local_worker)
    Sys.chmod(queue_local_worker, "0755")
  }
}
