## Shorter timeout here so that things don't fail too miserably.  Note
## that the timeout is _bidirectional_; so the worker will fail if the
## timeout is reached.
start_cluster <- function(n=1, ..., timeout=60) {
  Sys.setenv(R_TESTS="")
  cl <- parallel::makeCluster(n, "PSOCK", ..., timeout=timeout)
  withCallingHandlers({
    parallel::clusterEvalQ(cl, {
      loadNamespace("queuer")
      attach(getNamespace("queuer"), name=paste0("package:queuer"))
    })
    attr(cl, "pid") <- unlist(parallel::clusterCall(cl, Sys.getpid))
  },
  error=function(e) parallel::stopCluster(cl))
  cl
}

start_cluster <- function(root, context_id, n=1, timeout=60, ...) {
  Sys.setenv(R_TESTS="")
  cl <- parallel::makeCluster(n, "PSOCK", ..., timeout=timeout)
  on.exit(parallel::stopCluster(cl))

  parallel::clusterEvalQ(cl, {
    options(warn=1)
    loadNamespace("queuer")
    ## attach(getNamespace("queuer"), name=paste0("package:queuer"))
  })
  pid <- unlist(parallel::clusterCall(cl, Sys.getpid))

  ret <- list(
    cl=cl,
    pid=pid,
    exists=function() {
      pid_exists(pid)
    },
    kill=function() {
      pskill(pid)
    },
    stop=function() {
      parallel::stopCluster(cl)
    },
    send_call=function(...) {
      for (i in seq_along(cl)) {
        parallel:::sendCall(cl[[i]], ...)
      }
    },
    recieve=function(...) {
      ret <- vector("list", length(cl))
      for (i in seq_along(cl)) {
        ret[[i]] <- parallel:::recvResult(cl[[i]], ...)
      }
      ret
    },
    can_accept_call=function(timeout=0) {
      socketSelect(lapply(cl, function(x) x$con), TRUE, timeout)
    },
    can_return_result=function(timeout=0) {
      socketSelect(lapply(cl, function(x) x$con), FALSE, timeout)
    }
  )
  on.exit()
  ret
}

PSKILL_SUCCESS <- tools::pskill(Sys.getpid(), 0)
pid_exists <- function(pid) {
  tools::pskill(pid, 0) == PSKILL_SUCCESS
}

empty_named_character <- function() {
  setNames(character(0), character(0))
}
