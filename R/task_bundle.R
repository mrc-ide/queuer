##' Create a task bundle.  Generally these are not created manually,
##' but this page serves to document what task bundles are and the
##' methods that they have.
##'
##' A task bundle exists to group together tasks that are related.  It
##' is possible for a task to belong to multiple bundles.
##'
##' @title Create a task bundle
##'
##' @param obj An observer or queue object; something that can be
##'   passed through to \code{\link{context_db}}.
##'
##' @param tasks A list of tasks.
##'
##' @param group Group name
##'
##' @param names Optional vector of names to label output with.
##'
##' @export
task_bundle <- function(obj, task_ids=NULL, group=NULL, names=NULL) {
  new <- is.null(group)
  if (new) {
    group <- create_group(NULL, FALSE)
  }
  if (is.null(task_ids)) {
    if (new) {
      stop("task_ids cannot be NULL for nonexistant group")
    }
    task_ids <- tryCatch(
      context::context_db(obj)$get(grp, "task_groups"),
      error=function(e) stop(sprintf("No such group %s", group)))
  }
  .R6_task_bundle$new(obj, task_ids, group, names)
}

.R6_task_bundle <- R6::R6Class(
  "task_bundle",

  public=list(
    db=NULL,
    tasks=NULL,
    group=NULL,
    names=NULL,
    ids=NULL,
    done=NULL,

    initialize=function(obj, task_ids, group, names) {
      self$db <- context::context_db(obj)
      self$tasks <- setNames(lapply(task_ids, obj$task_get), task_ids)
      self$group <- group
      self$names <- NULL
      self$ids <- task_ids
      self$check()
    },

    status=function(named=TRUE) {
      tasks_status(self, self$ids, named=named)
    },
    times=function(unit_elapsed="secs") {
      tasks_times(self, self$ids, unit_elapsed)
    },
    results=function(partial=FALSE) {
      if (partial) {
        task_bundle_partial(self)
      } else {
        self$wait(0, 0, FALSE)
      }
    },
    wait=function(timeout=60, time_poll=1, progress_bar=TRUE) {
      task_bundle_wait(self, timeout, time_poll, progress_bar)
    },
    check=function() {
      status <- self$status()
      self$done <-
        !(status == "PENDING" | status == "RUNNING" | status == "ORPHAN")
      self$done
    }
    ## TODO: delete(), overview()
  ))

task_bundle_wait <- function(bundle, timeout, time_poll, progress_bar) {
  ## NOTE: For Redis we'd probably implement this differently due to
  ## the availability of BLPOP.  Note that would require *nonzero
  ## integer* time_poll though, and that 0.1 would become 0 which
  ## would block forever.
  task_ids <- bundle$ids
  done <- bundle$check()

  ## Immediately collect all completed results:
  results <- setNames(vector("list", length(task_ids)), task_ids)
  if (any(done)) {
    results[done] <- lapply(bundle$tasks[done], function(t) t$result())
  }

  cleanup <- function(results) {
    setNames(results, bundle$names)
  }
  if (all(done)) {
    return(cleanup(results))
  } else if (timeout == 0) {
    stop("Tasks not yet completed; can't be immediately returned")
  }

  p <- progress(total=length(bundle$tasks), show=progress_bar)
  p(sum(done))
  i <- 1L
  times_up <- time_checker(timeout)
  db <- context::context_db(bundle)
  while (!all(done)) {
    if (times_up()) {
      bundle$done <- done
      if (progress_bar) {
        message()
      }
      stop(sprintf("Exceeded maximum time (%d / %d tasks pending)",
                   sum(!done), length(done)))
    }
    res <- task_bundle_fetch1(db, task_ids[!done], time_poll)
    if (is.null(res$id)) {
      p(0)
    } else {
      p(1)
      task_id <- res[[1]]
      result <- res[[2]]
      done[[task_id]] <- TRUE
      ## NOTE: This conditional is needed to avoid deleting the
      ## element in results if we get a NULL result.
      if (!is.null(result)) {
        results[[task_id]] <- result
      }
    }
  }
  cleanup(results)
}

task_bundle_partial <- function(bundle) {
  task_ids <- bundle$ids
  done <- bundle$check()
  results <- setNames(vector("list", length(task_ids)), task_ids)
  if (any(done)) {
    results[done] <- lapply(bundle$tasks[done], function(t) t$result())
  }
  setNames(results, bundle$names)
}

## This is going to be something that a queue should provide and be
## willing to replace; something like a true blocking wait (Redis)
## will always be a lot nicer than filesystem polling.  Polling too
## quickly will cause filesystem overuse here.  Could do this with a
## growing timeout, perhaps.
task_bundle_fetch1 <- function(db, task_ids, timeout) {
  ## TODO: ideally exists() would be vectorisable.  That would require
  ## the underlying driver to express some traits about what it can
  ## do.
  ##
  ## In the absence of being able to do them in bulk, it might be
  ## worth explicitly looping over the set with a break?
  done <- vapply(task_ids, db$exists, logical(1), "task_results",
                 USE.NAMES=FALSE)
  if (any(done)) {
    id <- task_ids[[which(done)[[1]]]]
    list(id=id, value=db$get(id, "task_results"))
  } else {
    Sys.sleep(timeout)
    NULL
  }
}
