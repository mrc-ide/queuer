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
##' @param name Group name
##'
##' @param X Metadata to associate with the tasks.  This is used by
##'   the bulk interface (\code{\link{qlapply}} and
##'   \code{\link{enqueue_bulk}} to associate the first argument with
##'   the bundle).
##'
##' @param overwrite Logical indicating if an existing bundle with the
##'   same name should be overwritten.  If \code{FALSE} and a bundle
##'   with this name already exists, an error will be thrown.
##'
##' @export
##' @rdname task_bundle
task_bundle_create <- function(obj, task_ids=NULL, name=NULL, X=NULL,
                               overwrite=FALSE) {
  db <- context::context_db(obj)
  name <- create_bundle_name(name, overwrite, db)
  db$set(name, task_ids, "task_bundles")
  db$set(name, X, "task_bundles_X")
  task_bundle_get(obj, name)
}

##' @export
##' @rdname task_bundle
task_bundle_get <- function(obj, name) {
  .R6_task_bundle$new(obj, name)
}

.R6_task_bundle <- R6::R6Class(
  "task_bundle",

  public=list(
    db=NULL,
    tasks=NULL,
    name=NULL,
    names=NULL,
    ids=NULL,
    done=NULL,
    X=NULL,

    initialize=function(obj, name) {
      self$db <- context::context_db(obj)
      task_ids <- self$db$get(name, "task_bundles")
      self$name <- name
      self$tasks <- setNames(lapply(task_ids, task, obj=obj), task_ids)

      self$ids <- unname(task_ids)
      self$names <- names(task_ids)
      self$X <- self$db$get(name, "task_bundles_X")
      self$check()
    },

    status=function(named=TRUE) {
      ## TODO: Only need to check the undone ones here...
      ret <- tasks_status(self, self$ids, named=named)
      self$done <- setNames(!(ret %in% c("PENDING", "RUNNING", "ORPHAN")),
                            self$ids)
      ret
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
      self$status()
      self$done
    }
    ## TODO: delete(), overview()
  ))

task_bundles_list <- function(obj) {
  context::context_db(obj)$list("task_bundles")
}

task_bundles_info <- function(obj) {
  bundles <- task_bundles_list(obj)
  db <- context::context_db(obj)

  task_ids <- lapply(bundles, db$get, "task_bundles")
  task_times <-
    unlist_times(lapply(task_ids, function(x) db$get(x[[1L]], "task_time_sub")))

  i <- order(task_times)
  data.frame(name=bundles[i],
             length=lengths(task_ids[i]),
             created=unlist_times(task_times[i]),
             stringsAsFactors=FALSE)
}

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

##' @importFrom ids aa
create_bundle_name <- function(name, overwrite, db) {
  if (is.null(name)) {
    repeat {
      name <- ids::aa(1)()
      if (!db$exists(name, "task_bundles")) {
        break
      }
    }
    message(sprintf("Creating bundle: '%s'", name))
  } else if (!overwrite && db$exists(name, "task_bundles")) {
    stop("Task bundle already exists: ", name)
  }
  name
}
