## TODO: obj->root?
task_bundle_create <- function(task_ids, obj, name = NULL, X = NULL,
                               overwrite = FALSE, homogeneous = NULL) {
  ## TODO: flag if the task is homogeneous; we do this by setting a
  ## flag homogeneous.  If NULL, then check by looking at the
  ## expressions of all tasks.  If TRUE or FALSE set as such.  This
  ## will impact things like function_name
  if (length(task_ids) < 1L) {
    stop("task_ids must be nonempty")
  }
  root <- context::context_root_get(obj)
  db <- root$db
  name <- create_bundle_name(name, overwrite, db)

  if (is.null(homogeneous)) {
    task_names <- context::task_function_name(task_ids, root)
    homogeneous <- length(unique(task_names)) == 1L
  }

  db$mset(name, list(task_ids, homogeneous, X),
          c("task_bundles", "task_bundles_homogeneous", "task_bundles_X"))
  task_bundle$new(name, root)
}


task_bundle <- R6::R6Class(
  "task_bundle",

  public = list(
    name = NULL,
    root = NULL,
    ids = NULL,
    tasks = NULL,
    names = NULL,
    done = NULL,
    X = NULL,
    db = NULL,
    homogeneous = NULL,

    initialize = function(name, root) {
      self$name <- name
      self$root <- context::context_root_get(root)
      self$db <- self$root$db

      task_ids <- self$db$get(name, "task_bundles")
      self$ids <- unname(task_ids)
      self$names <- names(task_ids)
      self$homogeneous <- self$db$get(name, "task_bundles_homogeneous")

      ## TODO: is there any reason why this needs access to the root,
      ## and not simply to the context (or even the db).

      ## This does not do a db read
      self$tasks <- setNames(lapply(
        task_ids, queuer_task$new, self$root, FALSE),
        task_ids)
      self$X <- self$db$get(name, "task_bundles_X")

      self$check()
    },

    check = function() {
      self$status()
      self$done
    },

    times = function(unit_elapsed = "secs") {
      context::task_times(self$ids, self$db, unit_elapsed)
    },

    results = function(partial = FALSE) {
      if (partial) {
        task_bundle_partial(self)
      } else {
        self$wait(0, 0, FALSE)
      }
    },

    wait = function(timeout = 60, time_poll = 1, progress = NULL) {
      task_bundle_wait(self, timeout, time_poll, progress)
    },

    status = function(named = TRUE) {
      ## TODO: Only need to check the undone ones here?
      ret <- context::task_status(self$ids, self$db, named)
      self$done <- setNames(!(ret %in% c("PENDING", "RUNNING", "ORPHAN")),
                            self$ids)
      ret
    },

    expr = function(locals = FALSE) {
      setNames(lapply(self$ids, context::task_expr, self$db, locals),
               self$ids)
    },

    ## This is disabled for now, because it's not really clear how
    ## best to run it.
    ##
    ## log = function() {
    ##   setNames(lapply(self$ids, context::task_log, self$root),
    ##            self$names)
    ## },

    function_name = function() {
      if (self$homogeneous) {
        context::task_function_name(self$ids[[1]], self$db)
      } else {
        NA_character_
      }
    }

    ## TODO: this is not enabled because it's problematic:
    ##
    ## * does not allow unsubmission
    ## * leaves an invalid pointer to the task bundle
    ##
    ## delete = function(tasks = FALSE) {
    ##   if (tasks) {
    ##     ## NOTE: This is not ideal because we can't undelete the tasks
    ##     ## here unless the 'obj' that we're composed with will support
    ##     ## unsubmission.
    ##     context::task_delete(self$ids, self$db)
    ##   }
    ##   task_bundle_delete(self$name, self$db)
    ## }
  ))

task_bundle_list <- function(db) {
  db$list("task_bundles")
}

task_bundle_info <- function(obj) {
  bundles <- task_bundle_list(obj$db)
  db <- obj$db

  task_ids <- db$mget(bundles, "task_bundles")
  task_id1 <- vcapply(task_ids, "[[", 1L)

  task_time_sub <- context::task_times(task_id1, db, sorted = FALSE)$submitted
  task_function <- context::task_function_name(task_id1, obj$db)
  i <- order(task_time_sub)

  ret <- data.frame(name = bundles[i],
                    "function" = task_function[i],
                    length = lengths(task_ids)[i],
                    created = task_time_sub[i],
                    stringsAsFactors = FALSE,
                    check.names = FALSE)
  rownames(ret) <- NULL
  ret
}

task_bundle_wait <- function(bundle, timeout, time_poll, progress) {
  ## NOTE: For Redis we'd probably implement this differently due to
  ## the availability of BLPOP.  Note that would require *nonzero
  ## integer* time_poll though, and that 0.1 would become 0 which
  ## would block forever.
  task_ids <- bundle$ids
  done <- bundle$check()
  db <- bundle$db

  if (timeout == 0 && !all(done)) {
    stop(sprintf("Tasks not yet completed (%d / %d tasks pending)",
                 sum(!done), length(done)))
  }

  ## Immediately collect all completed results:
  results <- setNames(vector("list", length(task_ids)), task_ids)
  cleanup <- function(results) {
    bundle$done <- done
    setNames(results, bundle$names)
  }

  if (any(done)) {
    results[done] <- db$mget(task_ids[done], "task_results")
  }
  if (all(done)) {
    return(cleanup(results))
  }

  p <- progress_timeout(total = length(bundle$tasks), show = progress,
                        timeout = timeout)
  p(sum(done))
  time_poll <- min(time_poll, timeout)
  while (!all(done)) {
    res <- task_bundle_fetch1(db, task_ids[!done])
    if (is.null(res$id)) {
      ## This is put here so that we never abort while actively
      ## collecting jobs.
      if (p(0)) {
        ## Even though we're aborting, because bundles are a reference
        ## class, updating the done-ness should be done before failing
        ## here.
        bundle$done <- done
        p(clear = TRUE)
        stop(sprintf("Exceeded maximum time (%d / %d tasks pending)",
                     sum(!done), length(done)))
      }
      Sys.sleep(time_poll)
    } else {
      task_id <- res[[1]]
      result <- res[[2]]
      p(length(task_id))
      done[task_id] <- TRUE
      results[task_id] <- result
    }
  }
  cleanup(results)
}

task_bundle_partial <- function(bundle) {
  task_ids <- bundle$ids
  done <- bundle$check()
  results <- setNames(vector("list", length(task_ids)), task_ids)
  if (any(done)) {
    results[done] <- bundle$db$mget(task_ids[done], "task_results")
  }
  setNames(results, bundle$names)
}

## This is going to be something that a queue should provide and be
## willing to replace; something like a true blocking wait (Redis)
## will always be a lot nicer than filesystem polling.  Polling too
## quickly will cause filesystem overuse here.  Could do this with a
## growing timeout, perhaps.
task_bundle_fetch1 <- function(db, task_ids) {
  done <- db$exists(task_ids, "task_results")
  if (any(done)) {
    id <- task_ids[done]
    list(id = id, value = db$mget(id, "task_results"))
  } else {
    NULL
  }
}

task_bundle_delete <- function(name, db) {
  ns <- c("task_bundles", "task_bundles_homogeneous", "task_bundles_X")
  db$del(name, ns)
}

create_bundle_name <- function(name, overwrite, db) {
  if (is.null(name)) {
    repeat {
      name <- ids::adjective_animal()
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
