##' Combine two or more task bundles
##'
##' For now task bundles must have the same function to be combined.
##' @title Combine task bundles
##' @param ... Any number of task bundles
##'
##' @param bundles A list of bundles (used in place of \code{...} and
##'   probably more useful for programming).
##'
##' @param name Group name
##'
##' @param overwrite Logical indicating if an existing bundle with the
##'   same name should be overwritten.  If \code{FALSE} and a bundle
##'   with this name already exists, an error will be thrown.
##'
##' @export
task_bundle_combine <- function(..., bundles = list(...),
                                name = NULL, overwrite = FALSE) {
  if (length(bundles) < 2) {
    stop("Provide at least two task bundles")
  }
  names(bundles) <- NULL

  ok <- vlapply(bundles, inherits, "task_bundle")
  if (any(!ok)) {
    stop("All elements of ... or bundles must be task_bundle objects")
  }

  ## Check that the functions of each bundle job are the same.
  ##
  ## TODO: now that there is a 'homogeneous' flag in the bundles
  ## itself, we can probably do away with this.
  homogeneous <- all(vlapply(bundles, function(x) x$homogeneous))
  fns <- vcapply(bundles, function(x) x$function_name())
  if (homogeneous && length(unique(fns)) != 1L) {
    stop("task bundles must have same function to combine")
  }

  task_ids <- unlist(lapply(bundles, function(x) x$ids), FALSE, FALSE)

  named <- vlapply(bundles, function(x) !is.null(x$names))
  if (all(named)) {
    names(task_ids) <- unlist(lapply(bundles, function(x) x$names),
                              FALSE, FALSE)
  } else if (any(named)) {
    tmp <- lapply(bundles, function(x) x$names)
    tmp[!named] <- lapply(bundles[!named], function(x) rep("", length(x$ids)))
    names(task_ids) <- unlist(tmp, FALSE, FALSE)
  }

  X <- lapply(bundles, function(x) x$X)
  is_df <- vlapply(X, is.data.frame)
  if (all(is_df)) {
    if (length(unique(lapply(X, names))) != 1L) {
      stop("All bundle data.frames must have the same column names")
    }
    X <- do.call("rbind", X)
  } else if (any(is_df)) {
    stop("Can't combine these task bundles")
  } else {
    X <- unlist(X, FALSE)
  }

  task_bundle_create(task_ids, bundles[[1]]$root, name, X, overwrite,
                     homogeneous)
}
