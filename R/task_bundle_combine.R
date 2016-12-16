##' Combine two or more task bundles
##'
##' For now task bundles must have the same function to be combined.
##' @title Combine task bundles
##' @param ... Any number of task bundles
##'
##' @param bundles A list of bundles (used in place of \code{...} and
##'   probably more useful for programming).
##'
##' @inheritParams task_bundle_create
##' @export
task_bundle_combine <- function(..., bundles = list(...),
                                name = NULL, overwrite = FALSE) {
  if (length(bundles) == 0L) {
    stop("Provide at least one task bundle")
  }
  names(bundles) <- NULL

  ok <- vlapply(bundles, inherits, "task_bundle")
  if (any(!ok)) {
    stop("All elements of ... or bundles must be task_bundle objects")
  }

  ## Check that the functions of each bundle job are the same.
  ##
  ## NOTE: we should probably check here (somewhere!) that the the
  ## task bundles are all composed of things that have the same names
  ## within a bundle.
  fns <- vcapply(bundles, function(x) x$function_name())
  if (length(unique(fns)) != 1L) {
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
    X <- do.call("rbind", X)
  } else {
    if (any(is_df)) {
      X[is_df] <- lapply(X[is_df], df_to_list)
    }
    X <- unlist(X, FALSE)
  }

  task_bundle_create(bundles[[1]], task_ids, name, X, overwrite)
}
