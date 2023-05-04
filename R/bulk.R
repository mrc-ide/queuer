qlapply <- function(obj, private, X, FUN, ...,
                    envir = parent.frame(),
                    timeout = 0, time_poll = 1, progress = NULL,
                    name = NULL, overwrite = FALSE) {
  ## TODO: The dots here are going to cause grief at some point.  I
  ## may need a more robust way of passing additional arguments in,
  ## but not sure what that looks like...
  enqueue_bulk(obj, private, X, FUN, ..., do_call = FALSE,
               timeout = timeout, time_poll = time_poll,
               progress = progress, name = name,
               envir = envir, overwrite = overwrite)
}

## A downside of the current treatment of dots is there are quite a
## few arguments on the RHS of it; if a function uses any of these
## they're not going to be allowed access to them.  Usually this seems
## solved by something like progress. = TRUE but I think that looks
## horrid.  So for now leave it as-is and we'll see what happens.
##
## TODO: Consider allowing DOTS as an argument itself.

enqueue_bulk <- function(obj, private, X, FUN, ..., do_call = TRUE,
                         envir = parent.frame(),
                         timeout = 0, time_poll = 1, progress = NULL,
                         name = NULL, use_names = TRUE,
                         overwrite = FALSE, depends_on = NULL) {
  obj <- enqueue_bulk_submit(obj, private, X, FUN, ..., do_call = do_call,
                             envir = envir, progress = progress, name = name,
                             use_names = use_names, overwrite = overwrite, depends_on = depends_on)
  if (timeout > 0) {
    ## TODO: this is possibly going to change as interrupt changes in
    ## current R-devel (as of 3.3.x)
    tryCatch(obj$wait(timeout, time_poll, progress),
             interrupt = function(e) obj)
  } else {
    obj
  }
}

enqueue_bulk_submit <- function(obj, private, X, FUN, ..., DOTS = NULL,
                                do_call = FALSE,
                                envir = parent.frame(), progress = NULL,
                                name = NULL, use_names = TRUE,
                                overwrite = FALSE, depends_on = NULL) {
  ## TODO: If I push this to *only* be a method, then the assertion is
  ## not needed.
  if (!inherits(obj, "queue_base")) {
    stop("'obj' must be a queue object (inheriting from queue_base)")
  }

  name <- create_bundle_name(name, overwrite, obj$context$db)

  obj$initialize_context()
  fun_dat <- match_fun_queue(FUN, envir, obj$context$envir)
  FUN <- fun_dat$name_symbol %||% fun_dat$value

  ## It is important not to use list(...) here and instead capture the
  ## symbols.  Otherwise later when we print the expression bad things
  ## will happen!
  if (is.null(DOTS)) {
    DOTS <- lapply(lazyeval::lazy_dots(...), "[[", "expr")
  }
  ids <- context::bulk_task_save(X, FUN, obj$context, DOTS,
                                 do_call, use_names, envir, depends_on)

  message(sprintf("submitting %s tasks", length(ids)))
  private$submit_or_delete(ids, names(ids))

  task_bundle_create(ids, obj, name, X, overwrite = TRUE, homogeneous = TRUE)
}

## This does the necessary wrangling of argument lengths and orientation
mapply_X <- function(...) {
  dots <- list(...)
  len <- lengths(dots)
  ul <- unique(len)

  if (length(ul) == 2L && min(ul) == 1L) {
    n <- max(len)
    dots[len == 1L] <- lapply(dots[len == 1L], rep_len, n)
    ul <- n
  } else if (length(ul) != 1L) {
    stop("Every element of '...' must have the same length (or 1)")
  }

  lapply(seq_len(ul), function(i) lapply(dots, "[[", i))
}
