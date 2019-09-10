fifo_thor <- function(path) {
  R6_fifo_thor$new(path)
}


R6_fifo_thor <- R6::R6Class(
  "fifo_thor",
  cloneable = FALSE,

  private = list(
    mdb = NULL,
    key = NULL,
    split = function(x) {
      strsplit(x %||% "", "\r", fixed = TRUE)[[1]]
    },

    join = function(a, b = NULL) {
      paste(c(a, b), collapse = "\r")
    }
  ),

  public = list(
    initialize = function(path, key = "queue") {
      loadNamespace("thor")
      private$mdb <- thor::mdb_env(path)
      private$key <- key
    },

    read = function() {
      private$split(private$mdb$get(private$key, FALSE))
   },

    push = function(x) {
      private$mdb$with_transaction(function(txn) {
        prev <- private$split(txn$get(private$key, FALSE))
        txn$put(private$key, private$join(prev, x))
        invisible(length(prev) + length(x))
      }, write = TRUE)
    },

    pop = function() {
      private$mdb$with_transaction(function(txn) {
        queue <- private$split(txn$get(private$key, FALSE))
        if (length(queue) > 0L) {
          txn$put(private$key, private$join(queue[-1L]))
          queue[[1L]]
        } else {
          NULL
        }
      }, write = TRUE)
    },

    drop = function(x) {
      private$mdb$with_transaction(function(txn) {
        queue <- private$split(txn$get(private$key, FALSE))
        present <- x %in% queue
        if (any(present)) {
          txn$put(private$key, private$join(setdiff(queue, x)))
        }
        invisible(present)
      }, write = TRUE)
    }
  ))
