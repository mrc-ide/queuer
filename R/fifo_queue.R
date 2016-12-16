## This is something that might be better in its own package.
##
## Dealing with types here is hard, so for now I will assume that this
## is going to be storing a set of *strings* (not objects, which could
## be accessed based on the string).

## Here, we might want to make this either "safe" or "unsafe"; use the
## db access regardless.  But it would probably be better to have the
## unsafe version use an in-memory queue (currently implemented
## naively).

## The different interfaces here could be dealt with by testing the
## type of the database server (doing the _r one when it's NULL).  For
## now leave this be.

## * For environment based storage, no need to lock because there
##   cannot be any contention.
##
## * For RDS based storage, use seagull to lock the database
##
## * For Redis, we can do everything atomically so it's really easy
##
## * For DBI based databases we need to do things atomically in a
##   transaction, but it could be done.  Worth getting right because
##   we'll be needing this at some point perhaps.
fifo_seagull <- function(db, key, namespace, lockfile, timeout) {
  loadNamespace("seagull")
  force(db)
  force(key)
  force(namespace)
  force(lockfile)
  force(timeout)

  read <- function() {
    tryCatch(db$get(key, namespace),
             KeyError = function(e) character(0))
  }
  write <- function(x) {
    db$set(key, x, namespace)
  }

  push <- function(x) {
    seagull::with_flock(lockfile, {
      tot <- c(read(), x)
      db$set(key, tot, namespace)
      invisible(length(tot))
    }, timeout = timeout)
  }

  pop <- function() {
    seagull::with_flock(lockfile, {
      queue <- read()
      if (length(queue) > 0L) {
        db$set(key, queue[-1L], namespace)
        queue[[1]]
      } else {
        NULL
      }
    }, timeout = timeout)
  }

  drop <- function(x) {
    seagull::with_flock(lockfile, {
      queue <- read()
      if (length(queue) > 0L) {
        db$set(key, setdiff(queue, x), namespace)
      }
      invisible(x %in% queue)
    }, timeout = timeout)
  }

  list(read = read,
       push = push,
       pop = pop,
       drop = drop)
}


## fifo_redis <- function(con, key, timeout) {
##   force(con)
##   force(key)
##   force(timeout)

##   read <- function() {
##     vcapply(con$LRANGE(key, 0, -1), identity)
##   }
##   push <- function(x) {
##     con$RPUSH(key, x)
##   }
##   pop <- function(x) {
##     con$LPOP(key)
##   }
##   drop <- function(x) {
##     ## This needs to be done in a LUA script.
##     stop("not yet implemented")
##   }

##   list(read = read,
##        push = push,
##        pop = pop,
##        drop = drop)
## }

## fifo_r <- function() {
##   data <- character()

##   read <- function() {
##     data
##   }
##   push <- function(x) {
##     data <<- c(data, x)
##     invisible(length(data))
##   }
##   pop <- function(x) {
##     ret <- data[[1L]]
##     data <<- data[-1L]
##     ret
##   }
##   drop <- function(x) {
##     data <<- setdiff(data, x)
##   }

##   list(read = read,
##        push = push,
##        pop = pop,
##        drop = drop)
## }

## TODO: I don't think that seagull is a good idea, necessarily.  It
## would be nicer to use RSQLite here (which does support concurrency)
## perhaps.
##
## It might be nice to think about a generalised distributed
## queue here that could be backed by different packages:
##
## * R in memory (single process)
## * seagull
## * SQLite
## * Redis
##
## these would all have different pros and cons and would help a
## bunch.
## stop("rethink the queue here")
