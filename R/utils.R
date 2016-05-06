time_checker <- function(timeout, remaining=FALSE) {
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units="secs")
  if (remaining) {
    function() {
      as.double(timeout - (Sys.time() - t0), "secs")
    }
  } else {
    function() {
      Sys.time() - t0 > timeout
    }
  }
}

## Not necessarily the fastest, but it should do.
df_to_list <- function(x, use_names) {
  keep <- c("names", "class", "row.names")
  at <- attributes(x)
  attributes(x) <- at[intersect(names(at), keep)]
  ret <- unname(lapply(split(x, seq_len(nrow(x))), as.list))
  if (!use_names) {
    ret <- lapply(ret, unname)
  }
  if (is.character(at$row.names)) {
    names(ret) <- at$row.names
  }
  ret
}

progress_has_spin <- function() {
  packageVersion("progress") > numeric_version("1.0.2")
}

progress <- function(total, ..., show=TRUE, prefix="", fmt=NULL) {
  if (show) {
    if (is.null(fmt)) {
      fmt <- paste0(prefix,
                    if (progress_has_spin()) "(:spin) ",
                    "[:bar] :percent")
    }
    pb <- progress::progress_bar$new(fmt, total=total)
    function(len=1, ..., update=FALSE) {
      if (update) {
        invisible(pb$update(len, ...))
      } else {
        invisible(pb$tick(len, ...))
      }
    }
  } else {
    function(...) {}
  }
}

## Short-circuit apply; returns the index of the first element of x
## for which cond(x[[i]]) holds true.
scapply <- function(x, cond, no_match=NA_integer_) {
  for (i in seq_along(x)) {
    if (isTRUE(cond(x[[i]]))) {
      return(i)
    }
  }
  no_match
}

trim_id <- function(x, head=7, tail=0) {
  n <- nchar(x)
  i <- (head + tail) < (n - 3)
  if (any(i)) {
    x[i] <- sprintf("%s...%s",
                    substr(x[i], 1, head),
                    substr(x[i], n - tail + 1, n))
  }
  x
}

## The R time objects really want me poke my eyes out.  Perhaps there
## is a better way of doing this?  Who knows?
unlist_times <- function(x) {
  if (length(x) == 0L) {
    structure(numeric(0), class=c("POSIXct", "POSIXt"), tzone="UTC")
  } else {
    tmp <- unlist(x)
    attributes(tmp) <- attributes(x[[1L]])
    tmp
  }
}
