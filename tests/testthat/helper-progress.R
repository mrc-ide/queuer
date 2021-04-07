get_output <- function(...) {
  type <- "output"

  cleanup <- function() {
    unlink(tmp)
    sink(NULL, type = type)
  }

  tmp <- tempfile()
  on.exit(cleanup())

  sink(tmp, type = type)
  force(...)

  ## Windows has some strange readBin and sink interplay,
  ## so we need to remove the sink before reading the file
  sink(NULL, type = type)
  x <- readBin(tmp, raw(0), n = file.info(tmp)$size)
  unlink(tmp)

  ## No cleanup is needed any more
  on.exit(force(1))

  rawToChar(x)
}

run_progress_timeout <- function(total, timeout, time_poll, n = total, ...,
                                 width = 40) {
  p <- progress_timeout(total, timeout, ...,
                        show_after = 0, stream = stdout(),
                        force = TRUE, width = width)
  expired <- FALSE
  done <- FALSE
  p(0)
  for (i in seq_len(n)) {
    Sys.sleep(time_poll)
    if (p()) {
      expired <- TRUE
      break
    }
  }
  done <- i == n
  list(done = done, expired = expired)
}

run_progress_timeout_single <- function(timeout, time_poll, n, label = "thing",
                                        ...) {
  p <- progress_timeout(NULL, timeout, label = label, ...,
                        show_after = 0, stream = stdout(),
                        force = TRUE, width = 50)
  expired <- FALSE
  done <- FALSE
  p(0)
  for (i in seq_len(n)) {
    Sys.sleep(time_poll)
    if (p()) {
      expired <- TRUE
      break
    }
  }
  done <- i == n
  if (done) {
    p(clear = TRUE)
  }
  list(done = done, expired = expired)
}

win_newline <- function(..., collapse = NULL) {
  x <- paste0(...)
  if (.Platform$OS.type == "windows") {
    x <- gsub("\n", "\r\n", x, fixed = TRUE)
  }
  x
}

prepare_expected <- function(x) {
  x <- gsub("\\", "\\\\", x, fixed = TRUE)
  x <- paste0("\\r", c(strsplit(x, "\r")[[1]][-1], ""))
  writeLines(
    sprintf("win_newline(\n%s\n)",
            paste(sprintf('  "%s"', x), collapse = ",\n")))
}

PROGRESS_RESTART <- 3

is_windows <- function() {
  tolower(Sys.info()[["sysname"]]) == "windows"
}


on_ci <- function() {
  isTRUE(as.logical(Sys.getenv("CI")))
}
