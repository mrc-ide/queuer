## The whole disaster zone here is up for revision at some point.
## Basically the idea is to check before running that we can find the
## function in the worker and to make sure that the worker version and
## our version are the same.  I'll resketch this at some point.

## This is also needed to detect if the function needs sending with
## the job data to the queue.  This is the case when anonymous
## functions are sent, for example.

## I'll pull out a few entrypoints here, write some tests, and start
## refactoring I think.

## TODO: match_fun -> find_fun? locate_fun?

match_fun <- function(fun, envir) {
  fun_lazy <- lazyeval::lazy(fun, envir)
  if (is.character(fun)) {
    match_fun_name(fun, envir)
  } else if (is.symbol(fun) ||
             is.call(fun) && identical(fun[[1L]], quote(`::`))) {
    ## TODO: handle the quoted :: case without going via deparse.
    match_fun_name(deparse(fun), envir)
  } else if (is.primitive(fun)) {
    ## This used to work with lazyeval, but there's been a breaking
    ## change.  Yay, but not terribly surprising.  As a result, we
    ## need to search through base and try and find it, which can be
    ## quite slow.
    if (is.symbol(fun_lazy$expr)) {
      ## This line will not trigger until lazyeval is fixed:
      match_fun_name(deparse(fun_lazy$expr), envir) # nocov
    } else {
      ## workaround for lazyeval 0.2:
      match_fun_primitive(fun)
    }
  } else if (is.function(fun)) {
    if (is_function_definition(fun_lazy$expr)) {
      match_fun_sanitise(NULL, fun_lazy$env, fun)
    } else {
      match_fun_value(fun, envir)
    }
  } else {
    stop("Invalid input")
  }
}

## TODO: For functions that are not found, we can try and serialise
## them I think.  That's going to work best for things like
## `function(x) bar(x, a, b)` but it might be hard to pick up all the
## locals without doing some serious messing around.

match_fun_queue <- function(fun, envir = parent.frame(),
                            envir_queue = .GlobalEnv) {
  dat <- match_fun(fun, envir)
  if (is.null(dat$namespace) && !identical(envir, envir_queue)) {
    ## Now, try to find the function in the queue's environment:
    name <- dat$name
    if (!is.null(name)) {
      ok <-
        exists_function(name, envir_queue) &&
        identical(deparse(dat$envir[[name]]),
                  deparse(get(name, envir_queue, mode = "function")))
      if (!ok && !is.null(dat$name)) {
        dat["name"] <- list(NULL)
      }
    }
  }

  if (is.null(dat$name)) {
    dat["name_symbol"] <- list(NULL)
  } else if (is.null(dat$namespace)) {
    dat$name_symbol <- as.name(dat[[2]])
  } else {
    dat$name_symbol <- call("::", as.name(dat[[1]]), as.name(dat[[2]]))
  }

  dat
}

## TODO: consider `<global>::` and `<local>::` as special names?
## NOTE: This differs from match_fun_symbol because it allows skipping
## up the search path to identify functions in specific parts of the
## search path.  If a namespace-qualified value is given, we can
## ignore envir entirely.
match_fun_name <- function(str, envir) {
  if (has_namespace(str)) {
    ret  <- split_namespace(str)
    if (!exists_function_ns(ret[[2]], ret[[1]])) {
      stop(sprintf("Did not find function '%s' in namespace '%s'",
                   ret[[2]], ret[[1]]))
    }
    match_fun_sanitise(ret[[2]], asNamespace(ret[[1]]))
  } else {
    name <- str
    fun_envir <- find_fun_by_name(name, envir)
    match_fun_sanitise(name, fun_envir)
  }
}

## This one is much harder and might take a while if it has to recurse
## through all functions.
match_fun_value <- function(fun, envir, stopat = .GlobalEnv) {
  nm <- environmentName(environment(fun))
  if (nzchar(nm)) {
    e <- if (nm == "R_GlobalEnv") .GlobalEnv else asNamespace(nm)
    name <- find_fun_in_envir(fun, e)
    if (!is.null(name)) {
      return(match_fun_sanitise(name, e))
    }
  }
  res <- find_fun_by_value(fun, envir, stopat)
  match_fun_sanitise(res$name, res$envir)
}

match_fun_primitive <- function(fun) {
  match_fun_value(fun, baseenv(), emptyenv())
}

## This is going to search back and find the location of a function by
## descending through environments recursively.
find_fun_by_name <- function(name, envir) {
  if (identical(envir, emptyenv())) {
    stop(sprintf("Did not find function '%s' in environment", name))
  }
  if (exists_function_here(name, envir)) {
    envir
  } else {
    find_fun_by_name(name, parent.env(envir))
  }
}

find_fun_by_value <- function(fun, envir, stopat = emptyenv()) {
  if (identical(envir, stopat)) {
    stop("Did not find function")
  }
  name <- find_fun_in_envir(fun, envir)
  if (!is.null(name)) {
    list(name = name, envir = envir)
  } else {
    find_fun_by_value(fun, parent.env(envir))
  }
}

## Determine the name of a function, given it's value and an
## environment to find it in.
find_fun_in_envir <- function(fun, envir) {
  pos <- ls(envir)
  i <- scapply(pos, function(x) identical(fun, envir[[x]]), NULL)
  if (is.null(i)) i else pos[[i]]
}

## TODO: might be worth also passing in 'envir' as the starting
## environment; then we can determine if we're looking at:
##   namespace
##   global env
##   given env
##   other env
## TODO: Might also return the environment here as a named list so
## that we can do some further faffing?
match_fun_sanitise <- function(name, fun_envir, value = NULL) {
  if (is.null(name)) {
    name <- ns <- NULL
  } else {
    ns <- environmentName(fun_envir)
    ## Don't treat the global environment specially here:
    if (identical(ns, "R_GlobalEnv")) {
      ns <- NULL
    } else if (nzchar(ns)) {
      ## Might be best here to treat all environments as non-namespace
      ## unless we get a 'package:' name?
      ns <- sub("^package:", "", ns)
    } else {
      ns <- NULL
    }
  }
  if (is.environment(fun_envir) && !isNamespace(fun_envir)) {
    ## NOTE: this might be overkill?
    fun_envir_name <- environmentName(fun_envir)
    if (grepl("^package:", fun_envir_name)) {
      fun_envir <- asNamespace(sub("^package:", "", fun_envir_name))
    }
  }
  list(namespace = ns,
       name = name,
       envir = fun_envir,
       value = value %||% get(name, fun_envir))
}

## Will be prone to false positives but worth a shot
has_namespace <- function(str) {
  ## TODO: Here, and split_namespace, this is do-able but requires
  ## some trickery so we can tell that the function is hidden.
  ## grepl(":::?", str, fixed = TRUE)
  grepl("::", str, fixed = TRUE)
}

split_namespace <- function(str) {
  ## res <- strsplit(str, ":::?", fixed = TRUE)[[1]]
  res <- strsplit(str, "::", fixed = TRUE)[[1]]
  if (length(res) != 2L) {
    stop(sprintf("Invalid namespace-qualified name '%s'", str))
  }
  res
}

exists_function <- function(name, envir) {
  exists(name, envir, mode = "function")
}
exists_function_here <- function(name, envir) {
  exists(name, envir, mode = "function", inherits = FALSE)
}
exists_function_ns <- function(name, ns) {
  if (ns %in% .packages()) {
    exists_function_here(name, getNamespace(ns))
  } else {
    FALSE
  }
}

is_function_definition <- function(x) {
  is.language(x) && is.recursive(x) && identical(x[[1L]], quote(`function`))
}
