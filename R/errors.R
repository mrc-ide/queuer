## TODO: should this be in context?
UnfetchableTask <- function(task_id, task_status) {
  structure(list(task_id=task_id,
                 task_status=task_status),
            class=c("UnfetchableTask", "error", "condition"))
}
