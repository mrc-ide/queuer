# queuer

> Queue Tasks

Queue tasks to number of backends.

This package exists to make use of HPC systems where jobs are submitted to remote computers.  There is considerable overlap here with the `BatchJobs` package, and it might be better to target whatever extensibility that package has.

This package is optimised for use with [`rrqueue`](https://github.com/traitecoevo/rrqueue) and [`didewin`](https://github.com/dide-tools/didewin).  A probable requirement will be that the underlying backend should use [`context`](https://github.com/dide-win/context) for data storage and session control.
