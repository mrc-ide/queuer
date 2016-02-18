# queuer

> Queue Tasks

Queue tasks to number of backends.

This package exists to make use of HPC systems where jobs are submitted to remote computers.  There is considerable overlap here with the `BatchJobs` package, and it might be better to target whatever extensibility that package has.

This package is optimised for use with [`rrqueue`](https://github.com/traitecoevo/rrqueue) and [`didewin`](https://github.com/dide-tools/didewin).  A probable requirement will be that the underlying backend should use [`context`](https://github.com/dide-win/context) for data storage and session control.

## Warning

**This package is currently under heavy development and many core features _will_ change over the next few weeks/months.  You're welcome to use it, but expect frequent and unadvertised breaking changes.**

## Installation

(Relatively) stable version via `drat`:

```r
drat:::add("richfitz")
install.packages("queuer")
```

Development versions via `devtools`:

```r
devtools::install_github(c(
  "gaborcsardi/progress",
  "dide-tools/context",
  "richfitz/queuer"))
```
