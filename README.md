# queuer

[![Project Status: WIP - Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](http://www.repostatus.org/badges/latest/wip.svg)](http://www.repostatus.org/#wip)
[![Travis-CI Build Status](https://travis-ci.org/richfitz/queuer.svg?branch=master)](https://travis-ci.org/richfitz/queuer)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/richfitz/queuer?branch=master&svg=true)](https://ci.appveyor.com/project/richfitz/queuer)
[![codecov.io](https://codecov.io/github/richfitz/queuer/coverage.svg?branch=master)](https://codecov.io/github/richfitz/queuer?branch=master)

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
