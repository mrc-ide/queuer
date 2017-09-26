# queuer

[![Project Status: WIP - Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](http://www.repostatus.org/badges/latest/wip.svg)](http://www.repostatus.org/#wip)
[![Travis-CI Build Status](https://travis-ci.org/mrc-ide/queuer.svg?branch=master)](https://travis-ci.org/mrc-ide/queuer)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/mrc-ide/queuer?branch=master&svg=true)](https://ci.appveyor.com/project/mrc-ide/queuer)
[![codecov.io](https://codecov.io/github/mrc-ide/queuer/coverage.svg?branch=master)](https://codecov.io/github/mrc-ide/queuer?branch=master)

> Queue Tasks

Queue tasks and then run them.

This package exists to make use of HPC systems where jobs are submitted to remote computers.  There is considerable overlap here with the `BatchJobs` package, and it might be better to target whatever extensibility that package has.

## Installation

(Relatively) stable version via `drat`:

```r
drat:::add("mrc-ide")
install.packages("queuer")
```
