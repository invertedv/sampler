## Sampler
[![Go Report Card](https://goreportcard.com/badge/github.com/invertedv/sampler)](https://goreportcard.com/report/github.com/invertedv/sampler)
[![godoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/mod/github.com/invertedv/sampler?tab=overview)

This package does two things:

1. Produces strats.  That is, it produces a table of row counts for each stratum of an input source. The strata are
defined by the user.
2. Creates an output table from the input source that is balanced along the desired strata.

### Sampling Procedure

The goal is to generate a sample with an equal number of rows for each stratum.  A sample rate for each stratum is
determined to achieve this.  If each stratum has sufficient rows, then we're done.  To the extent some strata have
insufficent rows, then the total sample will be short.  Also, the sample won't be exactly balanced.  To achieve
balance one would have to do one of these:

- Target a sample per strata that is equal to the size of the smallest stratum;
- Resample strata with insufficient rows

Practically, the first is not an option as often there will be a stratum with very few rows. The second is tantamount
to up-weighting the small strata.  In that case, these observations may become influential observations.

Instead of these, this package adopts the philosophy that an approximate balance goes a long way to reducing the 
leverage of huge strata which is typical in data. The sampling algorithm used by sampler is:

1. At the start
     - desired sample = total sample desired / # of strata
     - stratum sample rate = min(desired sample / stratum size, sample rate cap)
     - stratum captured sample = stratum sample rate * stratum size
     - stratum free observations = stratum size - stratum captured sample 
2. Update
     - total sample desired = total sample desired - total captured
     - if this number is "small" stop.
     - stratum desired sample = total sample desired / # of strata with free observations
     - stratum sample rate = min(stratum desired sample + stratum previously captured sample / stratum size, sample rate cap)
     - stratum captured sample = stratum sample rate * stratum size
     - stratum free observations = stratum size - stratum captured sample 

Step 2 is repeated (iterations capped at 5) until the target sample size is achieved (within tolerance) or 
no strata have free observations.
