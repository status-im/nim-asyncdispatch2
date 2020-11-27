#
#                 Chronos Rate Limiter
#              (c) Copyright 2020-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

## This module implements rate limiting algorithm which is heavy inspired by
## article "How we built rate limiting capable of scaling to millions of
## domains" by Julien Desgats.
## https://blog.cloudflare.com/counting-things-a-lot-of-different-things/
##
## This is example on how to use rate limit for specific resource only:
##
##  .. code-block::nim
##    # We are creating RateCounter for resource with rate limiting 5 req/sec.
##    var counter = RateCounter.init(1.seconds, 5)
##
##    var sucess = 0
##    var failed = 0
##    for i in 0 ..< 100:
##      if counter.checkRate():
##        inc(succeed)
##      else:
##        inc(failed)
##    echo "Number of successfull requests = ", succeed
##    echo "Number of failed requests = ", failed
##
## When using two counter you can easily apply per-resource and per-consumer
## rate limits at once, but beware using ``checkRate()`` procedure inside of
## ``if`` statement.
##
##  .. code-block::nim
##    if counter1.checkRate() and counter2.checkRate():
##      echo "Request received"
##
## If initial ``counter1.checkRate()`` returns ``false`` then
## second statement ``counter2.checkRate()`` will not be executed at all and
## so access will not be properly counted.
##
## To avoid this problem get results of checks before checking it in ``if``
## statement.
##
##  .. code-block::nim
##    let check1 = counter1.checkRate()
##    let check2 = counter2.checkRate()
##    if check1 and check2:
##      echo "Request received"
##
import timer
# Exporting timer to allow consumers to use timer procedures.
export timer

type
  RateCounter* = object
    period*: int64
    maxRate*: float
    start*: int64
    requests*: array[2, float]

template nanoseconds(a: Moment): int64 =
  (a - Moment()).nanoseconds()

proc init*(t: typedesc[RateCounter], period: Duration,
           count: uint64): RateCounter =
  ## Create new rate counter for resource with period of time ``period`` and
  ## number of requests ``count``.
  ##
  ## For example to rate limit access to resource for 10 requests per minute:
  ##  .. code-block::nim
  ##    RateCounter.init(1.minutes, 10'u64)
  ##
  ## or to rate limit access to resource for 1_000 requests per 10 seconds:
  ##  .. code-block::nim
  ##    RateCounter.init(10.seconds, 1_000'u64)
  ##
  doAssert(period != ZeroDuration, "`period` could not be zero")
  doAssert(count != 0, "`count` could not be zero")
  RateCounter(period: period.nanoseconds(), maxRate: float(count))

proc checkRate*(ctr: var RateCounter, moment = Moment.now().nanoseconds): bool =
  ## Returns ``true`` if resource is able to accept requests, e.g. rate limit
  ## ``ctr`` is not yet reached.
  if ctr.maxRate <= 0.0 or ctr.period <= 0'i64:
    ## This could happen if ``rc`` was not initialized properly.
    true
  else:
    let current = moment div ctr.period
    var index = current - ctr.start
    let rate =
      if index == 0:
        # Request still in first counter
        ctr.requests[0] += 1.0
        ctr.requests[0]
      elif index == 1:
        # Request in second counter
        ctr.requests[1] += 1.0
        let modulo = moment mod ctr.period
        ctr.requests[0] * (float(ctr.period - modulo) / float(ctr.period)) +
          ctr.requests[1]
      elif index == 2:
        # Request passed second counter
        inc(ctr.start)
        ctr.requests[0] = ctr.requests[1]
        ctr.requests[1] = 1.0
        let modulo = moment mod ctr.period
        ctr.requests[0] * (float(ctr.period - modulo) / float(ctr.period)) +
          ctr.requests[1]
      else:
        # Request after big timeout
        ctr.start = current
        ctr.requests[0] = 1.0
        ctr.requests[1] = 0.0
        ctr.requests[0]
    (rate <= ctr.maxRate)
