#                Chronos Test Suite
#            (c) Copyright 2020-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import unittest, os
import ../chronos/ratelimit

suite "RateLimiter test suite":
  test "Single subject allow/reject test with real timer":
    var counter = RateCounter.init(100.milliseconds, 5)

    var success = 0
    var failure = 0
    for i in 0 ..< 100:
      if counter.checkRate():
        inc(success)
      else:
        inc(failure)

    check success == 5
    check failure == 95

    # Sleeping for 200 milliseconds for sure
    os.sleep(200)
    success = 0
    failure = 0

    for i in 0 ..< 100:
      if counter.checkRate():
        inc(success)
      else:
        inc(failure)

    check success == 5
    check failure == 95
