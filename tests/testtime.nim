#                Chronos Test Suite
#            (c) Copyright 2018-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import os, unittest
import ../chronos, ../chronos/timer

when defined(nimHasUsed): {.used.}

suite "Asynchronous timers test suite":
  const TimersCount = 10

  proc timeWorker(time: Duration): Future[Duration] {.async.} =
    var st = Moment.now()
    await sleepAsync(time)
    var et = Moment.now()
    result = et - st

  proc waitAll[T](futs: seq[Future[T]]): Future[void] =
    var counter = len(futs)
    var retFuture = newFuture[void]("waitAll")
    proc cb(udata: pointer) =
      dec(counter)
      if counter == 0:
        retFuture.complete()
    for fut in futs:
      fut.addCallback(cb)
    return retFuture

  proc test(timeout: Duration): Future[Duration] {.async.} =
    var workers = newSeq[Future[Duration]](TimersCount)
    for i in 0..<TimersCount:
      workers[i] = timeWorker(timeout)
    await waitAll(workers)
    var sum: Duration
    for i in 0..<TimersCount:
      var time = workers[i].read()
      sum = sum + time
    result = sum div 10'i64

  proc testTimer(): bool =
    let a = Moment.now()
    waitFor(sleepAsync(1000.milliseconds))
    let b = Moment.now()
    let d = b - a
    result = (d >= 1000.milliseconds) and (d <= 3000.milliseconds)
    if not result:
      echo d

  test "Timer reliability test [" & asyncTimer & "]":
    check testTimer() == true
  test $TimersCount & " timers with 10ms timeout":
    var res = waitFor(test(10.milliseconds))
    check (res >= 10.milliseconds) and (res <= 100.milliseconds)
  test $TimersCount & " timers with 100ms timeout":
    var res = waitFor(test(100.milliseconds))
    check (res >= 100.milliseconds) and (res <= 1000.milliseconds)
  test $TimersCount & " timers with 1000ms timeout":
    var res = waitFor(test(1000.milliseconds))
    check (res >= 1000.milliseconds) and (res <= 5000.milliseconds)

  test "Interval trigger":
    proc testInterval(): Future[bool] {.async, gcsafe.} =
      var count = 0
      let cancelation = addInterval(1.millis,
                                    proc (data: pointer = nil) {.gcsafe.} = 
                                      count.inc())

      # wait 100 milliseconds and then complete the interval
      await sleepAsync(100.millis)
      cancelation.complete()

      # allow interval to finish
      await sleepAsync(1.millis)
      result = count > 50

    check waitFor(testInterval()) == true

  test "Interval cancelation":
    proc testInterval(): Future[bool] {.async, gcsafe.} =
      var count = 0
      var cancelation: Future[void]
      proc handler(data: pointer = nil) {.gcsafe.} = 
        if count == 10:
          cancelation.complete()
          return
        count.inc()

      cancelation = addInterval(1.millis, handler)

      await sleepAsync(20.millis)
      check cancelation.finished() == true
      result = count == 10 # shouldn't be called more than 10 times

    check waitFor(testInterval()) == true
