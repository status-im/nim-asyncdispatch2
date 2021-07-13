#                Chronos Test Suite
#            (c) Copyright 2018-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

import unittest2
import ../chronos

when defined(nimHasUsed): {.used.}

suite "Cancellation test suite":
  test "cancel() async procedure test":
    var completed = 0

    proc client1() {.async.} =
      await sleepAsync(1.seconds)
      inc(completed)

    proc client2() {.async.} =
      await client1()
      inc(completed)

    proc client3() {.async.} =
      await client2()
      inc(completed)

    proc client4() {.async.} =
      await client3()
      inc(completed)

    var fut = client4()
    fut.cancel()

    # Future must not be cancelled immediately, because it has many nested
    # futures.
    check:
      not fut.cancelled()

    expect(CancelledError):
      waitFor fut

    check: completed == 0

  test "cancelAndWait() test":
    var completed = 0

    proc client1() {.async.} =
      await sleepAsync(1.seconds)
      inc(completed)

    proc client2() {.async.} =
      await client1()
      inc(completed)

    proc client3() {.async.} =
      await client2()
      inc(completed)

    proc client4() {.async.} =
      await client3()
      inc(completed)

    var fut = client4()
    waitFor cancelAndWait(fut)
    check:
      fut.cancelled()

  test "Break cancellation propagation test":
    var completed = 0

    proc client1() {.async.} =
      await sleepAsync(1.seconds)
      inc(completed)

    proc client2() {.async.} =
      try:
        await client1()
      except CancelledError:
        discard
      inc(completed)

    var fut1 = client2()
    var fut2 = client2()
    fut1.cancel()
    waitFor fut1
    waitFor cancelAndWait(fut2)

    check:
      not fut1.cancelled()
      not fut2.cancelled()
      completed == 2

  test "Cancellation callback test":
    var completed = 0
    var cancelled = 0

    proc client1(duration: Duration): Future[void] =
      ## Suspends the execution of the current async procedure for the next
      ## ``duration`` time.
      var retFuture = newFuture[void]()
      let moment = Moment.fromNow(duration)

      proc completion(data: pointer) {.gcsafe.} =
        inc(completed)
        if not(retFuture.finished()):
          retFuture.complete()

      proc cancel(udata: pointer) {.gcsafe.} =
        inc(cancelled)
        if not(retFuture.finished()):
          removeTimer(moment, completion, cast[pointer](retFuture))

      retFuture.cancelCallback = cancel
      discard setTimer(moment, completion, cast[pointer](retFuture))
      return retFuture

    var fut = client1(100.milliseconds)
    fut.cancel()
    waitFor(sleepAsync(500.milliseconds))

    check:
      fut.cancelled()
      completed == 0
      cancelled == 1

  test "Cancellation wait() test":
    proc testWaitAsync(): Future[bool] {.async.} =
      var neverFlag1, neverFlag2, neverFlag3: bool
      var waitProc1, waitProc2: bool
      proc neverEndingProc(): Future[void] =
        var res = newFuture[void]()
        proc continuation(udata: pointer) {.gcsafe.} =
          neverFlag2 = true
        proc cancellation(udata: pointer) {.gcsafe.} =
          neverFlag3 = true
        res.addCallback(continuation)
        res.cancelCallback = cancellation
        result = res
        neverFlag1 = true

      proc waitProc() {.async.} =
        try:
          await wait(neverEndingProc(), 100.milliseconds)
        except CancelledError:
          waitProc1 = true
        finally:
          waitProc2 = true

      var fut = waitProc()
      await cancelAndWait(fut)
      return (fut.state == FutureState.Finished) and
             neverFlag1 and neverFlag2 and neverFlag3 and
             waitProc1 and waitProc2

    check: waitFor(testWaitAsync())

  test "Cancellation withTimeout() test":
    proc testWithTimeoutAsync(): Future[bool] {.async.} =
      var neverFlag1, neverFlag2, neverFlag3: bool
      var waitProc1, waitProc2: bool
      proc neverEndingProc(): Future[void] =
        var res = newFuture[void]()
        proc continuation(udata: pointer) {.gcsafe.} =
          neverFlag2 = true
        proc cancellation(udata: pointer) {.gcsafe.} =
          neverFlag3 = true
        res.addCallback(continuation)
        res.cancelCallback = cancellation
        result = res
        neverFlag1 = true

      proc withTimeoutProc() {.async.} =
        try:
          discard await withTimeout(neverEndingProc(), 100.milliseconds)
          doAssert(false)
        except CancelledError:
          waitProc1 = true
        finally:
          waitProc2 = true

      var fut = withTimeoutProc()
      await cancelAndWait(fut)
      result = (fut.state == FutureState.Finished) and
               neverFlag1 and neverFlag2 and neverFlag3 and
               waitProc1 and waitProc2
    check: waitFor(testWithTimeoutAsync())

  test "Cancellation race test":
    proc testCancellationRaceAsync() {.async.} =

      proc raceProc1(someFut: Future[void]) {.async.} =
        await someFut

      proc raceProc2(someFut: Future[void], otherFut: Future[void]) {.async.} =
        await someFut
        await otherFut

      proc raceProc3(someFut: Future[void],
                     otherFut: Future[void]): Future[int] {.async.} =
        try:
          await someFut
        except ValueError:
          return 1
        except CancelledError:
          return 2
        try:
          await otherFut
        except ValueError:
          return 3
        except CancelledError:
          return 4
        return 5

      proc getOrDefault(future: Future[int], default: int = -1): int =
        if future.done():
          future.read()
        else:
          default

      # Ancestor Future is already completed before cancellation.
      let res1 =
        block:
          var someFut = newFuture[void]()
          var raceFut = raceProc1(someFut)
          someFut.complete()
          await cancelAndWait(raceFut)
          (raceFut.state, someFut.state)

      # Ancestor Future is already failed before cancellation.
      let res2 =
        block:
          var someFut = newFuture[void]()
          var raceFut = raceProc1(someFut)
          someFut.fail(newException(ValueError, ""))
          await cancelAndWait(raceFut)
          (raceFut.state, someFut.state)

      # Ancestor Future is already cancelled before cancellation.
      let res3 =
        block:
          var someFut = newFuture[void]()
          var raceFut = raceProc1(someFut)
          someFut.cancel()
          await cancelAndWait(raceFut)
          (raceFut.state, someFut.state)

      # Ancestor Future is pending before cancellation.
      let res4 =
        block:
          var someFut = newFuture[void]()
          var raceFut = raceProc1(someFut)
          await cancelAndWait(raceFut)
          (raceFut.state, someFut.state)

      # Ancestor Future is completed before cancellation and other Future is
      # not yet started. Because raceProc2() do not have any `try/except` it
      # will become `Cancelled` too, while `otherFut` which is not executed yet
      # and must be `Pending`.
      let res5 =
        block:
          var someFut = newFuture[void]()
          var otherFut = newFuture[void]()
          var raceFut = raceProc2(someFut, otherFut)
          someFut.complete()
          await cancelAndWait(raceFut)
          let res = (raceFut.state, someFut.state, otherFut.state)
          await cancelAndWait(otherFut)
          res

      # Ancestor Future is failed before cancellation and other Future is not
      # yet started. Because `raceProc2` do not have any `try/except` it
      # will fail to and become `Failed`, while `otherFut` is not executed yet
      # and must be `Pending`.
      let res6 =
        block:
          var someFut = newFuture[void]()
          var otherFut = newFuture[void]()
          var raceFut = raceProc2(someFut, otherFut)
          someFut.fail(newException(ValueError, ""))
          await cancelAndWait(raceFut)
          let res = (raceFut.state, someFut.state, otherFut.state)
          await cancelAndWait(otherFut)
          res

      # Ancestor Future is cancelled before cancellation and other Future is not
      # yet started. Because `raceProc2` do not have any `try/except` it
      # will become `Cancelled`, while `otherFut` is not executed yet and must
      # be `Pending`.
      let res7 =
        block:
          var someFut = newFuture[void]()
          var otherFut = newFuture[void]()
          var raceFut = raceProc2(someFut, otherFut)
          someFut.cancel()
          await cancelAndWait(raceFut)
          let res = (raceFut.state, someFut.state, otherFut.state)
          await cancelAndWait(otherFut)
          res

      # Ancestor Future is pending before cancellation. Other Future is not
      # yet started. Because `raceProc2` do not have any `try/except` it
      # will become `Cancelled`, while `otherFut` is not executed yet and must
      # be `Pending`.
      let res8 =
        block:
          var someFut = newFuture[void]()
          var otherFut = newFuture[void]()
          var raceFut = raceProc2(someFut, otherFut)
          await cancelAndWait(raceFut)
          let res = (raceFut.state, someFut.state, otherFut.state)
          await cancelAndWait(otherFut)
          res

      let res9 =
        block:
          var someFut = newFuture[void]()
          var otherFut = newFuture[void]()
          var raceFut = raceProc3(someFut, otherFut)
          someFut.complete()
          await cancelAndWait(raceFut)
          let res = (raceFut.state, someFut.state, otherFut.state,
                     raceFut.getOrDefault())
          await cancelAndWait(otherFut)
          res

      let res10 =
        block:
          var someFut = newFuture[void]()
          var otherFut = newFuture[void]()
          var raceFut = raceProc3(someFut, otherFut)
          someFut.fail(newException(ValueError, ""))
          await cancelAndWait(raceFut)
          let res = (raceFut.state, someFut.state, otherFut.state,
                     raceFut.getOrDefault())
          await cancelAndWait(otherFut)
          res

      let res11 =
        block:
          var someFut = newFuture[void]()
          var otherFut = newFuture[void]()
          var raceFut = raceProc3(someFut, otherFut)
          someFut.cancel()
          await cancelAndWait(raceFut)
          let res = (raceFut.state, someFut.state, otherFut.state,
                     raceFut.getOrDefault())
          await cancelAndWait(otherFut)
          res

      let res12 =
        block:
          var someFut = newFuture[void]()
          var otherFut = newFuture[void]()
          var raceFut = raceProc3(someFut, otherFut)
          await cancelAndWait(raceFut)
          let res = (raceFut.state, someFut.state, otherFut.state,
                     raceFut.getOrDefault())
          await cancelAndWait(otherFut)
          res

      check:
        res1 == (FutureState.Finished, FutureState.Finished)
        res2 == (FutureState.Failed, FutureState.Failed)
        res3 == (FutureState.Cancelled, FutureState.Cancelled)
        res4 == (FutureState.Cancelled, FutureState.Cancelled)
        res5 == (FutureState.Cancelled, FutureState.Finished,
                 FutureState.Pending)
        res6 == (FutureState.Failed, FutureState.Failed,
                 FutureState.Pending)
        res7 == (FutureState.Cancelled, FutureState.Cancelled,
                 FutureState.Pending)
        res8 == (FutureState.Cancelled, FutureState.Cancelled,
                 FutureState.Pending)
        res9 == (FutureState.Finished, FutureState.Finished,
                 FutureState.Pending, 4)
        res10 == (FutureState.Finished, FutureState.Failed,
                  FutureState.Pending, 1)
        res11 == (FutureState.Finished, FutureState.Cancelled,
                  FutureState.Pending, 2)
        res12 == (FutureState.Finished, FutureState.Cancelled,
                  FutureState.Pending, 2)

    waitFor(testCancellationRaceAsync())

  test "awaitrc() test":
    proc testCancellationRCAsync() {.async.} =

      proc testSimple() {.async.} =
        await sleepAsync(0.milliseconds)

      proc testProc1(someFut: Future[void]) {.async.} =
        awaitrc someFut

      proc testProc2() {.async.} =
        awaitrc testSimple()

      proc testProc3(kind: int): Future[string] {.async.} =
        try:
          await testSimple()
          return "ERROR"
        except CancelledError:
          if kind == 0:
            awaitrc testSimple()
            awaitrc testSimple()
            awaitrc testSimple()
            return "OK"
          elif kind == 1:
            awaitrc testSimple()
            awaitrc testSimple()
            awaitrc testSimple()
            if true:
              raise newException(ValueError, "")

      let res1 =
        block:
          var someFut = newFuture[void]()
          var testFut = testProc1(someFut)
          someFut.complete()
          try:
            await testFut
            (testFut.state, someFut.state, "")
          except CatchableError as exc:
            (testFut.state, someFut.state, $exc.name)

      let res2 =
        block:
          var someFut = newFuture[void]()
          var testFut = testProc1(someFut)
          someFut.fail(newException(ValueError, "Value error"))
          try:
            await testFut
            (testFut.state, someFut.state, "")
          except CatchableError as exc:
            (testFut.state, someFut.state, $exc.name)

      let res3 =
        block:
          var someFut = newFuture[void]()
          var testFut = testProc1(someFut)
          someFut.cancel()
          try:
            await testFut
            (testFut.state, someFut.state, "")
          except CatchableError as exc:
            (testFut.state, someFut.state, $exc.name)

      let res4 =
        block:
          var testFut = testProc2()
          try:
            await testFut.cancelAndWait()
            (testFut.state, "")
          except CatchableError as exc:
            (testFut.state, $exc.name)

      let res5 =
        block:
          var testFut = testProc3(0)
          try:
            await testFut.cancelAndWait()
            let res = testFut.read()
            (testFut.state, res, "")
          except CatchableError as exc:
            (testFut.state, "", $exc.name)

      let res6 =
        block:
          var testFut = testProc3(1)
          try:
            await testFut.cancelAndWait()
            let res = testFut.read()
            (testFut.state, res, "")
          except CatchableError as exc:
            (testFut.state, "", $exc.name)

      check:
        res1 == (FutureState.Finished, FutureState.Finished, "")
        res2 == (FutureState.Failed, FutureState.Failed, "ValueError")
        res3 == (FutureState.Cancelled, FutureState.Cancelled, "CancelledError")
        res4 == (FutureState.Finished, "")
        res5 == (FutureState.Finished, "OK", "")
        res6 == (FutureState.Failed, "", "ValueError")

    waitFor(testCancellationRCAsync())
