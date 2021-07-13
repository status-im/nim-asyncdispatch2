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

suite "Future[T] behavior test suite":
  proc testFuture1(): Future[int] {.async.} =
    await sleepAsync(0.milliseconds)

  proc testFuture2(): Future[int] {.async.} =
    return 1

  proc testFuture3(): Future[int] {.async.} =
    result = await testFuture2()

  proc testFuture100(): Future[int] {.async.} =
    await sleepAsync(100.milliseconds)

  proc testFuture4(): Future[int] {.async.} =
    ## Test for not immediately completed future and timeout = -1
    result = 0
    try:
      discard await wait(testFuture1(), InfiniteDuration)
      result = 1
    except:
      result = 0

    if result == 0:
      return -1

    ## Test for immediately completed future and timeout = -1
    result = 0
    try:
      discard await wait(testFuture2(), InfiniteDuration)
      result = 2
    except:
      result = 0

    if result == 0:
      return -2

    ## Test for not immediately completed future and timeout = 0
    result = 0
    try:
      discard await wait(testFuture1(), 0.milliseconds)
    except AsyncTimeoutError:
      result = 3

    if result == 0:
      return -3

    ## Test for immediately completed future and timeout = 0
    result = 0
    try:
      discard await wait(testFuture2(), 0.milliseconds)
      result = 4
    except:
      result = 0

    if result == 0:
      return -4

    ## Test for future which cannot be completed in timeout period
    result = 0
    try:
      discard await wait(testFuture100(), 50.milliseconds)
    except AsyncTimeoutError:
      result = 5

    if result == 0:
      return -5

    ## Test for future which will be completed before timeout exceeded.
    try:
      discard await wait(testFuture100(), 500.milliseconds)
      result = 6
    except:
      result = -6

  test "Async undefined behavior (#7758) test":
    var fut = testFuture1()
    poll()
    poll()
    if not fut.finished:
      poll()
    check: fut.finished

  test "Immediately completed asynchronous procedure test":
    var fut = testFuture3()
    check: fut.finished

  test "Future[T] callbacks are invoked in reverse order (#7197) test":
    var testResult = ""
    var fut = testFuture1()
    fut.addCallback proc(udata: pointer) =
      testResult &= "1"
    fut.addCallback proc(udata: pointer) =
      testResult &= "2"
    fut.addCallback proc(udata: pointer) =
      testResult &= "3"
    fut.addCallback proc(udata: pointer) =
      testResult &= "4"
    fut.addCallback proc(udata: pointer) =
      testResult &= "5"
    discard waitFor(fut)
    poll()

    check:
      fut.finished
      testResult == "12345"

  test "Future[T] callbacks not changing order after removeCallback()":
    var testResult = ""
    var fut = testFuture1()
    proc cb1(udata: pointer) =
      testResult &= "1"
    proc cb2(udata: pointer) =
      testResult &= "2"
    proc cb3(udata: pointer) =
      testResult &= "3"
    proc cb4(udata: pointer) =
      testResult &= "4"
    proc cb5(udata: pointer) =
      testResult &= "5"
    fut.addCallback cb1
    fut.addCallback cb2
    fut.addCallback cb3
    fut.addCallback cb4
    fut.addCallback cb5
    fut.removeCallback cb3
    discard waitFor(fut)
    poll()
    check:
      fut.finished
      testResult == "1245"

  test "wait[T]() test":
    check:
      waitFor(testFuture4()) == 6

  test "discard result Future[T] test":
    var completedFutures = 0

    proc client1() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)

    proc client2() {.async.} =
      await sleepAsync(200.milliseconds)
      inc(completedFutures)

    proc client3() {.async.} =
      await sleepAsync(300.milliseconds)
      inc(completedFutures)

    proc client4() {.async.} =
      await sleepAsync(400.milliseconds)
      inc(completedFutures)

    proc client5() {.async.} =
      await sleepAsync(500.milliseconds)
      inc(completedFutures)

    proc client1f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client2f() {.async.} =
      await sleepAsync(200.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client3f() {.async.} =
      await sleepAsync(300.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client4f() {.async.} =
      await sleepAsync(400.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client5f() {.async.} =
      await sleepAsync(500.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    discard client1()
    discard client1f()
    discard client2()
    discard client2f()
    discard client3()
    discard client3f()
    discard client4()
    discard client4f()
    discard client5()
    discard client5f()

    waitFor(sleepAsync(2000.milliseconds))
    check:
      completedFutures == 10

  test "allFutures(zero) test":
    var tseq = newSeq[Future[int]]()
    var fut = allFutures(tseq)
    check:
      fut.finished

  test "allFutures(varargs) test":
    var completedFutures = 0

    proc vlient1() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)

    proc vlient2() {.async.} =
      await sleepAsync(200.milliseconds)
      inc(completedFutures)

    proc vlient3() {.async.} =
      await sleepAsync(300.milliseconds)
      inc(completedFutures)

    proc vlient4() {.async.} =
      await sleepAsync(400.milliseconds)
      inc(completedFutures)

    proc vlient5() {.async.} =
      await sleepAsync(500.milliseconds)
      inc(completedFutures)

    proc vlient1f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc vlient2f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc vlient3f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc vlient4f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc vlient5f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client1(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      result = 1

    proc client2(): Future[int] {.async.} =
      await sleepAsync(200.milliseconds)
      inc(completedFutures)
      result = 1

    proc client3(): Future[int] {.async.} =
      await sleepAsync(300.milliseconds)
      inc(completedFutures)
      result = 1

    proc client4(): Future[int] {.async.} =
      await sleepAsync(400.milliseconds)
      inc(completedFutures)
      result = 1

    proc client5(): Future[int] {.async.} =
      await sleepAsync(500.milliseconds)
      inc(completedFutures)
      result = 1

    proc client1f(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client2f(): Future[int] {.async.} =
      await sleepAsync(200.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client3f(): Future[int] {.async.} =
      await sleepAsync(300.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client4f(): Future[int] {.async.} =
      await sleepAsync(400.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client5f(): Future[int] {.async.} =
      await sleepAsync(500.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    waitFor(allFutures(vlient1(), vlient2(), vlient3(), vlient4(), vlient5()))

    check: completedFutures == 5
    completedFutures = 0

    waitFor(allFutures(vlient1(), vlient1f(),
                       vlient2(), vlient2f(),
                       vlient3(), vlient3f(),
                       vlient4(), vlient4f(),
                       vlient5(), vlient5f()))

    check: completedFutures == 10

    completedFutures = 0
    waitFor(allFutures(client1(), client2(), client3(), client4(), client5()))
    check: completedFutures  == 5

    completedFutures = 0
    waitFor(allFutures(client1(), client1f(),
                       client2(), client2f(),
                       client3(), client3f(),
                       client4(), client4f(),
                       client5(), client5f()))
    check: completedFutures == 10

  test "allFutures(varargs) test":
    var completedFutures = 0
    var vfutures = newSeq[Future[void]]()
    var nfutures = newSeq[Future[int]]()

    proc vlient1() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)

    proc vlient2() {.async.} =
      await sleepAsync(200.milliseconds)
      inc(completedFutures)

    proc vlient3() {.async.} =
      await sleepAsync(300.milliseconds)
      inc(completedFutures)

    proc vlient4() {.async.} =
      await sleepAsync(400.milliseconds)
      inc(completedFutures)

    proc vlient5() {.async.} =
      await sleepAsync(500.milliseconds)
      inc(completedFutures)

    proc vlient1f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc vlient2f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc vlient3f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc vlient4f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc vlient5f() {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client1(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      result = 1

    proc client2(): Future[int] {.async.} =
      await sleepAsync(200.milliseconds)
      inc(completedFutures)
      result = 1

    proc client3(): Future[int] {.async.} =
      await sleepAsync(300.milliseconds)
      inc(completedFutures)
      result = 1

    proc client4(): Future[int] {.async.} =
      await sleepAsync(400.milliseconds)
      inc(completedFutures)
      result = 1

    proc client5(): Future[int] {.async.} =
      await sleepAsync(500.milliseconds)
      inc(completedFutures)
      result = 1

    proc client1f(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client2f(): Future[int] {.async.} =
      await sleepAsync(200.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client3f(): Future[int] {.async.} =
      await sleepAsync(300.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client4f(): Future[int] {.async.} =
      await sleepAsync(400.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    proc client5f(): Future[int] {.async.} =
      await sleepAsync(500.milliseconds)
      inc(completedFutures)
      if true:
        raise newException(ValueError, "")

    vfutures.setLen(0)
    for i in 0..<10:
      vfutures.add(vlient1())
      vfutures.add(vlient2())
      vfutures.add(vlient3())
      vfutures.add(vlient4())
      vfutures.add(vlient5())

    waitFor(allFutures(vfutures))
    # 5 * 10 completed futures = 50
    check: completedFutures == 50

    completedFutures = 0
    vfutures.setLen(0)
    for i in 0..<10:
      vfutures.add(vlient1())
      vfutures.add(vlient1f())
      vfutures.add(vlient2())
      vfutures.add(vlient2f())
      vfutures.add(vlient3())
      vfutures.add(vlient3f())
      vfutures.add(vlient4())
      vfutures.add(vlient4f())
      vfutures.add(vlient5())
      vfutures.add(vlient5f())

    waitFor(allFutures(vfutures))
    # 10 * 10 completed futures = 100
    check: completedFutures == 100

    completedFutures = 0
    nfutures.setLen(0)
    for i in 0..<10:
      nfutures.add(client1())
      nfutures.add(client2())
      nfutures.add(client3())
      nfutures.add(client4())
      nfutures.add(client5())

    waitFor(allFutures(nfutures))
    # 5 * 10 completed futures = 50
    check: completedFutures == 50

    completedFutures = 0
    nfutures.setLen(0)
    for i in 0..<10:
      nfutures.add(client1())
      nfutures.add(client1f())
      nfutures.add(client2())
      nfutures.add(client2f())
      nfutures.add(client3())
      nfutures.add(client3f())
      nfutures.add(client4())
      nfutures.add(client4f())
      nfutures.add(client5())
      nfutures.add(client5f())

    waitFor(allFutures(nfutures))

    # 10 * 10 completed futures = 100
    check: completedFutures == 100

  test "allFutures() already completed test":
    proc client1(): Future[int] {.async.} =
      result = 1

    proc client2(): Future[int] {.async.} =
      if true:
        raise newException(ValueError, "")

    var fut = allFutures(client1(), client2())
    check:
      fut.finished()
      not(fut.failed())

  test "allFinished() already completed test":
    proc client1(): Future[int] {.async.} =
      result = 1

    proc client2(): Future[int] {.async.} =
      if true:
        raise newException(ValueError, "")

    var fut = allFinished(client1(), client2())
    check:
      fut.finished()
      not(fut.failed())
      len(fut.read()) == 2

  test "one(zero) test":
    var tseq = newSeq[Future[int]]()
    var fut = one(tseq)
    check: fut.finished and fut.failed

  test "one(varargs) test":
    proc vlient1() {.async.} =
      await sleepAsync(100.milliseconds)

    proc vlient2() {.async.} =
      await sleepAsync(200.milliseconds)

    proc vlient3() {.async.} =
      await sleepAsync(300.milliseconds)

    proc client1(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      result = 10

    proc client2(): Future[int] {.async.} =
      await sleepAsync(200.milliseconds)
      result = 20

    proc client3(): Future[int] {.async.} =
      await sleepAsync(300.milliseconds)
      result = 30

    var fut11 = vlient1()
    var fut12 = vlient2()
    var fut13 = vlient3()
    var res1 = waitFor(one(fut11, fut12, fut13))

    var fut21 = vlient2()
    var fut22 = vlient1()
    var fut23 = vlient3()
    var res2 = waitFor(one(fut21, fut22, fut23))

    var fut31 = vlient3()
    var fut32 = vlient2()
    var fut33 = vlient1()
    var res3 = waitFor(one(fut31, fut32, fut33))

    check:
      fut11 == res1
      fut22 == res2
      fut33 == res3

    var cut11 = client1()
    var cut12 = client2()
    var cut13 = client3()
    var res4 = waitFor(one(cut11, cut12, cut13))

    var cut21 = client2()
    var cut22 = client1()
    var cut23 = client3()
    var res5 = waitFor(one(cut21, cut22, cut23))

    var cut31 = client3()
    var cut32 = client2()
    var cut33 = client1()
    var res6 = waitFor(one(cut31, cut32, cut33))

    check:
      cut11 == res4
      cut22 == res5
      cut33 == res6

  test "one(seq) test":
    proc vlient1() {.async.} =
      await sleepAsync(100.milliseconds)

    proc vlient2() {.async.} =
      await sleepAsync(200.milliseconds)

    proc vlient3() {.async.} =
      await sleepAsync(300.milliseconds)

    proc client1(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      result = 10

    proc client2(): Future[int] {.async.} =
      await sleepAsync(200.milliseconds)
      result = 20

    proc client3(): Future[int] {.async.} =
      await sleepAsync(300.milliseconds)
      result = 30

    var v10 = vlient1()
    var v11 = vlient2()
    var v12 = vlient3()
    var res1 = waitFor(one(@[v10, v11, v12]))

    var v20 = vlient2()
    var v21 = vlient1()
    var v22 = vlient3()
    var res2 = waitFor(one(@[v20, v21, v22]))

    var v30 = vlient3()
    var v31 = vlient2()
    var v32 = vlient1()
    var res3 = waitFor(one(@[v30, v31, v32]))

    check:
      res1 == v10
      res2 == v21
      res3 == v32

    var c10 = client1()
    var c11 = client2()
    var c12 = client3()
    var res4 = waitFor(one(@[c10, c11, c12]))

    var c20 = client2()
    var c21 = client1()
    var c22 = client3()
    var res5 = waitFor(one(@[c20, c21, c22]))

    var c30 = client3()
    var c31 = client2()
    var c32 = client1()
    var res6 = waitFor(one(@[c30, c31, c32]))

    check:
      res4 == c10
      res5 == c21
      res6 == c32

  test "one(completed) test":
    proc client1(): Future[int] {.async.} =
      result = 1

    proc client2(): Future[int] {.async.} =
      if true:
        raise newException(ValueError, "")

    proc client3(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      result = 3

    var f10 = client1()
    var f20 = client2()
    var f30 = client3()
    var fut1 = one(f30, f10, f20)
    var f11 = client1()
    var f21 = client2()
    var f31 = client3()
    var fut2 = one(f31, f21, f11)

    check:
      fut1.finished()
      not(fut1.failed())
      fut1.read() == f10
      fut2.finished()
      not(fut2.failed())
      fut2.read() == f21

  proc waitForNeLocal[T](fut: Future[T]): Future[T] =
    ## **Blocks** the current thread until the specified future completes.
    while not(fut.finished()):
      poll()
    result = fut

  test "or() test":
    proc client1() {.async.} =
      await sleepAsync(200.milliseconds)

    proc client2() {.async.} =
      await sleepAsync(300.milliseconds)

    proc client3() {.async.} =
      await sleepAsync(100.milliseconds)
      if true:
        raise newException(ValueError, "")

    proc client4() {.async.} =
      await sleepAsync(400.milliseconds)
      if true:
        raise newException(KeyError, "")

    var f1 = waitForNeLocal(client1() or client2())
    var f2 = waitForNeLocal(client2() or client1())
    var f3 = waitForNeLocal(client1() or client4())
    var f4 = waitForNeLocal(client2() or client4())
    var f5 = waitForNeLocal(client1() or client3())
    var f6 = waitForNeLocal(client3() or client1())
    var f7 = waitForNeLocal(client2() or client4())
    var f8 = waitForNeLocal(client4() or client2())
    var f9 = waitForNeLocal(client3() or client4())
    var f10 = waitForNeLocal(client4() or client3())

    check:
      (f1.finished() and not(f1.failed()))
      (f2.finished() and not(f2.failed()))
      (f3.finished() and not(f3.failed()))
      (f4.finished() and not(f4.failed()))
      (f5.finished() and f5.failed())
      (f6.finished() and f6.failed())
      (f7.finished() and not(f7.failed()))
      (f8.finished() and not(f8.failed()))
      (f9.finished() and f9.failed())
      (f10.finished() and f10.failed())

  test "or() already completed test":
    proc client1(): Future[int] {.async.} =
      result = 1
    proc client2(): Future[int] {.async.} =
      if true:
        raise newException(ValueError, "")
    proc client3(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      result = 3

    var f1 = client1() or client2()
    var f2 = client1() or client3()
    var f3 = client2() or client3()
    var f4 = client2() or client1()
    var f5 = client3() or client1()
    var f6 = client3() or client2()

    check:
      (f1.finished() and not(f1.failed())) and
             (f2.finished() and not(f2.failed())) and
             (f3.finished() and f3.failed()) and
             (f4.finished() and f4.failed()) and
             (f5.finished() and not(f5.failed())) and
             (f6.finished() and f6.failed())

  proc testAsyncSpawnAsync(): Future[bool] {.async.} =

    proc completeTask1() {.async.} =
      discard

    proc completeTask2() {.async.} =
      await sleepAsync(100.milliseconds)

    proc errorTask() {.async.} =
      if true:
        raise newException(ValueError, "")

    proc cancelTask() {.async.} =
      await sleepAsync(10.seconds)

    try:
      var fut1 = completeTask1()
      var fut2 = completeTask2()
      asyncSpawn fut1
      asyncSpawn fut2
      await sleepAsync(200.milliseconds)
      if not(fut1.finished()) or not(fut2.finished()):
        return false
      if fut1.failed() or fut1.cancelled() or fut2.failed() or fut2.cancelled():
        return false
    except:
      return false

    try:
      asyncSpawn errorTask()
      return false
    except FutureDefect:
      discard
    except:
      return false

    try:
      var fut = cancelTask()
      await cancelAndWait(fut)
      asyncSpawn fut
      return false
    except FutureDefect:
      discard
    except:
      return false

    return true

  test "asyncSpawn() test":
    check: waitFor(testAsyncSpawnAsync())

  test "location test":
    # WARNING: This test is very sensitive to line numbers and module name.

    proc macroFuture() {.async.} =
      let someVar {.used.} = 5
      let someOtherVar {.used.} = 4
      if true:
        let otherVar {.used.} = 3

    template templateFuture(): untyped =
      newFuture[void]("template")

    proc procFuture(): Future[void] =
      newFuture[void]("procedure")

    var fut1 = macroFuture()
    var fut2 = templateFuture()
    var fut3 = procFuture()

    fut2.complete()
    fut3.complete()

    let loc10 = fut1.location[0]
    let loc11 = fut1.location[1]
    let loc20 = fut2.location[0]
    let loc21 = fut2.location[1]
    let loc30 = fut3.location[0]
    let loc31 = fut3.location[1]

    proc chk(loc: ptr SrcLoc, file: string, line: int,
             procedure: string): bool =
      if len(procedure) == 0:
        (loc.line == line) and ($loc.file  == file)
      else:
        (loc.line == line) and ($loc.file  == file) and
        (loc.procedure == procedure)

    check:
      chk(loc10, "testfut.nim", 837, "macroFuture")
      chk(loc11, "testfut.nim", 838, "")
      chk(loc20, "testfut.nim", 850, "template")
      chk(loc21, "testfut.nim", 853, "")
      chk(loc30, "testfut.nim", 847, "procedure")
      chk(loc31, "testfut.nim", 854, "")

  test "withTimeout(fut) should wait cancellation test":
    proc futureNeverEnds(): Future[void] =
      newFuture[void]("neverending.future")

    proc futureOneLevelMore() {.async.} =
      await futureNeverEnds()

    proc testWithTimeout(): Future[bool] {.async.} =
      var fut = futureOneLevelMore()
      try:
        let res = await withTimeout(fut, 100.milliseconds)
        # Because `fut` is never-ending Future[T], `withTimeout` should return
        # `false` but it also has to wait until `fut` is cancelled.
        if not(res) and fut.cancelled():
          return true
        else:
          return false
      except CatchableError:
        return false

    check: waitFor testWithTimeout()

  test "wait(fut) should wait cancellation test":
    proc futureNeverEnds(): Future[void] =
      newFuture[void]("neverending.future")

    proc futureOneLevelMore() {.async.} =
      await futureNeverEnds()

    proc testWait(): Future[bool] {.async.} =
      var fut = futureOneLevelMore()
      try:
        await wait(fut, 100.milliseconds)
        return false
      except AsyncTimeoutError:
        # Because `fut` is never-ending Future[T], `wait` should raise
        # `AsyncTimeoutError`, but only after `fut` is cancelled.
        if fut.cancelled():
          return true
        else:
          return false
      except CatchableError:
        return false

    check: waitFor testWait()

  test "race(zero) test":
    var tseq = newSeq[FutureBase]()
    var fut1 = race(tseq)
    var fut2 = race()
    var fut3 = race([])
    check:
      fut1.failed()
      fut2.failed()
      fut3.failed()

  test "race(varargs) test":
    proc vlient1() {.async.} =
      await sleepAsync(100.milliseconds)

    proc vlient2() {.async.} =
      await sleepAsync(200.milliseconds)

    proc vlient3() {.async.} =
      await sleepAsync(300.milliseconds)

    proc ilient1(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      result = 10

    proc ilient2(): Future[int] {.async.} =
      await sleepAsync(200.milliseconds)
      result = 20

    proc ilient3(): Future[int] {.async.} =
      await sleepAsync(300.milliseconds)
      result = 30

    proc slient1(): Future[string] {.async.} =
      await sleepAsync(100.milliseconds)
      result = "sclient1"

    proc slient2(): Future[string] {.async.} =
      await sleepAsync(200.milliseconds)
      result = "sclient2"

    proc slient3(): Future[string] {.async.} =
      await sleepAsync(300.milliseconds)
      result = "sclient3"

    var fut11 = vlient1()
    var fut12 = ilient2()
    var fut13 = slient3()
    var res1 = waitFor(race(fut11, fut12, fut13))

    var fut21 = vlient2()
    var fut22 = ilient1()
    var fut23 = slient3()
    var res2 = waitFor(race(fut21, fut22, fut23))

    var fut31 = vlient3()
    var fut32 = ilient2()
    var fut33 = slient1()
    var res3 = waitFor(race(fut31, fut32, fut33))

    var fut41 = vlient1()
    var fut42 = slient2()
    var fut43 = ilient3()
    var res4 = waitFor(race(fut41, fut42, fut43))

    check:
      FutureBase(fut11) == res1
      FutureBase(fut22) == res2
      FutureBase(fut33) == res3
      FutureBase(fut41) == res4

  test "race(seq) test":
    proc vlient1() {.async.} =
      await sleepAsync(100.milliseconds)

    proc vlient2() {.async.} =
      await sleepAsync(200.milliseconds)

    proc vlient3() {.async.} =
      await sleepAsync(300.milliseconds)

    proc ilient1(): Future[int] {.async.} =
      await sleepAsync(100.milliseconds)
      result = 10

    proc ilient2(): Future[int] {.async.} =
      await sleepAsync(200.milliseconds)
      result = 20

    proc ilient3(): Future[int] {.async.} =
      await sleepAsync(300.milliseconds)
      result = 30

    proc slient1(): Future[string] {.async.} =
      await sleepAsync(100.milliseconds)
      result = "slient1"

    proc slient2(): Future[string] {.async.} =
      await sleepAsync(200.milliseconds)
      result = "slient2"

    proc slient3(): Future[string] {.async.} =
      await sleepAsync(300.milliseconds)
      result = "slient3"

    var v10 = vlient1()
    var v11 = ilient2()
    var v12 = slient3()
    var res1 = waitFor(race(@[FutureBase(v10), FutureBase(v11),
                              FutureBase(v12)]))

    var v20 = vlient2()
    var v21 = ilient1()
    var v22 = slient3()
    var res2 = waitFor(race(@[FutureBase(v20), FutureBase(v21),
                              FutureBase(v22)]))

    var v30 = vlient3()
    var v31 = ilient2()
    var v32 = slient1()
    var res3 = waitFor(race(@[FutureBase(v30), FutureBase(v31),
                              FutureBase(v32)]))

    var v40 = vlient1()
    var v41 = slient2()
    var v42 = ilient3()
    var res4 = waitFor(race(@[FutureBase(v40), FutureBase(v41),
                              FutureBase(v42)]))

    check:
      res1 == FutureBase(v10)
      res2 == FutureBase(v21)
      res3 == FutureBase(v32)
      res4 == FutureBase(v40)

  test "race() already completed test":
    proc client1(): Future[int] {.async.} =
      result = 1

    proc client2() {.async.} =
      if true:
        raise newException(ValueError, "")

    proc client3(): Future[string] {.async.} =
      await sleepAsync(100.milliseconds)
      result = "client3"

    var f10 = client1()
    var f20 = client2()
    var f30 = client3()
    var fut1 = race(f30, f10, f20)
    var f11 = client1()
    var f21 = client2()
    var f31 = client3()
    var fut2 = race(f31, f21, f11)

    check:
      fut1.done() and fut1.read() == FutureBase(f10)
      fut2.done() and fut2.read() == FutureBase(f21)

  test "race() cancellation test":
    proc client1() {.async.} =
      await sleepAsync(100.milliseconds)

    proc client2(): Future[int] {.async.} =
      await sleepAsync(200.milliseconds)
      return 10

    proc client3(): Future[string] {.async.} =
      await sleepAsync(300.milliseconds)
      return "client3"

    var f1 = client1()
    var f2 = client2()
    var f3 = client3()
    var fut = race(f1, f2, f3)
    waitFor(cancelAndWait(fut))

    check:
      not f1.finished()
      not f2.finished()
      not f3.finished()

    waitFor(sleepAsync(400.milliseconds))

    check:
      f1.finished()
      f2.finished()
      f3.finished()
