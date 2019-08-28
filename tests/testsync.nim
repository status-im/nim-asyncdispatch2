#                Chronos Test Suite
#            (c) Copyright 2018-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import unittest
import ../chronos

const hasThreadSupport* = compileOption("threads")

suite "Asynchronous sync primitives test suite":
  var testLockResult = ""
  var testEventResult = ""
  var testQueue1Result = 0
  var testQueue2Result = 0
  var testQueue3Result = 0

  proc testLock(n: int, lock: AsyncLock) {.async.} =
    await lock.acquire()
    testLockResult = testLockResult & $n
    lock.release()

  proc test1(): string =
    var lock = newAsyncLock()
    waitFor lock.acquire()
    discard testLock(0, lock)
    discard testLock(1, lock)
    discard testLock(2, lock)
    discard testLock(3, lock)
    discard testLock(4, lock)
    discard testLock(5, lock)
    discard testLock(6, lock)
    discard testLock(7, lock)
    discard testLock(8, lock)
    discard testLock(9, lock)
    lock.release()
    ## There must be exactly 20 poll() calls
    for i in 0..<20:
      poll()
    result = testLockResult

  proc testBehaviorLock(n1, n2, n3: Duration): Future[seq[int]] {.async.} =
    var stripe: seq[int]

    proc task(lock: AsyncLock, n: int, timeout: Duration) {.async.} =
      await lock.acquire()
      stripe.add(n * 10)
      await sleepAsync(timeout)
      lock.release()
      await lock.acquire()
      stripe.add(n * 10 + 1)
      await sleepAsync(timeout)
      lock.release()

    var lock = newAsyncLock()
    var fut1 = task(lock, 1, n1)
    var fut2 = task(lock, 2, n2)
    var fut3 = task(lock, 3, n3)
    await allFutures(fut1, fut2, fut3)
    result = stripe

  proc testCancelLock(n1, n2, n3: Duration,
                      cancelIndex: int): Future[seq[int]] {.async.} =
    var stripe: seq[int]

    proc task(lock: AsyncLock, n: int, timeout: Duration) {.async.} =
      await lock.acquire()
      stripe.add(n * 10)
      await sleepAsync(timeout)
      lock.release()
      await lock.acquire()
      stripe.add(n * 10 + 1)
      await sleepAsync(timeout)
      lock.release()

    var lock = newAsyncLock()
    var fut1 = task(lock, 1, n1)
    var fut2 = task(lock, 2, n2)
    var fut3 = task(lock, 3, n3)
    if cancelIndex == 2:
      fut2.cancel()
    else:
      fut3.cancel()
    await allFutures(fut1, fut2, fut3)
    result = stripe


  proc testEvent(n: int, ev: AsyncEvent) {.async.} =
    await ev.wait()
    testEventResult = testEventResult & $n

  proc test2(): string =
    var event = newAsyncEvent()
    event.clear()
    discard testEvent(0, event)
    discard testEvent(1, event)
    discard testEvent(2, event)
    discard testEvent(3, event)
    discard testEvent(4, event)
    discard testEvent(5, event)
    discard testEvent(6, event)
    discard testEvent(7, event)
    discard testEvent(8, event)
    discard testEvent(9, event)
    event.fire()
    ## There must be exactly 2 poll() calls
    poll()
    poll()
    result = testEventResult

  proc task1(aq: AsyncQueue[int]) {.async.} =
    var item1 = await aq.get()
    var item2 = await aq.get()
    testQueue1Result = item1 + item2

  proc task2(aq: AsyncQueue[int]) {.async.} =
    await aq.put(1000)
    await aq.put(2000)

  proc test3(): int =
    var queue = newAsyncQueue[int](1)
    discard task1(queue)
    discard task2(queue)
    ## There must be exactly 2 poll() calls
    poll()
    poll()
    result = testQueue1Result

  const testsCount = 1000
  const queueSize = 10

  proc task3(aq: AsyncQueue[int]) {.async.} =
    for i in 1..testsCount:
      var item = await aq.get()
      testQueue2Result -= item

  proc task4(aq: AsyncQueue[int]) {.async.} =
    for i in 1..testsCount:
      await aq.put(i)
      testQueue2Result += i

  proc test4(): int =
    var queue = newAsyncQueue[int](queueSize)
    waitFor(allFutures(task3(queue), task4(queue)))
    result = testQueue2Result

  proc task51(aq: AsyncQueue[int]) {.async.} =
    var item1 = await aq.popFirst()
    var item2 = await aq.popLast()
    var item3 = await aq.get()
    testQueue3Result = item1 - item2 + item3

  proc task52(aq: AsyncQueue[int]) {.async.} =
    await aq.put(100)
    await aq.addLast(1000)
    await aq.addFirst(2000)

  proc test5(): int =
    var queue = newAsyncQueue[int](3)
    discard task51(queue)
    discard task52(queue)
    poll()
    poll()
    result = testQueue3Result

  proc test6(): bool =
    var queue = newAsyncQueue[int]()
    queue.putNoWait(1)
    queue.putNoWait(2)
    queue.putNoWait(3)
    queue.putNoWait(4)
    queue.putNoWait(5)
    queue.clear()
    result = (len(queue) == 0)

  proc test7(): bool =
    var queue = newAsyncQueue[int]()
    var arr1 = @[1, 2, 3, 4, 5]
    var arr2 = @[2, 2, 2, 2, 2]
    var arr3 = @[1, 2, 3, 4, 5]
    queue.putNoWait(1)
    queue.putNoWait(2)
    queue.putNoWait(3)
    queue.putNoWait(4)
    queue.putNoWait(5)
    var index = 0
    for item in queue.items():
      result = (item == arr1[index])
      inc(index)

    if not result: return

    queue[0] = 2

    result = (queue[0] == 2)

    if not result: return

    for item in queue.mitems():
      item = 2

    index = 0
    for item in queue.items():
      result = (item == arr2[index])
      inc(index)

    if not result: return

    queue[0] = 1
    queue[1] = 2
    queue[2] = 3
    queue[3] = 4
    queue[^1] = 5

    for i, item in queue.pairs():
      result = (item == arr3[i])

  proc test8(): bool =
    var q0 = newAsyncQueue[int]()
    q0.putNoWait(1)
    q0.putNoWait(2)
    q0.putNoWait(3)
    q0.putNoWait(4)
    q0.putNoWait(5)
    result = ($q0 == "[1, 2, 3, 4, 5]")
    if not result: return

    var q1 = newAsyncQueue[string]()
    q1.putNoWait("1")
    q1.putNoWait("2")
    q1.putNoWait("3")
    q1.putNoWait("4")
    q1.putNoWait("5")
    result = ($q1 == "[\"1\", \"2\", \"3\", \"4\", \"5\"]")

  proc test9(): bool =
    var q = newAsyncQueue[int]()
    q.putNoWait(1)
    q.putNoWait(2)
    q.putNoWait(3)
    q.putNoWait(4)
    q.putNoWait(5)
    result = (5 in q and not(6 in q))

  proc ateTest1(): bool =
    var e = newAsyncThreadEvent()
    let r1 = (waitFor(e.wait(100.milliseconds)) == WaitTimeout)
    let r2 = (e.waitSync(100.milliseconds) == WaitTimeout)
    e.close()
    result = r1 and r2

  proc ateTest2(): bool =
    var e = newAsyncThreadEvent()
    e.fire()
    let r1 = waitFor(e.wait(100.milliseconds)) == WaitSuccess
    let r2 = (e.waitSync(100.milliseconds)) == WaitTimeout
    e.fire()
    let r3 = (e.waitSync(100.milliseconds)) == WaitSuccess
    let r4 = waitFor(e.wait(100.milliseconds)) == WaitTimeout
    e.close()
    result = r1 and r2 and r3 and r4

  when hasThreadSupport:
    type
      ThreadArg = object
        event1: AsyncThreadEvent
        event2: AsyncThreadEvent
        event3: AsyncThreadEvent
        event4: AsyncThreadEvent

    proc ateSyncThread(arg: ThreadArg) {.thread.} =
      var res = true
      for i in 1..100:
        arg.event1.fire()
        let r = waitSync(arg.event2)
        if r != WaitSuccess:
          res = false
          break
      if res:
        arg.event4.fire()

    proc ateAsyncLoop(arg: ThreadArg): Future[bool] {.async.} =
      var res = true
      for i in 1..100:
        let r = await wait(arg.event1)
        if r == WaitSuccess:
          arg.event2.fire()
        else:
          res = false
          break
      return res

    proc ateAsyncThread(arg: ThreadArg) {.thread.} =
      let res = waitFor ateAsyncLoop(arg)
      if res:
        arg.event3.fire()

    proc ateTest3(): bool =
      var arg = ThreadArg(
        event1: newAsyncThreadEvent(),
        event2: newAsyncThreadEvent(),
        event3: newAsyncThreadEvent(),
        event4: newAsyncThreadEvent()
      )
      var thr1: Thread[ThreadArg]
      var thr2: Thread[ThreadArg]
      createThread(thr1, ateSyncThread, arg)
      createThread(thr2, ateAsyncThread, arg)
      let r1 = waitSync(arg.event3, 10.seconds)
      let r2 = waitSync(arg.event4, 10.seconds)
      result = (r1 == WaitSuccess) and (r2 == WaitSuccess)
      close(arg.event1)
      close(arg.event2)
      close(arg.event3)
      close(arg.event4)
      if result:
        joinThreads(thr1, thr2)

  test "AsyncLock() behavior test":
    check:
      test1() == "0123456789"
      waitFor(testBehaviorLock(10.milliseconds,
                               20.milliseconds,
                               50.milliseconds)) == @[10, 20, 30, 11, 21, 31]
      waitFor(testBehaviorLock(50.milliseconds,
                               20.milliseconds,
                               10.milliseconds)) == @[10, 20, 30, 11, 21, 31]
      waitFor(testCancelLock(10.milliseconds,
                             20.milliseconds,
                             50.milliseconds, 2)) == @[10, 30, 11, 31]
      waitFor(testCancelLock(50.milliseconds,
                             20.milliseconds,
                             10.milliseconds, 3)) == @[10, 20, 11, 21]
  test "AsyncEvent() behavior test":
    check test2() == "0123456789"
  test "AsyncQueue() behavior test":
    check test3() == 3000
  test "AsyncQueue() many iterations test":
    check test4() == 0
  test "AsyncQueue() addLast/addFirst/popLast/popFirst test":
    check test5() == 1100
  test "AsyncQueue() clear test":
    check test6() == true
  test "AsyncQueue() iterators/assignments test":
    check test7() == true
  test "AsyncQueue() representation test":
    check test8() == true
  test "AsyncQueue() contains test":
    check test9() == true
  test "AsyncThreadEvent single-threaded test #1":
    check ateTest1() == true
  test "AsyncThreadEvent single-threaded test #2":
    check ateTest2() == true
  test "AsyncThreadEvent multi-threaded test #1":
    when hasThreadSupport:
      check ateTest3() == true
    else:
      skip()
