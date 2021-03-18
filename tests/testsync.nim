#                Chronos Test Suite
#            (c) Copyright 2018-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import unittest
import ../chronos

when defined(nimHasUsed): {.used.}

suite "Asynchronous sync primitives test suite":
  var testLockResult {.threadvar.}: string
  var testEventResult {.threadvar.}: string
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
    for i in 0 ..< 20:
      poll()
    result = testLockResult

  proc testFlag(): Future[bool] {.async.} =
    var lock = newAsyncLock()
    var futs: array[4, Future[void]]
    futs[0] = lock.acquire()
    futs[1] = lock.acquire()
    futs[2] = lock.acquire()
    futs[3] = lock.acquire()

    proc checkFlags(b0, b1, b2, b3, b4: bool): bool =
      (lock.locked == b0) and
        (futs[0].finished == b1) and (futs[1].finished == b2) and
        (futs[2].finished == b3) and (futs[3].finished == b4)

    if not(checkFlags(true, true, false, false ,false)):
      return false

    lock.release()
    if not(checkFlags(true, true, false, false, false)):
      return false
    await sleepAsync(10.milliseconds)
    if not(checkFlags(true, true, true, false, false)):
      return false

    lock.release()
    if not(checkFlags(true, true, true, false, false)):
      return false
    await sleepAsync(10.milliseconds)
    if not(checkFlags(true, true, true, true, false)):
      return false

    lock.release()
    if not(checkFlags(true, true, true, true, false)):
      return false
    await sleepAsync(10.milliseconds)
    if not(checkFlags(true, true, true, true, true)):
      return false

    lock.release()
    if not(checkFlags(false, true, true, true, true)):
      return false
    await sleepAsync(10.milliseconds)
    if not(checkFlags(false, true, true, true, true)):
      return false

    return true

  proc testNoAcquiredRelease(): Future[bool] {.async.} =
    var lock = newAsyncLock()
    var res = false
    try:
      lock.release()
    except AsyncLockError:
      res = true
    return res

  proc testDoubleRelease(): Future[bool] {.async.} =
    var lock = newAsyncLock()
    var fut0 = lock.acquire()
    var fut1 = lock.acquire()
    var res = false
    asyncSpawn fut0
    asyncSpawn fut1
    lock.release()
    try:
      lock.release()
    except AsyncLockError:
      res = true
    return res

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
    ## There must be exactly 1 poll() call
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

  proc testPriorityBehavior(): int =
    proc task1(aq: AsyncPriorityQueue[int]): Future[int] {.async.} =
      var item1 = await aq.pop()
      var item2 = await aq.pop()
      return item1 + item2

    proc task2(aq: AsyncPriorityQueue[int]) {.async.} =
      await aq.push(1000)
      await aq.push(2000)

    var queue = newAsyncPriorityQueue[int](1)
    var fut = task1(queue)
    discard task2(queue)
    ## There must be exactly 2 poll() calls
    poll()
    poll()
    result = if fut.finished(): fut.read() else: 0

  proc testPriorityQueue(): bool =
    proc task1(aq: AsyncPriorityQueue[int]): Future[string] {.async.} =
      var res = ""
      for i in 0 ..< 10:
        var item = await aq.pop()
        res = res & $item
      return res

    proc task2(aq: AsyncPriorityQueue[int]) {.async.} =
      await aq.push(6)
      await aq.push(5)
      await aq.push(9)
      await aq.push(3)
      await aq.push(4)
      await aq.push(8)
      await aq.push(2)
      await aq.push(1)
      await aq.push(0)
      await aq.push(7)

    var queue1 = newAsyncPriorityQueue[int](10)
    var queue2 = newAsyncPriorityQueue[int](1)

    discard task2(queue1)
    let r1 = waitFor task1(queue1)
    discard task2(queue2)
    let r2 = waitFor task1(queue2)

    return r1 == "0123456789" and r2 == "6593482107"

  proc testAsyncSemaphoreBehavior(): bool =
    var res = ""

    proc testSemaphore(n: int, sem: AsyncSemaphore) {.async.} =
      await sem.acquire()
      res = res & $n
      sem.release()

    proc test(): Future[string] {.async.} =
      var sem = newAsyncSemaphore()
      await sem.acquire()
      discard testSemaphore(0, sem)
      discard testSemaphore(1, sem)
      discard testSemaphore(2, sem)
      discard testSemaphore(3, sem)
      discard testSemaphore(4, sem)
      discard testSemaphore(5, sem)
      discard testSemaphore(6, sem)
      discard testSemaphore(7, sem)
      discard testSemaphore(8, sem)
      discard testSemaphore(9, sem)
      sem.release()
      for i in 0 ..< 20:
        poll()
      return res

    return waitFor(test()) == "0123456789"

  proc testAsyncSemaphoreDoubleRelease(): bool =
    var res = ""

    proc testSemaphore(n: int, sem: AsyncSemaphore) {.async.} =
      await sem.acquire()
      res = res & $n
      sem.release()

    proc test(): Future[bool] {.async.} =
      var sem = newAsyncSemaphore()
      await sem.acquire()
      var fut1 = testSemaphore(0, sem)
      sem.release()
      await sem.acquire()
      await sleepAsync(50.milliseconds)
      if fut1.finished() == true:
        return false
      sem.release()
      await sleepAsync(50.milliseconds)
      if fut1.finished() == false:
        return false
      return res == "0"

    return waitFor(test())

  proc testAsyncSemaphoreCounter(): bool =
    var res = ""

    proc testSemaphore(n: int, sem: AsyncSemaphore) {.async.} =
      await sem.acquire()
      res = res & $n

    proc test(): Future[bool] {.async.} =
      var sem = newAsyncSemaphore(3)
      discard testSemaphore(0, sem)
      discard testSemaphore(1, sem)
      discard testSemaphore(2, sem)
      discard testSemaphore(3, sem)
      discard testSemaphore(4, sem)
      discard testSemaphore(5, sem)
      await sleepAsync(50.milliseconds)
      if res != "012":
        return false
      sem.release()
      await sleepAsync(50.milliseconds)
      if res != "0123":
        return false
      sem.release()
      sem.release()
      await sleepAsync(50.milliseconds)
      if res != "012345":
        return false
      return true

    return waitFor(test())

  test "AsyncLock() behavior test":
    check:
      test1() == "0123456789"
      waitFor(testBehaviorLock(10.milliseconds,
                               20.milliseconds,
                               50.milliseconds)) == @[10, 20, 30, 11, 21, 31]
      waitFor(testBehaviorLock(50.milliseconds,
                               20.milliseconds,
                               10.milliseconds)) == @[10, 20, 30, 11, 21, 31]
  test "AsyncLock() cancellation test":
    check:
      waitFor(testCancelLock(10.milliseconds,
                             20.milliseconds,
                             50.milliseconds, 2)) == @[10, 30, 11, 31]
      waitFor(testCancelLock(50.milliseconds,
                             20.milliseconds,
                             10.milliseconds, 3)) == @[10, 20, 11, 21]
  test "AsyncLock() flag consistency test":
    check waitFor(testFlag()) == true
  test "AsyncLock() double release test":
    check waitFor(testDoubleRelease()) == true
  test "AsyncLock() non-acquired release test":
    check waitFor(testNoAcquiredRelease()) == true
  test "AsyncEvent() behavior test":
    check test2() == "0123456789"
  test "AsyncQueue() behavior test":
    check test3() == 3000
  test "AsyncQueue() many iterations test":
    check test4() == 0
  test "AsyncQueue() addLast/addFirst/popLast/popFirst test":
    check test5() == 1100
  test "AsyncQueue() iterators/assignments test":
    check test7() == true
  test "AsyncQueue() representation test":
    check test8() == true
  test "AsyncQueue() contains test":
    check test9() == true
  test "AsyncPriorityQueue() behavior test":
    check testPriorityBehavior() == 3000
  test "AsyncPriorityQueue() priority test":
    check testPriorityQueue() == true
  test "AsyncSemaphore() behavior test":
    check testAsyncSemaphoreBehavior() == true
  test "AsyncSemaphore() double release test":
    check testAsyncSemaphoreDoubleRelease() == true
  test "AsyncSemaphore() counter test":
    check testAsyncSemaphoreCounter() == true
