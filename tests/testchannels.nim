#                Chronos Test Suite
#            (c) Copyright 2018-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import unittest
import ../chronos

const
  hasThreadSupport* = compileOption("threads")
  TestRunsCount = 100

suite "Asynchronous channels test suite":

  proc testStSync(runs: int, queue: int): bool =
    var tun = newAsyncChannel[int](queue)
    tun.open()
    for i in 0..<runs:
      tun.sendSync(i * 10)
      var msg = tun.recvSync()
      if msg != i * 10:
        tun.close()
        return false
    tun.close()
    return true

  proc testStAsync(runs: int, queue: int): Future[bool] {.async.} =
    var tun = newAsyncChannel[int](queue)
    tun.open()
    for i in 0..<runs:
      await tun.send(i * 10)
      var msg = await tun.recv()
      if msg != i * 10:
        tun.close()
        return false
    tun.close()
    return true

  proc testStCombined(runs: int, queue: int): Future[bool] {.async.} =
    var tun = newAsyncChannel[int](queue)
    tun.open()
    for i in 0..<runs:
      tun.sendSync(i * 10)
      var msg = await tun.recv()
      if msg != i * 10:
        tun.close()
        return false
    for i in 0..<runs:
      await tun.send(i * 10)
      var msg = tun.recvSync()
      if msg != i * 10:
        tun.close()
        return false
    tun.close()
    return true

  when hasThreadSupport:
    type
      ThreadArg = object
        runs: int
        tun: AsyncChannel[int]

    proc threadSyncFunc1(arg: ThreadArg) {.thread.} =
      # echo "Sending thread started [", getThreadId(), "]"
      arg.tun.open()
      for i in 0..<arg.runs:
        arg.tun.sendSync(i * 10)
      arg.tun.close()
      # echo "Sending thread finished [", getThreadId(), "]"

    proc testMtSync(threads: int, runs: int, queue: int): bool =
      var tun = newAsyncChannel[int](queue)
      var arg = ThreadArg(tun: tun, runs: runs)
      var thrs = newSeq[Thread[ThreadArg]](threads)
      for i in 0..<threads:
        createThread(thrs[i], threadSyncFunc1, arg)
      var total = threads * runs
      tun.open()
      for i in 0..<total:
        var msg = arg.tun.recvSync()
      joinThreads(thrs)
      tun.close()
      result = true

    proc threadAsyncFunc(arg: ThreadArg) {.async.} =
      arg.tun.open()
      for i in 0..<arg.runs:
        await arg.tun.send(i * 10)
      arg.tun.close()

    proc threadSyncFunc2(arg: ThreadArg) {.thread.} =
      waitFor threadAsyncFunc(arg)

    proc asyncReceiver(arg: ThreadArg, total: int) {.async.} =
      arg.tun.open()
      for i in 0..<total:
        var msg = await arg.tun.recv()
      arg.tun.close()

    proc testMtAsync(threads: int, runs: int, queue: int): bool =
      var tun = newAsyncChannel[int](queue)
      var arg = ThreadArg(tun: tun, runs: runs)
      var thrs = newSeq[Thread[ThreadArg]](threads)
      for i in 0..<threads:
        createThread(thrs[i], threadSyncFunc2, arg)
      var total = threads * runs
      waitFor asyncReceiver(arg, total)
      joinThreads(thrs)
      result = true

    proc threadCombinedAsyncFunc(arg: ThreadArg) {.async.} =
      arg.tun.open()
      for i in 0..<arg.runs:
        await arg.tun.send(i * 10)
      arg.tun.close()

    proc threadCombinedFunc(arg: ThreadArg) {.thread.} =
      arg.tun.open()
      for i in 0..<arg.runs:
        arg.tun.sendSync(i * 10)
      arg.tun.close()

      waitFor threadCombinedAsyncFunc(arg)

    proc testMtCombined(threads: int, runs: int, queue: int): bool =
      var tun = newAsyncChannel[int](queue)
      var arg = ThreadArg(tun: tun, runs: runs)
      var thrs = newSeq[Thread[ThreadArg]](threads)
      for i in 0..<threads:
        createThread(thrs[i], threadCombinedFunc, arg)
      var total = threads * runs
      tun.open()
      waitFor asyncReceiver(arg, total)
      for i in 0..<total:
        var msg = arg.tun.recvSync()
      joinThreads(thrs)
      tun.close()
      result = true

  test "Single-threaded synchronous with infinite queue test":
    check testStSync(500, -1) == true
  test "Single-threaded synchronous with limited queue test":
    check testStSync(500, 10) == true
  test "Single-threaded asynchronous with infinite queue test":
    check waitFor(testStAsync(500, -1)) == true
  test "Single-threaded synchronous with limited queue test":
    check waitFor(testStAsync(500, 10)) == true
  test "Single-threaded sync + async with infinite queue test":
    check waitFor(testStCombined(250, -1)) == true
  test "Single-threaded sync + async with limited queue test":
    check waitFor(testStCombined(250, 10)) == true

  test "Multi-threaded synchronous with infinite queue test":
    when hasThreadSupport:
      check testMtSync(10, TestRunsCount, -1) == true
    else:
      skip()
  test "Multi-threaded synchronous with limited queue test":
    when hasThreadSupport:
      check testMtSync(10, TestRunsCount, 10) == true
    else:
      skip()
  test "Multi-threaded asynchronous with infinite queue test":
    when hasThreadSupport:
      check testMtAsync(10, TestRunsCount, -1) == true
    else:
      skip()
  test "Multi-threaded asynchronous with limited queue test":
    when hasThreadSupport:
      check testMtAsync(10, TestRunsCount, 10) == true
    else:
      skip()
  test "Multi-threaded sync + async with infinite queue test":
    when hasThreadSupport:
      check testMtCombined(10, TestRunsCount, -1) == true
    else:
      skip()
  test "Multi-threaded sync + async with limited queue test":
    when hasThreadSupport:
      check testMtCombined(10, TestRunsCount, 10) == true
    else:
      skip()
