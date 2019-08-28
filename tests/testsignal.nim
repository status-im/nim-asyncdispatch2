#                Chronos Test Suite
#            (c) Copyright 2018-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import unittest, strutils
import ../chronos

when not defined(windows):
  import posix

suite "Signal handling test suite":
  when not defined(windows):
    var signalCounter = 0

    proc signalProc(udata: pointer) =
      var cdata = cast[ptr CompletionData](udata)
      signalCounter = cast[int](cdata.udata)
      removeSignal(int(cdata.fd))

    proc asyncProc() {.async.} =
      await sleepAsync(500.milliseconds)

    proc test(signal, value: int): bool =
      discard addSignal(signal, signalProc, cast[pointer](value))
      var fut = asyncProc()
      discard posix.kill(posix.getpid(), cint(signal))
      waitFor(fut)
      signalCounter == value

  test "SIGINT test":
    when defined(windows):
      skip()
    else:
      check test(SIGINT, 31337) == true
  test "SIGTERM test":
    when defined(windows):
      skip()
    else:
      check test(SIGTERM, 65537) == true
