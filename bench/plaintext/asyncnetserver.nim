#              Asyncdispatch2 Test Suite
#                 (c) Copyright 2018
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

import asyncnet, asyncdispatch

type
  KeyboardInterruptError = object of Exception

proc handleCtrlC() {.noconv.} =
  raise newException(KeyboardInterruptError, "Keyboard Interrupt")
setControlCHook(handleCtrlC)

proc handleConnection(client: AsyncSocket) {.async.} =
  var resp =
    "HTTP/1.1 200 OK\r\L" &
    "Content-Length: 15\r\L\r\L" &
    "Hello, World!\r\L"
  var line = newFutureVar[string]("asyncnet.recvLine")
  line.mget() = newString(256)
  while true:
    line.mget().setLen(0)
    await client.recvLineInto(line)
    if line.mget().len == 0:
      break
    if line.mget().len == 2:  # \r\L
      await client.send(resp)
  client.close()

proc serve() {.async.} =
  var server = newAsyncSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(8886))
  server.listen()
  try:
    while true:
      let client = await server.accept()
      asyncCheck handleConnection(client)
  finally:
    server.close()

when isMainModule:
  waitFor serve()
