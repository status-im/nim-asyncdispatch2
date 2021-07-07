#              Asyncdispatch2 Test Suite
#                 (c) Copyright 2018
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

import strutils

import ../../asyncdispatch2

type
  KeyboardInterruptError = object of Exception

proc handleCtrlC() {.noconv.} =
  raise newException(KeyboardInterruptError, "Keyboard Interrupt")
setControlCHook(handleCtrlC)

const
  CRLF = @['\r'.byte, '\L'.byte]

proc readHeaders(
    st: StreamTransport,
    readBuff: pointer,
    readSize: Natural) {.async.} =
  ## Discard headers
  while true:
    let read = await st.readUntil(readBuff, readSize, CRLF)
    if read == 2:  # \r\L
      break

proc handleConnection(
    ss: StreamServer,
    st: StreamTransport) {.async.} =
  var resp =
    "HTTP/1.1 200 OK\r\L" &
    "Content-Length: 15\r\L\r\L" &
    "Hello, World!\r\L"
  var readBuff: array[256, byte]
  while not st.atEof:
    await st.readHeaders(addr readBuff[0], readBuff.len)
    let w = await st.write(addr resp[0], resp.len)
    assert w == resp.len
  st.close()

proc serve(onAddress: string) =
  let
    ta = initTAddress(onAddress)
    svr = createStreamServer(ta, handleConnection, {ReuseAddr}, backlog = 128)
  svr.start()
  try:
    waitFor svr.join()
  finally:
    echo "bye!"
    svr.stop()
    svr.close()

when isMainModule:
  serve("127.0.0.1:8885")
