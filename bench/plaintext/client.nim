#              Asyncdispatch2 Test Suite
#                 (c) Copyright 2018
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

import strutils, unittest, os, times
import ../../asyncdispatch2

const
  numberOfClients = 256
  numberOfReqs = 1_000_000
  pipelineBy = 25
  reqsPerClient =
    (numberOfReqs div pipelineBy) div numberOfClients
  CRLF = @['\r'.byte, '\L'.byte]

proc readResponse(
    st: StreamTransport,
    readBuff: pointer,
    readSize: Natural) {.async.} =
  ## Discard response.
  while true:
    let read = await st.readUntil(readBuff, readSize, CRLF)
    if read == 2:  # \r\L
      break
  let read = await st.readUntil(readBuff, readSize, CRLF)
  assert read == "Hello, World!\r\L".len

proc pipeline(st: StreamTransport, reqs: int) {.async.} =
  var req =
    "GET / HTTP/1.1\r\L" &
    "Host: 127.0.0.1\r\L\r\L"
  var readBuff = newString(256)
  for _ in 0 .. reqs-1:
    for _ in 0 .. pipelineBy-1:
      let w = await st.write(req)
      assert w == req.len
    for i in 0 .. pipelineBy-1:
      assert(not st.atEof)
      await st.readResponse(addr readBuff[0], readBuff.len)
  st.close()

proc multiPipeline(onAddress: string) {.async.} =
  let ta = initTAddress(onAddress)
  var clients = newSeq[Future[void]](numberOfClients)
  for i in 0 ..< clients.len:
    let st = await connect(ta)
    clients[i] = pipeline(st, reqsPerClient)
  await all(clients)

template benchTime(name, body) =
  var start = cpuTime()
  body
  echo name, "\nRequests/sec: ", numberOfReqs.float / (cpuTime() - start)

when isMainModule:
  doAssert reqsPerClient > 0
  benchTime("asyncdispatch2"):
    waitFor multiPipeline("127.0.0.1:8885")
  benchTime("asyncnet"):
    waitFor multiPipeline("127.0.0.1:8886")
  # http does not support pipelining
