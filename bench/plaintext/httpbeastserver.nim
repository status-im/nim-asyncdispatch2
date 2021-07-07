#              Asyncdispatch2 Test Suite
#                 (c) Copyright 2018
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

import options, asyncdispatch

import httpbeast

proc onRequest(req: Request): Future[void] =
  assert req.path.get() == "/"
  req.send("Hello, World!\r\L")

run(onRequest, Settings(port: Port(8887)))
