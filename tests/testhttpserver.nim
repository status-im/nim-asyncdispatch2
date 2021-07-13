#                Chronos Test Suite
#            (c) Copyright 2021-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import std/[strutils, algorithm, strutils]
import unittest2
import ../chronos, ../chronos/apps/http/httpserver,
       ../chronos/apps/http/httpcommon
import stew/base10

when defined(nimHasUsed): {.used.}

suite "HTTP server testing suite":
  type
    TooBigTest = enum
      GetBodyTest, ConsumeBodyTest, PostUrlTest, PostMultipartTest

  proc httpClient(address: TransportAddress,
                  data: string): Future[string] {.async.} =
    var transp: StreamTransport
    try:
      transp = await connect(address)
      if len(data) > 0:
        let wres {.used.} = await transp.write(data)
      var rres = await transp.read()
      return bytesToString(rres)
    except CatchableError:
      return "EXCEPTION"
    finally:
      if not(isNil(transp)):
        await closeWait(transp)

  proc testTooBigBodyChunked(address: TransportAddress,
                             operation: TooBigTest): Future[bool] {.async.} =
    var serverRes = false
    proc process(r: RequestFence): Future[HttpResponseRef] {.
         async.} =
      if r.isOk():
        let request = r.get()
        try:
          case operation
          of GetBodyTest:
            let body {.used.} = await request.getBody()
          of ConsumeBodyTest:
            await request.consumeBody()
          of PostUrlTest:
            let ptable {.used.} = await request.post()
          of PostMultipartTest:
            let ptable {.used.} = await request.post()
        except HttpCriticalError as exc:
          if exc.code == Http413:
            serverRes = true
          # Reraising exception, because processor should properly handle it.
          raise exc
      else:
        return dumbResponse()

    let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
    let res = HttpServerRef.new(address, process,
                                maxRequestBodySize = 10,
                                socketFlags = socketFlags)
    if res.isErr():
      return false

    let server = res.get()
    server.start()

    let request =
      case operation
      of GetBodyTest, ConsumeBodyTest, PostUrlTest:
        "POST / HTTP/1.0\r\n" &
        "Content-Type: application/x-www-form-urlencoded\r\n" &
        "Transfer-Encoding: chunked\r\n" &
        "Cookie: 2\r\n\r\n" &
        "5\r\na=a&b\r\n5\r\n=b&c=\r\n4\r\nc&d=\r\n4\r\n%D0%\r\n" &
        "2\r\n9F\r\n0\r\n\r\n"
      of PostMultipartTest:
        "POST / HTTP/1.0\r\n" &
        "Host: 127.0.0.1:30080\r\n" &
        "Transfer-Encoding: chunked\r\n" &
        "Content-Type: multipart/form-data; boundary=f98f0\r\n\r\n" &
        "3b\r\n--f98f0\r\nContent-Disposition: form-data; name=\"key1\"" &
        "\r\n\r\nA\r\n\r\n" &
        "3b\r\n--f98f0\r\nContent-Disposition: form-data; name=\"key2\"" &
        "\r\n\r\nB\r\n\r\n" &
        "3b\r\n--f98f0\r\nContent-Disposition: form-data; name=\"key3\"" &
        "\r\n\r\nC\r\n\r\n" &
        "b\r\n--f98f0--\r\n\r\n" &
        "0\r\n\r\n"

    let data = await httpClient(address, request)
    await server.stop()
    await server.closeWait()
    return serverRes and (data.startsWith("HTTP/1.1 413"))

  test "Request headers timeout test":
    proc testTimeout(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          let request = r.get()
          return await request.respond(Http200, "TEST_OK", HttpTable.init())
        else:
          if r.error().error == HTTPServerError.TimeoutError:
            serverRes = true
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process, socketFlags = socketFlags,
                                  httpHeadersTimeout = 100.milliseconds)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let data = await httpClient(address, "")
      await server.stop()
      await server.closeWait()
      return serverRes and (data.startsWith("HTTP/1.1 408"))

    check waitFor(testTimeout(initTAddress("127.0.0.1:30080"))) == true

  test "Empty headers test":
    proc testEmpty(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          let request = r.get()
          return await request.respond(Http200, "TEST_OK", HttpTable.init())
        else:
          if r.error().error == HTTPServerError.CriticalError:
            serverRes = true
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process, socketFlags = socketFlags)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let data = await httpClient(address, "\r\n\r\n")
      await server.stop()
      await server.closeWait()
      return serverRes and (data.startsWith("HTTP/1.1 400"))

    check waitFor(testEmpty(initTAddress("127.0.0.1:30080"))) == true

  test "Too big headers test":
    proc testTooBig(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          let request = r.get()
          return await request.respond(Http200, "TEST_OK", HttpTable.init())
        else:
          if r.error().error == HTTPServerError.CriticalError:
            serverRes = true
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process,
                                  maxHeadersSize = 10,
                                  socketFlags = socketFlags)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let data = await httpClient(address, "GET / HTTP/1.1\r\n\r\n")
      await server.stop()
      await server.closeWait()
      return serverRes and (data.startsWith("HTTP/1.1 431"))

    check waitFor(testTooBig(initTAddress("127.0.0.1:30080"))) == true

  test "Too big request body test (content-length)":
    proc testTooBigBody(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          discard
        else:
          if r.error().error == HTTPServerError.CriticalError:
            serverRes = true
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process,
                                  maxRequestBodySize = 10,
                                  socketFlags = socketFlags)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let request = "GET / HTTP/1.1\r\nContent-Length: 20\r\n\r\n"
      let data = await httpClient(address, request)
      await server.stop()
      await server.closeWait()
      return serverRes and (data.startsWith("HTTP/1.1 413"))

    check waitFor(testTooBigBody(initTAddress("127.0.0.1:30080"))) == true

  test "Too big request body test (getBody()/chunked encoding)":
    check:
      waitFor(testTooBigBodyChunked(initTAddress("127.0.0.1:30080"),
              GetBodyTest)) == true

  test "Too big request body test (consumeBody()/chunked encoding)":
    check:
      waitFor(testTooBigBodyChunked(initTAddress("127.0.0.1:30080"),
              ConsumeBodyTest)) == true

  test "Too big request body test (post()/urlencoded/chunked encoding)":
    check:
      waitFor(testTooBigBodyChunked(initTAddress("127.0.0.1:30080"),
              PostUrlTest)) == true

  test "Too big request body test (post()/multipart/chunked encoding)":
    check:
      waitFor(testTooBigBodyChunked(initTAddress("127.0.0.1:30080"),
              PostMultipartTest)) == true

  test "Query arguments test":
    proc testQuery(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          let request = r.get()
          var kres = newSeq[string]()
          for k, v in request.query.stringItems():
            kres.add(k & ":" & v)
          sort(kres)
          serverRes = true
          return await request.respond(Http200, "TEST_OK:" & kres.join(":"),
                                       HttpTable.init())
        else:
          serverRes = false
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process,
                                  socketFlags = socketFlags)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let data1 = await httpClient(address,
                                  "GET /?a=1&a=2&b=3&c=4 HTTP/1.0\r\n\r\n")
      let data2 = await httpClient(address,
              "GET /?a=%D0%9F&%D0%A4=%D0%91&b=%D0%A6&c=%D0%AE HTTP/1.0\r\n\r\n")
      await server.stop()
      await server.closeWait()
      let r = serverRes and
              (data1.find("TEST_OK:a:1:a:2:b:3:c:4") >= 0) and
              (data2.find("TEST_OK:a:П:b:Ц:c:Ю:Ф:Б") >= 0)
      return r

    check waitFor(testQuery(initTAddress("127.0.0.1:30080"))) == true

  test "Headers test":
    proc testHeaders(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          let request = r.get()
          var kres = newSeq[string]()
          for k, v in request.headers.stringItems():
            kres.add(k & ":" & v)
          sort(kres)
          serverRes = true
          return await request.respond(Http200, "TEST_OK:" & kres.join(":"),
                                       HttpTable.init())
        else:
          serverRes = false
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process,
                                  socketFlags = socketFlags)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let message =
        "GET / HTTP/1.0\r\n" &
        "Host: www.google.com\r\n" &
        "Content-Type: text/html\r\n" &
        "Expect: 100-continue\r\n" &
        "Cookie: 1\r\n" &
        "Cookie: 2\r\n\r\n"
      let expect = "TEST_OK:content-type:text/html:cookie:1:cookie:2" &
                   ":expect:100-continue:host:www.google.com"
      let data = await httpClient(address, message)
      await server.stop()
      await server.closeWait()
      return serverRes and (data.find(expect) >= 0)

    check waitFor(testHeaders(initTAddress("127.0.0.1:30080"))) == true

  test "POST arguments (urlencoded/content-length) test":
    proc testPostUrl(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          var kres = newSeq[string]()
          let request = r.get()
          if request.meth in PostMethods:
            let post = await request.post()
            for k, v in post.stringItems():
              kres.add(k & ":" & v)
            sort(kres)
            serverRes = true
          return await request.respond(Http200, "TEST_OK:" & kres.join(":"),
                                       HttpTable.init())
        else:
          serverRes = false
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process,
                                  socketFlags = socketFlags)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let message =
        "POST / HTTP/1.0\r\n" &
        "Content-Type: application/x-www-form-urlencoded\r\n" &
        "Content-Length: 20\r\n" &
        "Cookie: 2\r\n\r\n" &
        "a=a&b=b&c=c&d=%D0%9F"
      let data = await httpClient(address, message)
      let expect = "TEST_OK:a:a:b:b:c:c:d:П"
      await server.stop()
      await server.closeWait()
      return serverRes and (data.find(expect) >= 0)

    check waitFor(testPostUrl(initTAddress("127.0.0.1:30080"))) == true

  test "POST arguments (urlencoded/chunked encoding) test":
    proc testPostUrl2(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          var kres = newSeq[string]()
          let request = r.get()
          if request.meth in PostMethods:
            let post = await request.post()
            for k, v in post.stringItems():
              kres.add(k & ":" & v)
            sort(kres)
            serverRes = true
          return await request.respond(Http200, "TEST_OK:" & kres.join(":"),
                                       HttpTable.init())
        else:
          serverRes = false
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process,
                                  socketFlags = socketFlags)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let message =
        "POST / HTTP/1.0\r\n" &
        "Content-Type: application/x-www-form-urlencoded\r\n" &
        "Transfer-Encoding: chunked\r\n" &
        "Cookie: 2\r\n\r\n" &
        "5\r\na=a&b\r\n5\r\n=b&c=\r\n4\r\nc&d=\r\n4\r\n%D0%\r\n" &
        "2\r\n9F\r\n0\r\n\r\n"
      let data = await httpClient(address, message)
      let expect = "TEST_OK:a:a:b:b:c:c:d:П"
      await server.stop()
      await server.closeWait()
      return serverRes and (data.find(expect) >= 0)

    check waitFor(testPostUrl2(initTAddress("127.0.0.1:30080"))) == true

  test "POST arguments (multipart/content-length) test":
    proc testPostMultipart(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          var kres = newSeq[string]()
          let request = r.get()
          if request.meth in PostMethods:
            let post = await request.post()
            for k, v in post.stringItems():
              kres.add(k & ":" & v)
            sort(kres)
            serverRes = true
          return await request.respond(Http200, "TEST_OK:" & kres.join(":"),
                                       HttpTable.init())
        else:
          serverRes = false
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process,
                                  socketFlags = socketFlags)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let message =
        "POST / HTTP/1.0\r\n" &
        "Host: 127.0.0.1:30080\r\n" &
        "User-Agent: curl/7.55.1\r\n" &
        "Accept: */*\r\n" &
        "Content-Length: 343\r\n" &
        "Content-Type: multipart/form-data; " &
        "boundary=------------------------ab5706ba6f80b795\r\n\r\n" &
        "--------------------------ab5706ba6f80b795\r\n" &
        "Content-Disposition: form-data; name=\"key1\"\r\n\r\n" &
        "value1\r\n" &
        "--------------------------ab5706ba6f80b795\r\n" &
        "Content-Disposition: form-data; name=\"key2\"\r\n\r\n" &
        "value2\r\n" &
        "--------------------------ab5706ba6f80b795\r\n" &
        "Content-Disposition: form-data; name=\"key2\"\r\n\r\n" &
        "value4\r\n" &
        "--------------------------ab5706ba6f80b795--\r\n"
      let data = await httpClient(address, message)
      let expect = "TEST_OK:key1:value1:key2:value2:key2:value4"
      await server.stop()
      await server.closeWait()
      return serverRes and (data.find(expect) >= 0)

    check waitFor(testPostMultipart(initTAddress("127.0.0.1:30080"))) == true

  test "POST arguments (multipart/chunked encoding) test":
    proc testPostMultipart2(address: TransportAddress): Future[bool] {.async.} =
      var serverRes = false
      proc process(r: RequestFence): Future[HttpResponseRef] {.
           async.} =
        if r.isOk():
          var kres = newSeq[string]()
          let request = r.get()
          if request.meth in PostMethods:
            let post = await request.post()
            for k, v in post.stringItems():
              kres.add(k & ":" & v)
            sort(kres)
          serverRes = true
          return await request.respond(Http200, "TEST_OK:" & kres.join(":"),
                                       HttpTable.init())
        else:
          serverRes = false
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process,
                                  socketFlags = socketFlags)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      let message =
        "POST / HTTP/1.0\r\n" &
        "Host: 127.0.0.1:30080\r\n" &
        "Transfer-Encoding: chunked\r\n" &
        "Content-Type: multipart/form-data; boundary=---" &
        "---------------------f98f0e32c55fa2ae\r\n\r\n" &
        "271\r\n" &
        "--------------------------f98f0e32c55fa2ae\r\n" &
        "Content-Disposition: form-data; name=\"key1\"\r\n\r\n" &
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" &
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\r\n" &
        "--------------------------f98f0e32c55fa2ae\r\n" &
        "Content-Disposition: form-data; name=\"key2\"\r\n\r\n" &
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" &
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\r\n" &
        "--------------------------f98f0e32c55fa2ae\r\n" &
        "Content-Disposition: form-data; name=\"key2\"\r\n\r\n" &
        "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC" &
        "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\r\n" &
        "--------------------------f98f0e32c55fa2ae--\r\n" &
        "\r\n0\r\n\r\n"

      let data = await httpClient(address, message)
      let expect = "TEST_OK:key1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" &
                   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" &
                   "AAAAA:key2:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" &
                   "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" &
                   "BBB:key2:CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC" &
                   "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
      await server.stop()
      await server.closeWait()
      return serverRes and (data.find(expect) >= 0)

    check waitFor(testPostMultipart2(initTAddress("127.0.0.1:30080"))) == true

  test "drop() connections test":
    const ClientsCount = 10

    proc testHTTPdrop(address: TransportAddress): Future[bool] {.async.} =
      var eventWait = newAsyncEvent()
      var eventContinue = newAsyncEvent()
      var count = 0

      proc process(r: RequestFence): Future[HttpResponseRef] {.async.} =
        if r.isOk():
          let request = r.get()
          inc(count)
          if count == ClientsCount:
            eventWait.fire()
          await eventContinue.wait()
          return await request.respond(Http404, "", HttpTable.init())
        else:
          return dumbResponse()

      let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
      let res = HttpServerRef.new(address, process,
                                  socketFlags = socketFlags,
                                  maxConnections = 100)
      if res.isErr():
        return false

      let server = res.get()
      server.start()

      var clients: seq[Future[string]]
      let message = "GET / HTTP/1.0\r\nHost: https://127.0.0.1:80\r\n\r\n"
      for i in 0 ..< ClientsCount:
        var clientFut = httpClient(address, message)
        if clientFut.finished():
          return false
        clients.add(clientFut)
      # Waiting for all clients to connect to the server
      await eventWait.wait()
      # Dropping
      await server.closeWait()
      # We are firing second event to unblock client loops, but this loops
      # must be already cancelled.
      eventContinue.fire()
      # Now all clients should be dropped
      discard await allFutures(clients).withTimeout(1.seconds)
      for item in clients:
        if item.read() != "":
          return false
      return true

    check waitFor(testHTTPdrop(initTAddress("127.0.0.1:30080"))) == true

  test "Content-Type multipart boundary test":
    const AllowedCharacters = {
      'a' .. 'z', 'A' .. 'Z', '0' .. '9',
      '\'', '(', ')', '+', '_', ',', '-', '.' ,'/', ':', '=', '?'
    }

    const FailureVectors = [
      "",
      "multipart/byteranges; boundary=A",
      "multipart/form-data;",
      "multipart/form-data; boundary",
      "multipart/form-data; boundary=",
      "multipart/form-data; boundaryMore=A",
      "multipart/form-data; charset=UTF-8; boundary",
      "multipart/form-data; charset=UTF-8; boundary=",
      "multipart/form-data; charset=UTF-8; boundary =",
      "multipart/form-data; charset=UTF-8; boundary= ",
      "multipart/form-data; charset=UTF-8; boundaryMore=",
      "multipart/form-data; charset=UTF-8; boundaryMore=A",
      "multipart/form-data; charset=UTF-8; boundaryMore=AAAAAAAAAAAAAAAAAAAA" &
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    ]

    const SuccessVectors = [
      ("multipart/form-data; boundary=A", "A"),
      ("multipart/form-data; charset=UTF-8; boundary=B", "B"),
      ("multipart/form-data; charset=UTF-8; boundary=--------------------" &
       "--------------------------------------------------", "-----------" &
       "-----------------------------------------------------------"),
      ("multipart/form-data; boundary=--------------------" &
       "--------------------------------------------------", "-----------" &
       "-----------------------------------------------------------"),
      ("multipart/form-data; boundary=--------------------" &
       "--------------------------------------------------; charset=UTF-8",
       "-----------------------------------------------------------------" &
       "-----"),
      ("multipart/form-data; boundary=ABCDEFGHIJKLMNOPQRST" &
       "UVWXYZabcdefghijklmnopqrstuvwxyz0123456789'()+_,-.; charset=UTF-8",
       "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'()" &
       "+_,-."),
      ("multipart/form-data; boundary=ABCDEFGHIJKLMNOPQRST" &
       "UVWXYZabcdefghijklmnopqrstuvwxyz0123456789'()+?=:/; charset=UTF-8",
       "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'()" &
       "+?=:/"),
      ("multipart/form-data; charset=UTF-8; boundary=ABCDEFGHIJKLMNOPQRST" &
       "UVWXYZabcdefghijklmnopqrstuvwxyz0123456789'()+_,-.",
       "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'()" &
       "+_,-."),
      ("multipart/form-data; charset=UTF-8; boundary=ABCDEFGHIJKLMNOPQRST" &
       "UVWXYZabcdefghijklmnopqrstuvwxyz0123456789'()+?=:/",
       "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'()" &
       "+?=:/")
    ]

    for i in 0 ..< 256:
      let boundary = "multipart/form-data; boundary=" & $char(i)
      if char(i) in AllowedCharacters:
        check getMultipartBoundary([boundary]).isOk()
      else:
        check getMultipartBoundary([boundary]).isErr()

    check:
      getMultipartBoundary([]).isErr()
      getMultipartBoundary(["multipart/form-data; boundary=A",
                            "multipart/form-data; boundary=B"]).isErr()
    for item in FailureVectors:
      check getMultipartBoundary([item]).isErr()
    for item in SuccessVectors:
      let res = getMultipartBoundary([item[0]])
      check:
        res.isOk()
        item[1] == res.get()

  test "HttpTable integer parser test":
    const TestVectors = [
      ("", 0'u64), ("0", 0'u64), ("-0", 0'u64), ("0-", 0'u64),
      ("01", 1'u64), ("001", 1'u64), ("0000000000001", 1'u64),
      ("18446744073709551615", 0xFFFF_FFFF_FFFF_FFFF'u64),
      ("18446744073709551616", 0'u64),
      ("99999999999999999999", 0'u64),
      ("999999999999999999999999999999999999", 0'u64),
      ("FFFFFFFFFFFFFFFF", 0'u64),
      ("0123456789ABCDEF", 0'u64)
    ]
    for i in 0 ..< 256:
      let res = Base10.decode(uint64, [char(i)])
      if char(i) in {'0' .. '9'}:
        check:
          res.isOk()
          res.get() == uint64(i - ord('0'))
      else:
        check res.isErr()

    for item in TestVectors:
      var ht = HttpTable.init([("test", item[0])])
      let value = ht.getInt("test")
      check value == item[1]

  test "HttpTable behavior test":
    var table1 = HttpTable.init()
    var table2 = HttpTable.init([("Header1", "value1"), ("Header2", "value2")])
    check:
      table1.isEmpty() == true
      table2.isEmpty() == false

    table1.add("Header1", "value1")
    table1.add("Header2", "value2")
    table1.add("HEADER2", "VALUE3")
    check:
      table1.getList("HeAdEr2") == @["value2", "VALUE3"]
      table1.getString("HeAdEr2") == "value2,VALUE3"
      table2.getString("HEADER1") == "value1"
      table1.count("HEADER2") == 2
      table1.count("HEADER1") == 1
      table1.getLastString("HEADER1") == "value1"
      table1.getLastString("HEADER2") == "VALUE3"
      "header1" in table1 == true
      "HEADER1" in table1 == true
      "header2" in table1 == true
      "HEADER2" in table1 == true
      "HEADER3" in table1 == false

    var
      data1: seq[tuple[key: string, value: string]]
      data2: seq[tuple[key: string, value: seq[string]]]
    for key, value in table1.stringItems(true):
      data1.add((key, value))
    for key, value in table1.items(true):
      data2.add((key, value))

    check:
      data1 == @[("Header2", "value2"), ("Header2", "VALUE3"),
                 ("Header1", "value1")]
      data2 == @[("Header2", @["value2", "VALUE3"]),
                 ("Header1", @["value1"])]

    table1.set("header2", "value4")
    check:
      table1.getList("header2") == @["value4"]
      table1.getString("header2") == "value4"
      table1.count("header2") == 1
      table1.getLastString("header2") == "value4"

  test "getTransferEncoding() test":
    var encodings = [
      "chunked", "compress", "deflate", "gzip", "identity", "x-gzip"
    ]

    const FlagsVectors = [
      {
        TransferEncodingFlags.Identity, TransferEncodingFlags.Chunked,
        TransferEncodingFlags.Compress, TransferEncodingFlags.Deflate,
        TransferEncodingFlags.Gzip
      },
      {
        TransferEncodingFlags.Identity, TransferEncodingFlags.Compress,
        TransferEncodingFlags.Deflate, TransferEncodingFlags.Gzip
      },
      {
        TransferEncodingFlags.Identity, TransferEncodingFlags.Deflate,
        TransferEncodingFlags.Gzip
      },
      { TransferEncodingFlags.Identity, TransferEncodingFlags.Gzip },
      { TransferEncodingFlags.Identity, TransferEncodingFlags.Gzip },
      { TransferEncodingFlags.Gzip },
      { TransferEncodingFlags.Identity }
    ]

    for i in 0 ..< 7:
      var checkEncodings = @encodings
      if i - 1 >= 0:
        for k in 0 .. (i - 1):
          checkEncodings.delete(0)

      while nextPermutation(checkEncodings):
        let res1 = getTransferEncoding([checkEncodings.join(", ")])
        let res2 = getTransferEncoding([checkEncodings.join(",")])
        let res3 = getTransferEncoding([checkEncodings.join("")])
        let res4 = getTransferEncoding([checkEncodings.join(" ")])
        let res5 = getTransferEncoding([checkEncodings.join(" , ")])
        check:
          res1.isOk()
          res1.get() == FlagsVectors[i]
          res2.isOk()
          res2.get() == FlagsVectors[i]
          res3.isErr()
          res4.isErr()
          res5.isOk()
          res5.get() == FlagsVectors[i]

    check:
      getTransferEncoding([]).tryGet() == { TransferEncodingFlags.Identity }
      getTransferEncoding(["", ""]).tryGet() ==
        { TransferEncodingFlags.Identity }

  test "getContentEncoding() test":
    var encodings = [
      "br", "compress", "deflate", "gzip", "identity", "x-gzip"
    ]

    const FlagsVectors = [
      {
        ContentEncodingFlags.Identity, ContentEncodingFlags.Br,
        ContentEncodingFlags.Compress, ContentEncodingFlags.Deflate,
        ContentEncodingFlags.Gzip
      },
      {
        ContentEncodingFlags.Identity, ContentEncodingFlags.Compress,
        ContentEncodingFlags.Deflate, ContentEncodingFlags.Gzip
      },
      {
        ContentEncodingFlags.Identity, ContentEncodingFlags.Deflate,
        ContentEncodingFlags.Gzip
      },
      { ContentEncodingFlags.Identity, ContentEncodingFlags.Gzip },
      { ContentEncodingFlags.Identity, ContentEncodingFlags.Gzip },
      { ContentEncodingFlags.Gzip },
      { ContentEncodingFlags.Identity }
    ]

    for i in 0 ..< 7:
      var checkEncodings = @encodings
      if i - 1 >= 0:
        for k in 0 .. (i - 1):
          checkEncodings.delete(0)

      while nextPermutation(checkEncodings):
        let res1 = getContentEncoding([checkEncodings.join(", ")])
        let res2 = getContentEncoding([checkEncodings.join(",")])
        let res3 = getContentEncoding([checkEncodings.join("")])
        let res4 = getContentEncoding([checkEncodings.join(" ")])
        let res5 = getContentEncoding([checkEncodings.join(" , ")])
        check:
          res1.isOk()
          res1.get() == FlagsVectors[i]
          res2.isOk()
          res2.get() == FlagsVectors[i]
          res3.isErr()
          res4.isErr()
          res5.isOk()
          res5.get() == FlagsVectors[i]

    check:
      getContentEncoding([]).tryGet() == { ContentEncodingFlags.Identity }
      getContentEncoding(["", ""]).tryGet() == { ContentEncodingFlags.Identity }

  test "queryParams() test":
    const Vectors = [
      ("id=1&id=2&id=3&id=4", {}, "id:1,id:2,id:3,id:4"),
      ("id=1,2,3,4", {}, "id:1,2,3,4"),
      ("id=1%2C2%2C3%2C4", {}, "id:1,2,3,4"),
      ("id=", {}, "id:"),
      ("id=&id=", {}, "id:,id:"),
      ("id=1&id=2&id=3&id=4", {QueryParamsFlag.CommaSeparatedArray},
       "id:1,id:2,id:3,id:4"),
      ("id=1,2,3,4", {QueryParamsFlag.CommaSeparatedArray},
       "id:1,id:2,id:3,id:4"),
      ("id=1%2C2%2C3%2C4", {QueryParamsFlag.CommaSeparatedArray},
       "id:1,id:2,id:3,id:4"),
      ("id=", {QueryParamsFlag.CommaSeparatedArray}, "id:"),
      ("id=&id=", {QueryParamsFlag.CommaSeparatedArray}, "id:,id:"),
      ("id=,", {QueryParamsFlag.CommaSeparatedArray}, "id:,id:"),
      ("id=,,", {QueryParamsFlag.CommaSeparatedArray}, "id:,id:,id:"),
      ("id=1&id=2&id=3,4,5,6&id=7%2C8%2C9%2C10",
       {QueryParamsFlag.CommaSeparatedArray},
       "id:1,id:2,id:3,id:4,id:5,id:6,id:7,id:8,id:9,id:10")
    ]

    proc toString(ht: HttpTable): string =
      var res: seq[string]
      for key, value in ht.items():
        for item in value:
          res.add(key & ":" & item)
      res.join(",")

    for vector in Vectors:
      var table = HttpTable.init()
      for key, value in queryParams(vector[0], vector[1]):
        table.add(key, value)
      check toString(table) == vector[2]

  test "Leaks test":
    proc getTrackerLeaks(trackerName: string): bool =
      let tracker = getTracker(trackerName)
      if isNil(tracker):
        false
      else:
        if tracker.isLeaked():
          echo tracker.dump()
          true
        else:
          false

    check:
      getTrackerLeaks("async.stream.reader") == false
      getTrackerLeaks("async.stream.writer") == false
      getTrackerLeaks("stream.server") == false
      getTrackerLeaks("stream.transport") == false
