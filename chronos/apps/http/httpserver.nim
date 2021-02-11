#
#        Chronos HTTP/S server implementation
#             (c) Copyright 2021-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import std/[tables, options, uri, strutils]
import stew/results, httputils
import ../../asyncloop, ../../asyncsync
import ../../streams/[asyncstream, boundstream, chunkstream, tlsstream]
import httptable, httpcommon, multipart
export httptable, httpcommon, multipart, tlsstream, asyncstream

type
  HttpServerFlags* {.pure.} = enum
    Secure, NoExpectHandler

  HttpServerError* {.pure.} = enum
    TimeoutError, CatchableError, RecoverableError, CriticalError

  HttpServerState* {.pure.} = enum
    ServerRunning, ServerStopped, ServerClosed

  HttpProcessError* = object
    error*: HTTPServerError
    code*: HttpCode
    exc*: ref CatchableError
    remote*: TransportAddress

  RequestFence*[T] = Result[T, HttpProcessError]

  HttpRequestFlags* {.pure.} = enum
    BoundBody, UnboundBody, MultipartForm, UrlencodedForm,
    ClientExpect

  HttpResponseFlags* {.pure.} = enum
    KeepAlive, Chunked

  HttpResponseState* {.pure.} = enum
    Empty, Prepared, Sending, Finished, Failed, Cancelled, Dumb

  HttpProcessCallback* =
    proc(req: RequestFence[HttpRequestRef]): Future[HttpResponseRef] {.gcsafe.}

  HttpServer* = object of RootObj
    instance*: StreamServer
    # semaphore*: AsyncSemaphore
    maxConnections*: int
    backlogSize: int
    baseUri*: Uri
    flags*: set[HttpServerFlags]
    socketFlags*: set[ServerFlags]
    secureFlags*: set[TLSFlags]
    connections*: Table[string, Future[void]]
    acceptLoop*: Future[void]
    lifetime*: Future[void]
    headersTimeout: Duration
    maxHeadersSize: int
    maxRequestBodySize: int
    processCallback: HttpProcessCallback
    tlsPrivateKey: TLSPrivateKey
    tlsCertificate: TLSCertificate

  HttpServerRef* = ref HttpServer

  HttpRequest* = object of RootObj
    headers*: HttpTable
    query*: HttpTable
    postTable: Option[HttpTable]
    rawPath*: string
    rawQuery*: string
    uri*: Uri
    scheme*: string
    version*: HttpVersion
    meth*: HttpMethod
    contentEncoding*: set[ContentEncodingFlags]
    transferEncoding*: set[TransferEncodingFlags]
    requestFlags*: set[HttpRequestFlags]
    contentLength: int
    connection*: HttpConnectionRef
    response*: Option[HttpResponseRef]

  HttpRequestRef* = ref HttpRequest

  HttpResponse* = object of RootObj
    status*: HttpCode
    version*: HttpVersion
    headersTable: HttpTable
    body: seq[byte]
    flags: set[HttpResponseFlags]
    state: HttpResponseState
    connection*: HttpConnectionRef
    chunkedWriter: AsyncStreamWriter

  HttpResponseRef* = ref HttpResponse

  HttpConnection* = object of RootObj
    server*: HttpServerRef
    transp: StreamTransport
    mainReader: AsyncStreamReader
    mainWriter: AsyncStreamWriter
    tlsStream: TLSAsyncStream
    reader*: AsyncStreamReader
    writer*: AsyncStreamWriter
    buffer: seq[byte]

  HttpConnectionRef* = ref HttpConnection

proc init(htype: typedesc[HttpProcessError], error: HTTPServerError,
          exc: ref CatchableError, remote: TransportAddress,
          code: HttpCode): HttpProcessError {.raises: [Defect].} =
  HttpProcessError(error: error, exc: exc, remote: remote, code: code)

proc new*(htype: typedesc[HttpServerRef],
          address: TransportAddress,
          processCallback: HttpProcessCallback,
          serverFlags: set[HttpServerFlags] = {},
          socketFlags: set[ServerFlags] = {ReuseAddr},
          serverUri = Uri(),
          tlsPrivateKey: TLSPrivateKey = nil,
          tlsCertificate: TLSCertificate = nil,
          secureFlags: set[TLSFlags] = {},
          maxConnections: int = -1,
          bufferSize: int = 4096,
          backlogSize: int = 100,
          httpHeadersTimeout = 10.seconds,
          maxHeadersSize: int = 8192,
          maxRequestBodySize: int = 1_048_576): HttpResult[HttpServerRef] =

  if HttpServerFlags.Secure in serverFlags:
    if isNil(tlsPrivateKey) or isNil(tlsCertificate):
      return err("PrivateKey or Certificate is missing")

  var res = HttpServerRef(
    maxConnections: maxConnections,
    headersTimeout: httpHeadersTimeout,
    maxHeadersSize: maxHeadersSize,
    maxRequestBodySize: maxRequestBodySize,
    processCallback: processCallback,
    backLogSize: backLogSize,
    flags: serverFlags,
    socketFlags: socketFlags,
    tlsPrivateKey: tlsPrivateKey,
    tlsCertificate: tlsCertificate
  )

  res.baseUri =
    try:
      if len(serverUri.hostname) > 0 and isAbsolute(serverUri):
        serverUri
      else:
        if HttpServerFlags.Secure in serverFlags:
          parseUri("https://" & $address & "/")
        else:
          parseUri("http://" & $address & "/")
    except TransportAddressError as exc:
      return err(exc.msg)

  try:
    res.instance = createStreamServer(address, flags = socketFlags,
                                      bufferSize = bufferSize,
                                      backlog = backlogSize)
    # if maxConnections > 0:
    #   res.semaphore = newAsyncSemaphore(maxConnections)
    res.lifetime = newFuture[void]("http.server.lifetime")
    res.connections = initTable[string, Future[void]]()
    return ok(res)
  except TransportOsError as exc:
    return err(exc.msg)
  except CatchableError as exc:
    return err(exc.msg)

proc getResponse*(req: HttpRequestRef): HttpResponseRef {.raises: [Defect].} =
  if req.response.isNone():
    var resp = HttpResponseRef(
      status: Http200,
      state: HttpResponseState.Empty,
      version: req.version,
      headersTable: HttpTable.init(),
      connection: req.connection,
      flags: if req.version == HttpVersion11: {KeepAlive} else: {}
    )
    req.response = some(resp)
    resp
  else:
    req.response.get()

proc dumbResponse*(): HttpResponseRef {.raises: [Defect].} =
  ## Create an empty response to return when request processor got no request.
  HttpResponseRef(state: HttpResponseState.Dumb, version: HttpVersion11)

proc getId(transp: StreamTransport): string {.inline.} =
  ## Returns string unique transport's identifier as string.
  $transp.remoteAddress() & "_" & $transp.localAddress()

proc hasBody*(request: HttpRequestRef): bool {.raises: [Defect].} =
  ## Returns ``true`` if request has body.
  request.requestFlags * {HttpRequestFlags.BoundBody,
                          HttpRequestFlags.UnboundBody} != {}

proc prepareRequest(conn: HttpConnectionRef,
                    req: HttpRequestHeader): HttpResultCode[HttpRequestRef] {.
     raises: [Defect].}=
  var request = HttpRequestRef(connection: conn)

  if req.version notin {HttpVersion10, HttpVersion11}:
    return err(Http505)

  request.scheme =
    if HttpServerFlags.Secure in conn.server.flags:
      "https"
    else:
      "http"

  request.version = req.version
  request.meth = req.meth

  request.rawPath =
    block:
      let res = req.uri()
      if len(res) == 0:
        return err(Http400)
      res

  request.uri =
    if request.rawPath != "*":
      let uri = parseUri(request.rawPath)
      if uri.scheme notin ["http", "https", ""]:
        return err(Http400)
      uri
    else:
      var uri = initUri()
      uri.path = "*"
      uri

  request.query =
    block:
      var table = HttpTable.init()
      for key, value in queryParams(request.uri.query):
        table.add(key, value)
      table

  request.headers =
    block:
      var table = HttpTable.init()
      # Retrieve headers and values
      for key, value in req.headers():
        table.add(key, value)
      # Validating HTTP request headers
      # Some of the headers must be present only once.
      if table.count("content-type") > 1:
        return err(Http400)
      if table.count("content-length") > 1:
        return err(Http400)
      if table.count("transfer-encoding") > 1:
        return err(Http400)
      table

  # Preprocessing "Content-Encoding" header.
  request.contentEncoding =
    block:
      let res = getContentEncoding(request.headers.getList("content-encoding"))
      if res.isErr():
        return err(Http400)
      else:
        res.get()

  # Preprocessing "Transfer-Encoding" header.
  request.transferEncoding =
    block:
      let res = getTransferEncoding(
                                   request.headers.getList("transfer-encoding"))
      if res.isErr():
        return err(Http400)
      else:
        res.get()

  # Almost all HTTP requests could have body (except TRACE), we perform some
  # steps to reveal information about body.
  if "content-length" in request.headers:
    let length = request.headers.getInt("content-length")
    if length > 0:
      if request.meth == MethodTrace:
        return err(Http400)
      if length > uint64(high(int)):
        return err(Http413)
      if length > uint64(conn.server.maxRequestBodySize):
        return err(Http413)
      request.contentLength = int(length)
      request.requestFlags.incl(HttpRequestFlags.BoundBody)
  else:
    if TransferEncodingFlags.Chunked in request.transferEncoding:
      if request.meth == MethodTrace:
        return err(Http400)
      request.requestFlags.incl(HttpRequestFlags.UnboundBody)

  if request.hasBody():
    # If request has body, we going to understand how its encoded.
    const
      UrlEncodedType = "application/x-www-form-urlencoded"
      MultipartType = "multipart/form-data"

    if "content-type" in request.headers:
      let contentType = request.headers.getString("content-type")
      let tmp = strip(contentType).toLowerAscii()
      if tmp.startsWith(UrlEncodedType):
        request.requestFlags.incl(HttpRequestFlags.UrlencodedForm)
      elif tmp.startsWith(MultipartType):
        request.requestFlags.incl(HttpRequestFlags.MultipartForm)

    if "expect" in request.headers:
      let expectHeader = request.headers.getString("expect")
      if strip(expectHeader).toLowerAscii() == "100-continue":
        request.requestFlags.incl(HttpRequestFlags.ClientExpect)

  ok(request)

proc getBodyReader*(request: HttpRequestRef): HttpResult[HttpBodyReader] =
  ## Returns stream's reader instance which can be used to read request's body.
  ##
  ## Please be sure to handle ``Expect`` header properly.
  ##
  ## Streams which was obtained using this procedure must be closed to avoid
  ## leaks.
  if HttpRequestFlags.BoundBody in request.requestFlags:
    let bstream = newBoundedStreamReader(request.connection.reader,
                                         request.contentLength)
    ok(newHttpBodyReader(bstream))
  elif HttpRequestFlags.UnboundBody in request.requestFlags:
    let maxBodySize = request.connection.server.maxRequestBodySize
    let bstream = newBoundedStreamReader(request.connection.reader, maxBodySize,
                                         comparison = BoundCmp.LessOrEqual)
    let cstream = newChunkedStreamReader(bstream)
    ok(newHttpBodyReader(cstream, bstream))
  else:
    err("Request do not have body available")

proc handleExpect*(request: HttpRequestRef) {.async.} =
  ## Handle expectation for ``Expect`` header.
  ## https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Expect
  if HttpServerFlags.NoExpectHandler notin request.connection.server.flags:
    if HttpRequestFlags.ClientExpect in request.requestFlags:
      if request.version == HttpVersion11:
        try:
          let message = $request.version & " " & $Http100 & "\r\n\r\n"
          await request.connection.writer.write(message)
        except CancelledError as exc:
          raise exc
        except AsyncStreamWriteError, AsyncStreamIncompleteError:
          raise newHttpCriticalError("Unable to send `100-continue` response")

proc getBody*(request: HttpRequestRef): Future[seq[byte]] {.async.} =
  ## Obtain request's body as sequence of bytes.
  let res = request.getBodyReader()
  if res.isErr():
    return @[]
  else:
    let reader = res.get()
    try:
      await request.handleExpect()
      return await reader.read()
    except AsyncStreamError:
      if reader.atBound():
        raise newHttpCriticalError("Maximum size of body reached", Http413)
      else:
        raise newHttpCriticalError("Unable to read request's body")
    finally:
      await closeWait(res.get())

proc consumeBody*(request: HttpRequestRef): Future[void] {.async.} =
  ## Consume/discard request's body.
  let res = request.getBodyReader()
  if res.isErr():
    return
  else:
    let reader = res.get()
    try:
      await request.handleExpect()
      discard await reader.consume()
    except AsyncStreamError:
      if reader.atBound():
        raise newHttpCriticalError("Maximum size of body reached", Http413)
      else:
        raise newHttpCriticalError("Unable to read request's body")
    finally:
      await closeWait(res.get())

proc sendErrorResponse(conn: HttpConnectionRef, version: HttpVersion,
                       code: HttpCode, keepAlive = true,
                       datatype = "text/text",
                       databody = ""): Future[bool] {.async.} =
  var answer = $version & " " & $code & "\r\n"
  answer.add("Date: " & httpDate() & "\r\n")
  if len(datatype) > 0:
    answer.add("Content-Type: " & datatype & "\r\n")
  if len(databody) > 0:
    answer.add("Content-Length: " & $len(databody) & "\r\n")
  if keepAlive:
    answer.add("Connection: keep-alive\r\n")
  else:
    answer.add("Connection: close\r\n")
  answer.add("\r\n")
  if len(databody) > 0:
    answer.add(databody)
  try:
    await conn.writer.write(answer)
    return true
  except CancelledError:
    return false
  except AsyncStreamWriteError:
    return false
  except AsyncStreamIncompleteError:
    return false

proc getRequest(conn: HttpConnectionRef): Future[HttpRequestRef] {.async.} =
  try:
    conn.buffer.setLen(conn.server.maxHeadersSize)
    let res = await conn.reader.readUntil(addr conn.buffer[0], len(conn.buffer),
                                          HeadersMark)
    conn.buffer.setLen(res)
    let header = parseRequest(conn.buffer)
    if header.failed():
      raise newHttpCriticalError("Malformed request recieved")
    else:
      let res = prepareRequest(conn, header)
      if res.isErr():
        raise newHttpCriticalError("Invalid request received", res.error)
      else:
        return res.get()
  except AsyncStreamIncompleteError, AsyncStreamReadError:
    raise newHttpDisconnectError()
  except AsyncStreamLimitError:
    raise newHttpCriticalError("Maximum size of request headers reached",
                               Http413)

proc new(ht: typedesc[HttpConnectionRef], server: HttpServerRef,
         transp: StreamTransport): HttpConnectionRef =
  let mainReader = newAsyncStreamReader(transp)
  let mainWriter = newAsyncStreamWriter(transp)
  let tlsStream =
    if HttpServerFlags.Secure in server.flags:
      newTLSServerAsyncStream(mainReader, mainWriter, server.tlsPrivateKey,
                              server.tlsCertificate,
                              minVersion = TLSVersion.TLS12,
                              flags = server.secureFlags)
    else:
      nil

  let reader =
    if isNil(tlsStream):
      mainReader
    else:
      cast[AsyncStreamReader](tlsStream.reader)

  let writer =
    if isNil(tlsStream):
      mainWriter
    else:
      cast[AsyncStreamWriter](tlsStream.writer)

  HttpConnectionRef(
    transp: transp,
    server: server,
    buffer: newSeq[byte](server.maxHeadersSize),
    mainReader: mainReader,
    mainWriter: mainWriter,
    tlsStream: tlsStream,
    reader: reader,
    writer: writer
  )

proc closeWait(conn: HttpConnectionRef) {.async.} =
  if HttpServerFlags.Secure in conn.server.flags:
    # First we will close TLS streams.
    await allFutures(conn.reader.closeWait(), conn.writer.closeWait())

  # After we going to close everything else.
  await allFutures(conn.mainReader.closeWait(), conn.mainWriter.closeWait(),
                   conn.transp.closeWait())

proc closeWait(req: HttpRequestRef) {.async.} =
  if req.response.isSome():
    let resp = req.response.get()
    if (HttpResponseFlags.Chunked in resp.flags) and
       not(isNil(resp.chunkedWriter)):
      await resp.chunkedWriter.closeWait()

proc createConnection(server: HttpServerRef,
                      transp: StreamTransport): Future[HttpConnectionRef] {.
     async.} =
  var conn = HttpConnectionRef.new(server, transp)
  if HttpServerFlags.Secure notin server.flags:
    # Non secure connection
    return conn

  try:
    await handshake(conn.tlsStream)
    return conn
  except CancelledError as exc:
    await conn.closeWait()
    raise exc
  except TLSStreamError:
    await conn.closeWait()
    raise newHttpCriticalError("Unable to establish secure connection")

proc processLoop(server: HttpServerRef, transp: StreamTransport) {.async.} =
  var
    conn: HttpConnectionRef
    connArg: RequestFence[HttpRequestRef]
    runLoop = false

  try:
    conn = await createConnection(server, transp)
    runLoop = true
  except CancelledError:
    # We could be cancelled only when we perform TLS handshake, connection
    server.connections.del(transp.getId())
    return
  except HttpCriticalError as exc:
    let error = HttpProcessError.init(HTTPServerError.CriticalError, exc,
                                      transp.remoteAddress(), exc.code)
    connArg = RequestFence[HttpRequestRef].err(error)
    runLoop = false

  if not(runLoop):
    try:
      # We still want to notify process callback about failure, but we ignore
      # result and swallow all the exceptions.
      discard await server.processCallback(connArg)
    except CancelledError:
      server.connections.del(transp.getId())
      return
    except CatchableError as exc:
      # There should be no exceptions, so we will raise `Defect`.
      raise newHttpDefect("Unexpected exception catched [" & $exc.name & "]")

  var breakLoop = false
  while runLoop:
    var
      arg: RequestFence[HttpRequestRef]
      resp: HttpResponseRef

    try:
      let request = await conn.getRequest().wait(server.headersTimeout)
      arg = RequestFence[HttpRequestRef].ok(request)
    except CancelledError:
      breakLoop = true
    except AsyncTimeoutError as exc:
      let error = HttpProcessError.init(HTTPServerError.TimeoutError, exc,
                                        transp.remoteAddress(), Http408)
      arg = RequestFence[HttpRequestRef].err(error)
    except HttpRecoverableError as exc:
      let error = HttpProcessError.init(HTTPServerError.RecoverableError, exc,
                                        transp.remoteAddress(), exc.code)
      arg = RequestFence[HttpRequestRef].err(error)
    except HttpCriticalError as exc:
      let error = HttpProcessError.init(HTTPServerError.CriticalError, exc,
                                        transp.remoteAddress(), exc.code)
      arg = RequestFence[HttpRequestRef].err(error)
    except HttpDisconnectError:
      # If remote peer disconnected we just exiting loop
      breakLoop = true
    except CatchableError as exc:
      let error = HttpProcessError.init(HTTPServerError.CatchableError, exc,
                                        transp.remoteAddress(), Http500)
      arg = RequestFence[HttpRequestRef].err(error)

    if breakLoop:
      break

    breakLoop = false
    var lastErrorCode: Option[HttpCode]

    try:
      resp = await conn.server.processCallback(arg)
    except CancelledError:
      breakLoop = true
    except HttpCriticalError as exc:
      lastErrorCode = some(exc.code)
    except HttpRecoverableError as exc:
      lastErrorCode = some(exc.code)
    except CatchableError:
      lastErrorCode = some(Http503)

    if breakLoop:
      break

    if arg.isErr():
      let code = arg.error().code
      case arg.error().error
      of HTTPServerError.TimeoutError:
        discard await conn.sendErrorResponse(HttpVersion11, code, false)
      of HTTPServerError.RecoverableError:
        discard await conn.sendErrorResponse(HttpVersion11, code, false)
      of HTTPServerError.CriticalError:
        discard await conn.sendErrorResponse(HttpVersion11, code, false)
      of HTTPServerError.CatchableError:
        discard await conn.sendErrorResponse(HttpVersion11, code, false)
      break
    else:
      let request = arg.get()
      var keepConn = if request.version == HttpVersion11: true else: false
      if lastErrorCode.isNone():
        if isNil(resp):
          # Response was `nil`.
          discard await conn.sendErrorResponse(HttpVersion11, Http404,
                                               false)
        else:
          case resp.state
          of HttpResponseState.Empty:
            # Response was ignored
            discard await conn.sendErrorResponse(HttpVersion11, Http404,
                                                 keepConn)
          of HttpResponseState.Prepared:
            # Response was prepared but not sent.
            discard await conn.sendErrorResponse(HttpVersion11, Http409,
                                                 keepConn)
          else:
            # some data was already sent to the client.
            discard
      else:
        discard await conn.sendErrorResponse(HttpVersion11, lastErrorCode.get(),
                                             false)
      # Closing and releasing all the request resources.
      await request.closeWait()

      if not(keepConn):
        break

  # Connection could be `nil` only when secure handshake is failed.
  if not(isNil(conn)):
    await conn.closeWait()

  server.connections.del(transp.getId())
  # if server.maxConnections > 0:
  #   server.semaphore.release()

proc acceptClientLoop(server: HttpServerRef) {.async.} =
  var breakLoop = false
  while true:
    try:
      # if server.maxConnections > 0:
      #   await server.semaphore.acquire()

      let transp = await server.instance.accept()
      server.connections[transp.getId()] = processLoop(server, transp)

    except CancelledError:
      # Server was stopped
      breakLoop = true
    except TransportOsError:
      # This is some critical unrecoverable error.
      breakLoop = true
    except TransportTooManyError:
      # Non critical error
      breakLoop = false
    except CatchableError:
      # Unexpected error
      breakLoop = true
      discard

    if breakLoop:
      break

proc state*(server: HttpServerRef): HttpServerState {.raises: [Defect].} =
  ## Returns current HTTP server's state.
  if server.lifetime.finished():
    ServerClosed
  else:
    if isNil(server.acceptLoop):
      ServerStopped
    else:
      if server.acceptLoop.finished():
        ServerStopped
      else:
        ServerRunning

proc start*(server: HttpServerRef) =
  ## Starts HTTP server.
  if server.state == ServerStopped:
    server.acceptLoop = acceptClientLoop(server)

proc stop*(server: HttpServerRef) {.async.} =
  ## Stop HTTP server from accepting new connections.
  if server.state == ServerRunning:
    await server.acceptLoop.cancelAndWait()

proc drop*(server: HttpServerRef) {.async.} =
  ## Drop all pending HTTP connections.
  if server.state in {ServerStopped, ServerRunning}:
    discard

proc closeWait*(server: HttpServerRef) {.async.} =
  ## Stop HTTP server and drop all the pending connections.
  if server.state != ServerClosed:
    await server.stop()
    await server.drop()
    await server.instance.closeWait()
    server.lifetime.complete()

proc join*(server: HttpServerRef): Future[void] =
  ## Wait until HTTP server will not be closed.
  var retFuture = newFuture[void]("http.server.join")

  proc continuation(udata: pointer) {.gcsafe.} =
    if not(retFuture.finished()):
      retFuture.complete()

  proc cancellation(udata: pointer) {.gcsafe.} =
    if not(retFuture.finished()):
      server.lifetime.removeCallback(continuation, cast[pointer](retFuture))

  if server.state == ServerClosed:
    retFuture.complete()
  else:
    server.lifetime.addCallback(continuation, cast[pointer](retFuture))
    retFuture.cancelCallback = cancellation

  retFuture

proc getMultipartReader*(req: HttpRequestRef): HttpResult[MultiPartReaderRef] =
  ## Create new MultiPartReader interface for specific request.
  if req.meth in PostMethods:
    if MultipartForm in req.requestFlags:
      let ctype = ? getContentType(req.headers.getList("content-type"))
      if ctype != "multipart/form-data":
        err("Content type is not supported")
      else:
        let boundary = ? getMultipartBoundary(
          req.headers.getList("content-type")
        )
        var stream = ? req.getBodyReader()
        ok(MultiPartReaderRef.new(stream, boundary))
    else:
      err("Request's data is not multipart encoded")
  else:
    err("Request's method do not supports multipart")

proc post*(req: HttpRequestRef): Future[HttpTable] {.async.} =
  ## Return POST parameters
  if req.postTable.isSome():
    return req.postTable.get()
  else:
    if req.meth notin PostMethods:
      return HttpTable.init()

    if UrlencodedForm in req.requestFlags:
      var table = HttpTable.init()
      # getBody() will handle `Expect`.
      var body = await req.getBody()
      # TODO (cheatfate) double copy here, because of `byte` to `char`
      # conversion.
      var strbody = newString(len(body))
      if len(body) > 0:
        copyMem(addr strbody[0], addr body[0], len(body))
      for key, value in queryParams(strbody):
        table.add(key, value)
      req.postTable = some(table)
      return table
    elif MultipartForm in req.requestFlags:
      var table = HttpTable.init()
      let res = getMultipartReader(req)
      if res.isErr():
        raise newHttpCriticalError("Unable to retrieve multipart form data")
      var mpreader = res.get()

      # We must handle `Expect` first.
      try:
        await req.handleExpect()
      except CancelledError as exc:
        await mpreader.closeWait()
        raise exc
      except HttpCriticalError as exc:
        await mpreader.closeWait()
        raise exc

      # Reading multipart/form-data parts.
      var runLoop = true
      while runLoop:
        var part: MultiPart
        try:
          part = await mpreader.readPart()
          var value = await part.getBody()
          # TODO (cheatfate) double copy here, because of `byte` to `char`
          # conversion.
          var strvalue = newString(len(value))
          if len(value) > 0:
            copyMem(addr strvalue[0], addr value[0], len(value))
          table.add(part.name, strvalue)
          await part.closeWait()
        except MultipartEOMError:
          runLoop = false
        except HttpCriticalError as exc:
          if not(part.isEmpty()):
            await part.closeWait()
          await mpreader.closeWait()
          raise exc
        except CancelledError as exc:
          if not(part.isEmpty()):
            await part.closeWait()
          await mpreader.closeWait()
          raise exc
      await mpreader.closeWait()
      req.postTable = some(table)
      return table
    else:
      if HttpRequestFlags.BoundBody in req.requestFlags:
        if req.contentLength != 0:
          raise newHttpCriticalError("Unsupported request body")
        return HttpTable.init()
      elif HttpRequestFlags.UnboundBody in req.requestFlags:
        raise newHttpCriticalError("Unsupported request body")

proc `keepalive=`*(resp: HttpResponseRef, value: bool) =
  doAssert(resp.state == HttpResponseState.Empty)
  if value:
    resp.flags.incl(KeepAlive)
  else:
    resp.flags.excl(KeepAlive)

proc keepalive*(resp: HttpResponseRef): bool {.raises: [Defect].} =
  KeepAlive in resp.flags

proc setHeader*(resp: HttpResponseRef, key, value: string) {.
     raises: [Defect].} =
  doAssert(resp.state == HttpResponseState.Empty)
  resp.headersTable.set(key, value)

proc addHeader*(resp: HttpResponseRef, key, value: string) {.
     raises: [Defect].}=
  doAssert(resp.state == HttpResponseState.Empty)
  resp.headersTable.add(key, value)

proc getHeader*(resp: HttpResponseRef, key: string,
                default: string = ""): string {.raises: [Defect].} =
  resp.headersTable.getString(key, default)

proc hasHeader*(resp: HttpResponseRef, key: string): bool {.raises: [Defect].} =
  key in resp.headersTable

template doHeaderDef(buf, resp, name, default) =
  buf.add(name)
  buf.add(": ")
  buf.add(resp.getHeader(name, default))
  buf.add("\r\n")

template doHeaderVal(buf, name, value) =
  buf.add(name)
  buf.add(": ")
  buf.add(value)
  buf.add("\r\n")

template checkPending(t: untyped) =
  if t.state != HttpResponseState.Empty:
    raise newHttpCriticalError("Response body was already sent")

proc prepareLengthHeaders(resp: HttpResponseRef, length: int): string {.
     raises: [Defect].}=
  var answer = $(resp.version) & " " & $(resp.status) & "\r\n"
  answer.doHeaderDef(resp, "Date", httpDate())
  answer.doHeaderDef(resp, "Content-Type", "text/html; charset=utf-8")
  if length > 0:
    answer.doHeaderVal("Content-Length", $(length))
  if "Connection" notin resp.headersTable:
    if KeepAlive in resp.flags:
      answer.doHeaderVal("Connection", "keep-alive")
    else:
      answer.doHeaderVal("Connection", "close")
  for k, v in resp.headersTable.stringItems():
    if k notin ["date", "content-type", "content-length"]:
      answer.doHeaderVal(normalizeHeaderName(k), v)
  answer.add("\r\n")
  answer

proc prepareChunkedHeaders(resp: HttpResponseRef): string {.
     raises: [Defect].} =
  var answer = $(resp.version) & " " & $(resp.status) & "\r\n"
  answer.doHeaderDef(resp, "Date", httpDate())
  answer.doHeaderDef(resp, "Content-Type", "text/html; charset=utf-8")
  answer.doHeaderDef(resp, "Transfer-Encoding", "chunked")
  if "Connection" notin resp.headersTable:
    if KeepAlive in resp.flags:
      answer.doHeaderVal("Connection", "keep-alive")
    else:
      answer.doHeaderVal("Connection", "close")
  for k, v in resp.headersTable.stringItems():
    if k notin ["date", "content-type", "content-length", "transfer-encoding"]:
      answer.doHeaderVal(normalizeHeaderName(k), v)
  answer.add("\r\n")
  answer

proc sendBody*(resp: HttpResponseRef, pbytes: pointer, nbytes: int) {.async.} =
  ## Send HTTP response at once by using bytes pointer ``pbytes`` and length
  ## ``nbytes``.
  doAssert(not(isNil(pbytes)), "pbytes must not be nil")
  doAssert(nbytes >= 0, "nbytes should be bigger or equal to zero")
  checkPending(resp)
  let responseHeaders = resp.prepareLengthHeaders(nbytes)
  resp.state = HttpResponseState.Prepared
  try:
    resp.state = HttpResponseState.Sending
    await resp.connection.writer.write(responseHeaders)
    if nbytes > 0:
      await resp.connection.writer.write(pbytes, nbytes)
    resp.state = HttpResponseState.Finished
  except CancelledError as exc:
    resp.state = HttpResponseState.Cancelled
    raise exc
  except AsyncStreamWriteError, AsyncStreamIncompleteError:
    resp.state = HttpResponseState.Failed
    raise newHttpCriticalError("Unable to send response")

proc sendBody*[T: string|seq[byte]](resp: HttpResponseRef, data: T) {.async.} =
  ## Send HTTP response at once by using data ``data``.
  checkPending(resp)
  let responseHeaders = resp.prepareLengthHeaders(len(data))
  resp.state = HttpResponseState.Prepared
  try:
    resp.state = HttpResponseState.Sending
    await resp.connection.writer.write(responseHeaders)
    if len(data) > 0:
      await resp.connection.writer.write(data)
    resp.state = HttpResponseState.Finished
  except CancelledError as exc:
    resp.state = HttpResponseState.Cancelled
    raise exc
  except AsyncStreamWriteError, AsyncStreamIncompleteError:
    resp.state = HttpResponseState.Failed
    raise newHttpCriticalError("Unable to send response")

proc sendError*(resp: HttpResponseRef, code: HttpCode, body = "") {.async.} =
  ## Send HTTP error status response.
  checkPending(resp)
  resp.status = code
  let responseHeaders = resp.prepareLengthHeaders(len(body))
  resp.state = HttpResponseState.Prepared
  try:
    resp.state = HttpResponseState.Sending
    await resp.connection.writer.write(responseHeaders)
    if len(body) > 0:
      await resp.connection.writer.write(body)
    resp.state = HttpResponseState.Finished
  except CancelledError as exc:
    resp.state = HttpResponseState.Cancelled
    raise exc
  except AsyncStreamWriteError, AsyncStreamIncompleteError:
    resp.state = HttpResponseState.Failed
    raise newHttpCriticalError("Unable to send response")

proc prepare*(resp: HttpResponseRef) {.async.} =
  ## Prepare for HTTP stream response.
  ##
  ## Such responses will be sent chunk by chunk using ``chunked`` encoding.
  resp.checkPending()
  let responseHeaders = resp.prepareChunkedHeaders()
  resp.state = HttpResponseState.Prepared
  try:
    resp.state = HttpResponseState.Sending
    await resp.connection.writer.write(responseHeaders)
    resp.chunkedWriter = newChunkedStreamWriter(resp.connection.writer)
    resp.flags.incl(HttpResponseFlags.Chunked)
  except CancelledError as exc:
    resp.state = HttpResponseState.Cancelled
    raise exc
  except AsyncStreamWriteError, AsyncStreamIncompleteError:
    resp.state = HttpResponseState.Failed
    raise newHttpCriticalError("Unable to send response")

proc sendChunk*(resp: HttpResponseRef, pbytes: pointer, nbytes: int) {.async.} =
  ## Send single chunk of data pointed by ``pbytes`` and ``nbytes``.
  doAssert(not(isNil(pbytes)), "pbytes must not be nil")
  doAssert(nbytes >= 0, "nbytes should be bigger or equal to zero")
  if HttpResponseFlags.Chunked notin resp.flags:
    raise newHttpCriticalError("Response was not prepared")
  if resp.state notin {HttpResponseState.Prepared, HttpResponseState.Sending}:
    raise newHttpCriticalError("Response in incorrect state")
  try:
    resp.state = HttpResponseState.Sending
    await resp.chunkedWriter.write(pbytes, nbytes)
    resp.state = HttpResponseState.Sending
  except CancelledError as exc:
    resp.state = HttpResponseState.Cancelled
    raise exc
  except AsyncStreamWriteError, AsyncStreamIncompleteError:
    resp.state = HttpResponseState.Failed
    raise newHttpCriticalError("Unable to send response")

proc sendChunk*[T: string|seq[byte]](resp: HttpResponseRef,
                                     data: T) {.async.} =
  ## Send single chunk of data ``data``.
  if HttpResponseFlags.Chunked notin resp.flags:
    raise newHttpCriticalError("Response was not prepared")
  if resp.state notin {HttpResponseState.Prepared, HttpResponseState.Sending}:
    raise newHttpCriticalError("Response in incorrect state")
  try:
    resp.state = HttpResponseState.Sending
    await resp.chunkedWriter.write(data)
    resp.state = HttpResponseState.Sending
  except CancelledError as exc:
    resp.state = HttpResponseState.Cancelled
    raise exc
  except AsyncStreamWriteError, AsyncStreamIncompleteError:
    resp.state = HttpResponseState.Failed
    raise newHttpCriticalError("Unable to send response")

proc finish*(resp: HttpResponseRef) {.async.} =
  ## Sending last chunk of data, so it will indicate end of HTTP response.
  if HttpResponseFlags.Chunked notin resp.flags:
    raise newHttpCriticalError("Response was not prepared")
  if resp.state notin {HttpResponseState.Prepared, HttpResponseState.Sending}:
    raise newHttpCriticalError("Response in incorrect state")
  try:
    resp.state = HttpResponseState.Sending
    await resp.chunkedWriter.finish()
    resp.state = HttpResponseState.Finished
  except CancelledError as exc:
    resp.state = HttpResponseState.Cancelled
    raise exc
  except AsyncStreamWriteError, AsyncStreamIncompleteError:
    resp.state = HttpResponseState.Failed
    raise newHttpCriticalError("Unable to send response")

proc respond*(req: HttpRequestRef, code: HttpCode, content: string,
              headers: HttpTable): Future[HttpResponseRef] {.async.} =
  ## Responds to the request with the specified ``HttpCode``, headers and
  ## content.
  let response = req.getResponse()
  response.status = code
  for k, v in headers.stringItems():
    response.addHeader(k, v)
  await response.sendBody(content)
  return response

proc requestInfo*(req: HttpRequestRef, contentType = "text/text"): string {.
     raises: [Defect].} =
  ## Returns comprehensive information about request for specific content
  ## type.
  ##
  ## Only two content-types are supported: "text/text" and "text/html".
  proc h(t: string): string =
    case contentType
    of "text/text":
      "\r\n" & t & " ===\r\n"
    of "text/html":
      "<h3>" & t & "</h3>"
    else:
      t
  proc kv(k, v: string): string =
    case contentType
    of "text/text":
      k & ": " & v & "\r\n"
    of "text/html":
      "<div><code><b>" & k & "</b></code><code>" & v & "</code></div>"
    else:
      k & ": " & v

  let header =
    case contentType
    of "text/html":
      "<html><head><title>Request Information</title>" &
      "<style>code {padding-left: 30px;}</style>" &
      "</head><body>"
    else:
      ""

  let footer =
    case contentType
    of "text/html":
      "</body></html>"
    else:
      ""

  var res = h("Request information")
  res.add(kv("request.scheme", $req.scheme))
  res.add(kv("request.method", $req.meth))
  res.add(kv("request.version", $req.version))
  res.add(kv("request.uri", $req.uri))
  res.add(kv("request.flags", $req.requestFlags))
  res.add(kv("request.TransferEncoding", $req.transferEncoding))
  res.add(kv("request.ContentEncoding", $req.contentEncoding))

  let body =
    if req.hasBody():
      if req.contentLength == 0:
        "present, size not available"
      else:
        "present, size = " & $req.contentLength
    else:
      "not available"
  res.add(kv("request.body", body))

  if not(req.query.isEmpty()):
    res.add(h("Query arguments"))
    for k, v in req.query.stringItems():
      res.add(kv(k, v))

  if not(req.headers.isEmpty()):
    res.add(h("HTTP headers"))
    for k, v in req.headers.stringItems(true):
      res.add(kv(k, v))

  if req.meth in PostMethods:
    if req.postTable.isSome():
      let postTable = req.postTable.get()
      if not(postTable.isEmpty()):
        res.add(h("POST arguments"))
        for k, v in postTable.stringItems():
          res.add(kv(k, v))

  res.add(h("Connection information"))
  let localAddress =
    try:
      $req.connection.transp.localAddress()
    except TransportError:
      "incorrect address"
  let remoteAddress =
    try:
      $req.connection.transp.remoteAddress()
    except TransportError:
      "incorrect address"

  res.add(kv("local.address", localAddress))
  res.add(kv("remote.address", remoteAddress))

  res.add(h("Server configuration"))
  let maxConn =
    if req.connection.server.maxConnections < 0:
      "unlimited"
    else:
      $req.connection.server.maxConnections
  res.add(kv("server.maxConnections", $maxConn))
  res.add(kv("server.maxHeadersSize", $req.connection.server.maxHeadersSize))
  res.add(kv("server.maxRequestBodySize",
             $req.connection.server.maxRequestBodySize))
  res.add(kv("server.backlog", $req.connection.server.backLogSize))
  res.add(kv("server.headersTimeout", $req.connection.server.headersTimeout))
  res.add(kv("server.baseUri", $req.connection.server.baseUri))
  res.add(kv("server.flags", $req.connection.server.flags))
  res.add(kv("server.socket.flags", $req.connection.server.socketFlags))
  header & res & footer
