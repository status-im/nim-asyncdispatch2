#
#                Chronos HTTP/S client
#             (c) Copyright 2021-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import std/[uri, tables, strutils, sequtils]
import stew/[results, base10, base64], httputils
import ../../asyncloop, ../../asyncsync
import ../../streams/[asyncstream, tlsstream, chunkstream, boundstream]
import httptable, httpcommon, httpagent, httpbodyrw, multipart
export httptable, httpcommon, httpagent, httpbodyrw, multipart

const
  HttpMaxHeadersSize* = 8192
    ## Maximum size of HTTP headers in octets
  HttpConnectTimeout* = 12.seconds
    ## Timeout for connecting to host (12 sec)
  HttpHeadersTimeout* = 120.seconds
    ## Timeout for receiving response headers (120 sec)
  HttpMaxRedirections* = 10
    ## Maximum number of Location redirections.
  HttpClientConnectionTrackerName* = "httpclient.connection"
    ## HttpClient connection leaks tracker name
  HttpClientRequestTrackerName* = "httpclient.request"
    ## HttpClient request leaks tracker name
  HttpClientResponseTrackerName* = "httpclient.response"
    ## HttpClient response leaks tracker name

type
  HttpClientConnectionState* {.pure.} = enum
    Closed                    ## Connection has been closed
    Closing,                  ## Connection is closing
    Resolving,                ## Resolving remote hostname
    Connecting,               ## Connecting to remote server
    Ready,                    ## Connected to remote server
    RequestHeadersSending,    ## Sending request headers
    RequestHeadersSent,       ## Request headers has been sent
    RequestBodySending,       ## Sending request body
    RequestBodySent,          ## Request body has been sent
    ResponseHeadersReceiving, ## Receiving response headers
    ResponseHeadersReceived,  ## Response headers has been received
    ResponseBodyReceiving,    ## Receiving response body
    ResponseBodyReceived,     ## Response body has been received
    Error                     ## Error happens

  HttpClientScheme* {.pure.} = enum
    NonSecure,                ## Non-secure connection
    Secure                    ## Secure TLS connection

  HttpClientRequestState* {.pure.} = enum
    Closed,                   ## Request has been closed
    Closing,                  ## Connection is closing
    Created,                  ## Request created
    Connecting,               ## Connecting to remote host
    HeadersSending,           ## Sending request headers
    HeadersSent,              ## Request headers has been sent
    BodySending,              ## Sending request body
    BodySent,                 ## Request body has been sent
    ResponseReceived,         ## Request's response headers received
    Error                     ## Error happens

  HttpClientResponseState* {.pure.} = enum
    Closed,                   ## Response has been closed
    Closing,                  ## Response is closing
    HeadersReceived,          ## Response headers received
    BodyReceiving,            ## Response body receiving
    BodyReceived,             ## Response body received
    Error                     ## Error happens

  HttpClientBodyFlag* {.pure.} = enum
    Sized,                    ## `Content-Length` present
    Chunked,                  ## `Transfer-Encoding: chunked` present
    Custom                    ## None of the above

  HttpClientRequestFlag* {.pure.} = enum
    CloseConnection,          ## Send `Connection: close` in request

  HttpHeaderTuple* = tuple
    key: string
    value: string

  HttpResponseTuple* = tuple
    status: int
    data: seq[byte]

  HttpClientConnection* = object of RootObj
    case kind*: HttpClientScheme
    of HttpClientScheme.NonSecure:
      discard
    of HttpClientScheme.Secure:
      treader*: AsyncStreamReader
      twriter*: AsyncStreamWriter
      tls*: TLSAsyncStream
    transp*: StreamTransport
    reader*: AsyncStreamReader
    writer*: AsyncStreamWriter
    state*: HttpClientConnectionState
    error*: ref HttpError
    remoteHostname*: string

  HttpClientConnectionRef* = ref HttpClientConnection

  HttpSessionRef* = ref object
    connections*: Table[string, seq[HttpClientConnectionRef]]
    maxRedirections*: int
    connectTimeout*: Duration
    headersTimeout*: Duration
    connectionBufferSize*: int
    maxConnections*: int
    flags*: HttpClientFlags

  HttpAddress* = object
    id*: string
    scheme*: HttpClientScheme
    hostname*: string
    port*: uint16
    path*: string
    query*: string
    anchor*: string
    username*: string
    password*: string
    addresses*: seq[TransportAddress]

  HttpClientRequest* = object
    meth*: HttpMethod
    address*: HttpAddress
    state: HttpClientRequestState
    version*: HttpVersion
    headers*: HttpTable
    bodyFlag: HttpClientBodyFlag
    flags: set[HttpClientRequestFlag]
    connection*: HttpClientConnectionRef
    session*: HttpSessionRef
    error*: ref HttpError
    buffer*: seq[byte]
    writer*: HttpBodyWriter
    redirectCount: int

  HttpClientRequestRef* = ref HttpClientRequest

  HttpClientResponse* = object
    state: HttpClientResponseState
    requestMethod*: HttpMethod
    address*: HttpAddress
    status*: int
    reason*: string
    version*: HttpVersion
    headers*: HttpTable
    connection*: HttpClientConnectionRef
    session*: HttpSessionRef
    reader*: HttpBodyReader
    error*: ref HttpError
    bodyFlag*: HttpClientBodyFlag
    contentEncoding*: set[ContentEncodingFlags]
    transferEncoding*: set[TransferEncodingFlags]
    contentLength*: uint64

  HttpClientResponseRef* = ref HttpClientResponse

  HttpClientFlag* {.pure.} = enum
    NoVerifyHost,        ## Skip remote server certificate verification
    NoVerifyServerName,  ## Skip remote server name CN verification
    NoInet4Resolution,   ## Do not resolve server hostname to IPv4 addresses
    NoInet6Resolution,   ## Do not resolve server hostname to IPv6 addresses
    NoAutomaticRedirect  ## Do not handle HTTP redirection automatically

  HttpClientFlags* = set[HttpClientFlag]

  HttpClientTracker* = ref object of TrackerBase
    opened*: int64
    closed*: int64

proc setupHttpClientConnectionTracker(): HttpClientTracker {.
     gcsafe, raises: [Defect].}
proc setupHttpClientRequestTracker(): HttpClientTracker {.
     gcsafe, raises: [Defect].}
proc setupHttpClientResponseTracker(): HttpClientTracker {.
     gcsafe, raises: [Defect].}

proc getHttpClientConnectionTracker(): HttpClientTracker {.inline.} =
  var res = cast[HttpClientTracker](getTracker(HttpClientConnectionTrackerName))
  if isNil(res):
    res = setupHttpClientConnectionTracker()
  res

proc getHttpClientRequestTracker(): HttpClientTracker {.inline.} =
  var res = cast[HttpClientTracker](getTracker(HttpClientRequestTrackerName))
  if isNil(res):
    res = setupHttpClientRequestTracker()
  res

proc getHttpClientResponseTracker(): HttpClientTracker {.inline.} =
  var res = cast[HttpClientTracker](getTracker(HttpClientResponseTrackerName))
  if isNil(res):
    res = setupHttpClientResponseTracker()
  res

proc dumpHttpClientConnectionTracking(): string {.gcsafe.} =
  let tracker = getHttpClientConnectionTracker()
  "Opened HTTP client connections: " & $tracker.opened & "\n" &
  "Closed HTTP client connections: " & $tracker.closed

proc dumpHttpClientRequestTracking(): string {.gcsafe.} =
  let tracker = getHttpClientRequestTracker()
  "Opened HTTP client requests: " & $tracker.opened & "\n" &
  "Closed HTTP client requests: " & $tracker.closed

proc dumpHttpClientResponseTracking(): string {.gcsafe.} =
  let tracker = getHttpClientResponseTracker()
  "Opened HTTP client responses: " & $tracker.opened & "\n" &
  "Closed HTTP client responses: " & $tracker.closed

proc leakHttpClientConnection(): bool {.gcsafe.} =
  var tracker = getHttpClientConnectionTracker()
  tracker.opened != tracker.closed

proc leakHttpClientRequest(): bool {.gcsafe.} =
  var tracker = getHttpClientRequestTracker()
  tracker.opened != tracker.closed

proc leakHttpClientResponse(): bool {.gcsafe.} =
  var tracker = getHttpClientResponseTracker()
  tracker.opened != tracker.closed

proc trackHttpClientConnection(t: HttpClientConnectionRef) {.inline.} =
  inc(getHttpClientConnectionTracker().opened)

proc untrackHttpClientConnection*(t: HttpClientConnectionRef) {.inline.}  =
  inc(getHttpClientConnectionTracker().closed)

proc trackHttpClientRequest(t: HttpClientRequestRef) {.inline.} =
  inc(getHttpClientRequestTracker().opened)

proc untrackHttpClientRequest*(t: HttpClientRequestRef) {.inline.}  =
  inc(getHttpClientRequestTracker().closed)

proc trackHttpClientResponse(t: HttpClientResponseRef) {.inline.} =
  inc(getHttpClientResponseTracker().opened)

proc untrackHttpClientResponse*(t: HttpClientResponseRef) {.inline.}  =
  inc(getHttpClientResponseTracker().closed)

proc setupHttpClientConnectionTracker(): HttpClientTracker {.gcsafe.} =
  var res = HttpClientTracker(opened: 0, closed: 0,
    dump: dumpHttpClientConnectionTracking,
    isLeaked: leakHttpClientConnection
  )
  addTracker(HttpClientConnectionTrackerName, res)
  res

proc setupHttpClientRequestTracker(): HttpClientTracker {.gcsafe.} =
  var res = HttpClientTracker(opened: 0, closed: 0,
    dump: dumpHttpClientRequestTracking,
    isLeaked: leakHttpClientRequest
  )
  addTracker(HttpClientRequestTrackerName, res)
  res

proc setupHttpClientResponseTracker(): HttpClientTracker {.gcsafe.} =
  var res = HttpClientTracker(opened: 0, closed: 0,
    dump: dumpHttpClientResponseTracking,
    isLeaked: leakHttpClientResponse
  )
  addTracker(HttpClientResponseTrackerName, res)
  res

proc new*(t: typedesc[HttpSessionRef],
          flags: HttpClientFlags = {},
          maxRedirections = HttpMaxRedirections,
          connectTimeout = HttpConnectTimeout,
          headersTimeout = HttpHeadersTimeout,
          connectionBufferSize = DefaultStreamBufferSize,
          maxConnections = -1): HttpSessionRef {.
     raises: [Defect] .} =
  ## Create new HTTP session object.
  ##
  ## ``maxRedirections`` - maximum number of HTTP 3xx redirections
  ## ``connectTimeout`` - timeout for ongoing HTTP connection
  ## ``headersTimeout`` - timeout for receiving HTTP response headers
  doAssert(maxRedirections >= 0, "maxRedirections should not be negative")
  HttpSessionRef(
    flags: flags,
    maxRedirections: maxRedirections,
    connectTimeout: connectTimeout,
    headersTimeout: headersTimeout,
    connectionBufferSize: connectionBufferSize,
    maxConnections: maxConnections,
    connections: initTable[string, seq[HttpClientConnectionRef]]()
  )

proc getTLSFlags(flags: HttpClientFlags): set[TLSFlags] {.raises: [Defect] .} =
  var res: set[TLSFlags]
  if HttpClientFlag.NoVerifyHost in flags:
    res.incl(TLSFlags.NoVerifyHost)
  if HttpClientFlag.NoVerifyServerName in flags:
    res.incl(TLSFlags.NoVerifyServerName)
  res

proc getAddress*(session: HttpSessionRef, url: Uri): HttpResult[HttpAddress] {.
     raises: [Defect] .} =
  let scheme =
    if len(url.scheme) == 0:
      HttpClientScheme.NonSecure
    else:
      case toLowerAscii(url.scheme)
      of "http":
        HttpClientScheme.NonSecure
      of "https":
        HttpClientScheme.Secure
      else:
        return err("URL scheme not supported")

  let port =
    if len(url.port) == 0:
      case scheme
      of HttpClientScheme.NonSecure:
        80'u16
      of HttpClientScheme.Secure:
        443'u16
    else:
      let res = Base10.decode(uint16, url.port)
      if res.isErr():
        return err("Invalid URL port number")
      res.get()

  let hostname =
    block:
      if len(url.hostname) == 0:
        return err("URL hostname is missing")
      url.hostname

  let id = hostname & ":" & Base10.toString(port)

  let addresses =
    try:
      if (HttpClientFlag.NoInet4Resolution in session.flags) and
         (HttpClientFlag.NoInet6Resolution in session.flags):
        # DNS resolution is disabled.
        @[initTAddress(hostname, Port(port))]
      else:
        if (HttpClientFlag.NoInet4Resolution notin session.flags) and
           (HttpClientFlag.NoInet6Resolution notin session.flags):
          # DNS resolution for both IPv4 and IPv6 addresses.
          resolveTAddress(hostname, Port(port))
        else:
          if HttpClientFlag.NoInet6Resolution in session.flags:
            # DNS resolution only for IPv4 addresses.
            resolveTAddress(hostname, Port(port), AddressFamily.IPv4)
          else:
            # DNS resolution only for IPv6 addresses
            resolveTAddress(hostname, Port(port), AddressFamily.IPv6)
    except TransportAddressError:
      return err("Could not resolve address of remote server")

  if len(addresses) == 0:
    return err("Could not resolve address of remote server")

  ok(HttpAddress(id: id, scheme: scheme, hostname: hostname, port: port,
                 path: url.path, query: url.query, anchor: url.anchor,
                 username: url.username, password: url.password,
                 addresses: addresses))

proc getAddress*(session: HttpSessionRef,
                 url: string): HttpResult[HttpAddress] {.raises: [Defect].} =
  ## Create new HTTP address using URL string ``url`` and .
  session.getAddress(parseUri(url))

proc getAddress*(address: TransportAddress,
                 ctype: HttpClientScheme = HttpClientScheme.NonSecure,
                 queryString: string = "/"): HttpAddress {.raises: [Defect].} =
  ## Create new HTTP address using Transport address ``address``, connection
  ## type ``ctype`` and query string ``queryString``.
  let uri = parseUri(queryString)
  HttpAddress(id: $address, scheme: ctype, hostname: address.host,
    port: uint16(address.port), path: uri.path, query: uri.query,
    anchor: uri.anchor, username: "", password: "", addresses: @[address]
  )

proc getUri*(address: HttpAddress): Uri =
  ## Retrieve URI from ``address``.
  let scheme =
    case address.scheme
    of HttpClientScheme.NonSecure:
      "http"
    of HttpClientScheme.Secure:
      "https"
  Uri(
    scheme: scheme, username: address.username, password: address.password,
    hostname: address.hostname, port: Base10.toString(address.port),
    path: address.path, query: address.query, anchor: address.anchor,
    opaque: false
  )

proc redirect*(srcuri, dsturi: Uri): Uri =
  ## Transform original's URL ``srcuri`` to ``dsturi``.
  if (len(dsturi.scheme) > 0) and (len(dsturi.hostname) > 0):
    # `dsturi` is absolute URL, replace
    dsturi
  else:
    # `dsturi` is relative URL, combine
    var tmpuri = dsturi
    tmpuri.username = ""
    tmpuri.password = ""
    combine(srcuri, tmpuri)

proc redirect*(session: HttpSessionRef,
               srcaddr: HttpAddress, uri: Uri): HttpResult[HttpAddress] =
  ## Transform original address ``srcaddr`` using redirected url ``uri`` and
  ## session ``session`` parameters.
  let srcuri = srcaddr.getUri()
  var newuri = srcuri.redirect(uri)
  if newuri.hostname != srcuri.hostname:
    session.getAddress(newuri)
  else:
    let scheme =
      case newuri.scheme
      of "http":
        HttpClientScheme.NonSecure
      of "https":
        HttpClientScheme.Secure
      else:
        return err("URL scheme not supported")

    let port =
      if len(newuri.port) == 0:
        case scheme:
        of HttpClientScheme.NonSecure:
          80'u16
        of HttpClientScheme.Secure:
          443'u16
      else:
        let res = Base10.decode(uint16, newuri.port)
        if res.isErr():
          return err("Invalid URL port number")
        res.get()

    if len(newuri.hostname) == 0:
      return err("URL hostname is missing")

    let id = newuri.hostname & ":" & Base10.toString(port)

    ok(HttpAddress(
      id: id, scheme: scheme, hostname: newuri.hostname, port: port,
      path: newuri.path, query: newuri.query, anchor: newuri.anchor,
      username: newuri.username, password: newuri.password,
      addresses: srcaddr.addresses
    ))

proc new(t: typedesc[HttpClientConnectionRef], session: HttpSessionRef,
         ha: HttpAddress, transp: StreamTransport): HttpClientConnectionRef =
  case ha.scheme
  of HttpClientScheme.NonSecure:
    let res = HttpClientConnectionRef(
      kind: HttpClientScheme.NonSecure,
      transp: transp,
      reader: newAsyncStreamReader(transp),
      writer: newAsyncStreamWriter(transp),
      state: HttpClientConnectionState.Connecting,
      remoteHostname: ha.id
    )
    trackHttpClientConnection(res)
    res
  of HttpClientScheme.Secure:
    let treader = newAsyncStreamReader(transp)
    let twriter = newAsyncStreamWriter(transp)
    let tls = newTLSClientAsyncStream(treader, twriter, ha.hostname,
                                      flags = session.flags.getTLSFlags())
    let res = HttpClientConnectionRef(
      kind: HttpClientScheme.Secure,
      transp: transp,
      treader: treader,
      twriter: twriter,
      reader: tls.reader,
      writer: tls.writer,
      tls: tls,
      state: HttpClientConnectionState.Connecting,
      remoteHostname: ha.id
    )
    trackHttpClientConnection(res)
    res

proc setState(request: HttpClientRequestRef, state: HttpClientRequestState) {.
     raises: [Defect] .} =
  request.state = state
  case state
  of HttpClientRequestState.HeadersSending:
    request.connection.state = HttpClientConnectionState.RequestHeadersSending
  of HttpClientRequestState.HeadersSent:
    request.connection.state = HttpClientConnectionState.RequestHeadersSent
  of HttpClientRequestState.BodySending:
    request.connection.state = HttpClientConnectionState.RequestBodySending
  of HttpClientRequestState.BodySent:
    request.connection.state = HttpClientConnectionState.RequestBodySent
  of HttpClientRequestState.ResponseReceived:
    request.connection.state = HttpClientConnectionState.ResponseHeadersReceived
  else:
    discard

proc setState(response: HttpClientResponseRef,
              state: HttpClientResponseState) {.raises: [Defect] .} =
  response.state = state
  case state
  of HttpClientResponseState.HeadersReceived:
    response.connection.state =
      HttpClientConnectionState.ResponseHeadersReceived
  of HttpClientResponseState.BodyReceiving:
    response.connection.state = HttpClientConnectionState.ResponseBodyReceiving
  of HttpClientResponseState.BodyReceived:
    response.connection.state = HttpClientConnectionState.ResponseBodyReceived
  else:
    discard

proc setError(request: HttpClientRequestRef, error: ref HttpError) {.
     raises: [Defect] .} =
  request.error = error
  request.setState(HttpClientRequestState.Error)
  if not(isNil(request.connection)):
    request.connection.state = HttpClientConnectionState.Error
    request.connection.error = error

proc setError(response: HttpClientResponseRef, error: ref HttpError) {.
     raises: [Defect] .} =
  response.error = error
  response.setState(HttpClientResponseState.Error)
  if not(isNil(response.connection)):
    response.connection.state = HttpClientConnectionState.Error
    response.connection.error = error

proc closeWait(conn: HttpClientConnectionRef) {.async.} =
  ## Close HttpClientConnectionRef instance ``conn`` and free all the resources.
  if conn.state notin {HttpClientConnectionState.Closing,
                       HttpClientConnectionState.Closed}:
    conn.state = HttpClientConnectionState.Closing
    let pending =
      block:
        var res: seq[Future[void]]
        if not(isNil(conn.reader)) and not(conn.reader.closed()):
          res.add(conn.reader.closeWait())
        if not(isNil(conn.writer)) and not(conn.writer.closed()):
          res.add(conn.writer.closeWait())
        res
    if len(pending) > 0:
      awaitrc allFutures(pending)
    case conn.kind
    of HttpClientScheme.Secure:
      awaitrc allFutures(conn.treader.closeWait(), conn.twriter.closeWait())
    of HttpClientScheme.NonSecure:
      discard
    awaitrc conn.transp.closeWait()
    conn.state = HttpClientConnectionState.Closed
    untrackHttpClientConnection(conn)

proc connect(session: HttpSessionRef,
             ha: HttpAddress): Future[HttpClientConnectionRef] {.async.} =
  ## Establish new connection with remote server using ``url`` and ``flags``.
  ## On success returns ``HttpClientConnectionRef`` object.

  # Here we trying to connect to every possible remote host address we got after
  # DNS resolution.
  for address in ha.addresses:
    let transp =
      try:
        await connect(address, bufferSize = session.connectionBufferSize)
      except CancelledError as exc:
        raise exc
      except CatchableError:
        nil
    if not(isNil(transp)):
      let conn =
        block:
          let res = HttpClientConnectionRef.new(session, ha, transp)
          case res.kind
          of HttpClientScheme.Secure:
            try:
              await res.tls.handshake()
              res.state = HttpClientConnectionState.Ready
            except CancelledError as exc:
              awaitrc res.closeWait()
              raise exc
            except AsyncStreamError:
              awaitrc res.closeWait()
              res.state = HttpClientConnectionState.Error
          of HttpClientScheme.Nonsecure:
            res.state = HttpClientConnectionState.Ready
          res
      if conn.state == HttpClientConnectionState.Ready:
        return conn

  # If all attempts to connect to the remote host have failed.
  raiseHttpConnectionError("Could not connect to remote host")

proc acquireConnection(session: HttpSessionRef,
                       ha: HttpAddress): Future[HttpClientConnectionRef] {.
     async.} =
  ## Obtain connection from ``session`` or establish a new one.
  let conn =
    block:
      let conns = session.connections.getOrDefault(ha.id)
      if len(conns) > 0:
        var res: HttpClientConnectionRef = nil
        for item in conns:
          if item.state == HttpClientConnectionState.Ready:
            res = item
            break
        res
      else:
        nil
  if not(isNil(conn)):
    return conn
  else:
    var default: seq[HttpClientConnectionRef]
    let res =
      try:
        await session.connect(ha).wait(session.connectTimeout)
      except AsyncTimeoutError:
        raiseHttpConnectionError("Connection timed out")
    session.connections.mgetOrPut(ha.id, default).add(res)
    return res

proc removeConnection(session: HttpSessionRef,
                      conn: HttpClientConnectionRef) {.async.} =
  var conns = session.connections.getOrDefault(conn.remoteHostname)
  conns.keepItIf(it != conn)
  session.connections[conn.remoteHostname] = conns
  await conn.closeWait()

proc releaseConnection(session: HttpSessionRef,
                       conn: HttpClientConnectionRef) {.async.} =
  ## Return connection back to the ``session``.
  ##
  ## If connection not in ``Ready`` state it will be closed and removed from
  ## the ``session``.
  if conn.state != HttpClientConnectionState.Ready:
    await session.removeConnection(conn)

proc closeWait*(session: HttpSessionRef) {.async.} =
  ## Closes HTTP session object.
  ##
  ## This closes all the connections opened to remote servers.
  var pending: seq[Future[void]]
  for items in session.connections.values():
    for item in items:
      pending.add(closeWait(item))
  await allFutures(pending)

proc closeWait*(request: HttpClientRequestRef) {.async.} =
  if request.state notin {HttpClientRequestState.Closing,
                          HttpClientRequestState.Closed}:
    request.setState(HttpClientRequestState.Closing)
    if not(isNil(request.writer)):
      if not(request.writer.closed()):
        awaitrc request.writer.closeWait()
        request.writer = nil
    if request.state != HttpClientRequestState.ResponseReceived:
      if not(isNil(request.connection)):
        awaitrc request.session.releaseConnection(request.connection)
        request.connection = nil
    request.session = nil
    request.error = nil
    request.setState(HttpClientRequestState.Closed)
    untrackHttpClientRequest(request)

proc closeWait*(response: HttpClientResponseRef) {.async.} =
  if response.state notin {HttpClientResponseState.Closing,
                           HttpClientResponseState.Closed}:
    response.setState(HttpClientResponseState.Closing)
    if not(isNil(response.reader)):
      if not(response.reader.closed()):
        awaitrc response.reader.closeWait()
        response.reader = nil
    if not(isNil(response.connection)):
      awaitrc response.session.releaseConnection(response.connection)
      response.connection = nil
    response.session = nil
    response.error = nil
    response.setState(HttpClientResponseState.Closed)
    untrackHttpClientResponse(response)

proc prepareResponse(request: HttpClientRequestRef,
                    data: openarray[byte]): HttpResult[HttpClientResponseRef] {.
     raises: [Defect] .} =
  ## Process response headers.
  let resp = parseResponse(data, false)
  if resp.failed():
    return err("Invalid headers received")

  let headers =
    block:
      var res = HttpTable.init()
      for key, value in resp.headers(data):
        res.add(key, value)
      if res.count(ContentTypeHeader) > 1:
        return err("Invalid headers received, too many `Content-Type`")
      if res.count(ContentLengthHeader) > 1:
        return err("Invalid headers received, too many `Content-Length`")
      if res.count(TransferEncodingHeader) > 1:
        return err("Invalid headers received, too many `Transfer-Encoding`")
      res

  # Preprocessing "Content-Encoding" header.
  let contentEncoding =
    block:
      let res = getContentEncoding(headers.getList(ContentEncodingHeader))
      if res.isErr():
        return err("Invalid headers received, invalid `Content-Encoding`")
      else:
        res.get()

  # Preprocessing "Transfer-Encoding" header.
  let transferEncoding =
    block:
      let res = getTransferEncoding(headers.getList(TransferEncodingHeader))
      if res.isErr():
        return err("Invalid headers received, invalid `Transfer-Encoding`")
      else:
        res.get()

  # Preprocessing "Content-Length" header.
  let (contentLength, bodyFlag) =
    if ContentLengthHeader in headers:
      let length = headers.getInt(ContentLengthHeader)
      (length, HttpClientBodyFlag.Sized)
    else:
      if TransferEncodingFlags.Chunked in transferEncoding:
        (0'u64, HttpClientBodyFlag.Chunked)
      else:
        (0'u64, HttpClientBodyFlag.Custom)

  let res = HttpClientResponseRef(
    state: HttpClientResponseState.HeadersReceived, status: resp.code,
    address: request.address, requestMethod: request.meth,
    reason: resp.reason(data), version: resp.version, session: request.session,
    connection: request.connection, headers: headers,
    contentEncoding: contentEncoding, transferEncoding: transferEncoding,
    contentLength: contentLength, bodyFlag: bodyFlag
  )
  request.setState(HttpClientRequestState.ResponseReceived)
  trackHttpClientResponse(res)
  ok(res)

proc getResponse(req: HttpClientRequestRef): Future[HttpClientResponseRef] {.
     async.} =
  var buffer: array[HttpMaxHeadersSize, byte]
  let bytesRead =
    try:
      await req.connection.reader.readUntil(addr buffer[0],
                                            len(buffer), HeadersMark).wait(
                                            req.session.headersTimeout)
    except CancelledError as exc:
      raise exc
    except AsyncTimeoutError:
      raiseHttpReadError("Reading response headers timed out")
    except AsyncStreamError:
      raiseHttpReadError("Could not read response headers")
  let resp = prepareResponse(req, buffer.toOpenArray(0, bytesRead - 1))
  if resp.isErr():
    raiseHttpProtocolError(resp.error())
  return resp.get()

proc new*(t: typedesc[HttpClientRequestRef], session: HttpSessionRef,
          ha: HttpAddress, meth: HttpMethod = MethodGet,
          version: HttpVersion = HttpVersion11,
          flags: set[HttpClientRequestFlag] = {},
          headers: openarray[HttpHeaderTuple] = [],
          body: openarray[byte] = []): HttpClientRequestRef {.
     raises: [Defect].} =
  let res = HttpClientRequestRef(
    state: HttpClientRequestState.Created, session: session, meth: meth,
    version: version, flags: flags, headers: HttpTable.init(headers),
    address: ha, bodyFlag: HttpClientBodyFlag.Custom, buffer: @body
  )
  trackHttpClientRequest(res)
  res

proc new*(t: typedesc[HttpClientRequestRef], session: HttpSessionRef,
          url: string, meth: HttpMethod = MethodGet,
          version: HttpVersion = HttpVersion11,
          flags: set[HttpClientRequestFlag] = {},
          headers: openarray[HttpHeaderTuple] = [],
          body: openarray[byte] = []): HttpResult[HttpClientRequestRef] {.
     raises: [Defect].} =
  let address = ? session.getAddress(parseUri(url))
  let res = HttpClientRequestRef(
    state: HttpClientRequestState.Created, session: session, meth: meth,
    version: version, flags: flags, headers: HttpTable.init(headers),
    address: address, bodyFlag: HttpClientBodyFlag.Custom, buffer: @body
  )
  trackHttpClientRequest(res)
  ok(res)

proc get*(t: typedesc[HttpClientRequestRef], session: HttpSessionRef,
          url: string, version: HttpVersion = HttpVersion11,
          flags: set[HttpClientRequestFlag] = {},
          headers: openarray[HttpHeaderTuple] = []
         ): HttpResult[HttpClientRequestRef] {.raises: [Defect].} =
  HttpClientRequestRef.new(session, url, MethodGet, version, flags, headers)

proc get*(t: typedesc[HttpClientRequestRef], session: HttpSessionRef,
          ha: HttpAddress, version: HttpVersion = HttpVersion11,
          flags: set[HttpClientRequestFlag] = {},
          headers: openarray[HttpHeaderTuple] = []
         ): HttpClientRequestRef {.raises: [Defect].} =
  HttpClientRequestRef.new(session, ha, MethodGet, version, flags, headers)

proc post*(t: typedesc[HttpClientRequestRef], session: HttpSessionRef,
           url: string, version: HttpVersion = HttpVersion11,
           flags: set[HttpClientRequestFlag] = {},
           headers: openarray[HttpHeaderTuple] = [],
           body: openarray[byte] = []
          ): HttpResult[HttpClientRequestRef] {.raises: [Defect].} =
  HttpClientRequestRef.new(session, url, MethodPost, version, flags, headers,
                           body)

proc post*(t: typedesc[HttpClientRequestRef], session: HttpSessionRef,
           url: string, version: HttpVersion = HttpVersion11,
           flags: set[HttpClientRequestFlag] = {},
           headers: openarray[HttpHeaderTuple] = [],
           body: openarray[char] = []): HttpResult[HttpClientRequestRef] {.
     raises: [Defect].} =
  HttpClientRequestRef.new(session, url, MethodPost, version, flags, headers,
                           body.toOpenArrayByte(0, len(body) - 1))

proc post*(t: typedesc[HttpClientRequestRef], session: HttpSessionRef,
           ha: HttpAddress, version: HttpVersion = HttpVersion11,
           flags: set[HttpClientRequestFlag] = {},
           headers: openarray[HttpHeaderTuple] = [],
           body: openarray[byte] = []): HttpClientRequestRef {.
     raises: [Defect].} =
  HttpClientRequestRef.new(session, ha, MethodPost, version, flags, headers,
                           body)

proc post*(t: typedesc[HttpClientRequestRef], session: HttpSessionRef,
           ha: HttpAddress, version: HttpVersion = HttpVersion11,
           flags: set[HttpClientRequestFlag] = {},
           headers: openarray[HttpHeaderTuple] = [],
           body: openarray[char] = []): HttpClientRequestRef {.
     raises: [Defect].} =
  HttpClientRequestRef.new(session, ha, MethodPost, version, flags, headers,
                           body.toOpenArrayByte(0, len(body) - 1))

proc prepareRequest(request: HttpClientRequestRef): string {.
     raises: [Defect].} =
  template hasChunkedEncoding(request: HttpClientRequestRef): bool =
    toLowerAscii(request.headers.getString(TransferEncodingHeader)) == "chunked"

  # We use ChronosIdent as `User-Agent` string if its not set.
  discard request.headers.hasKeyOrPut(UserAgentHeader, ChronosIdent)
  # We use request's hostname as `Host` string if its not set.
  discard request.headers.hasKeyOrPut(HostHeader, request.address.hostname)
  # We set `Connection` to value according to flags if its not set.
  if ConnectionHeader notin request.headers:
    if HttpClientRequestFlag.CloseConnection in request.flags:
      request.headers.add(ConnectionHeader, "close")
    else:
      request.headers.add(ConnectionHeader, "keep-alive")
  # We set `Accept` to accept any content if its not set.
  discard request.headers.hasKeyOrPut(AcceptHeader, "*/*")

  # We will send `Authorization` information only if username or password set,
  # and `Authorization` header is not present in request's headers.
  if len(request.address.username) > 0 or len(request.address.password) > 0:
    if AuthorizationHeader notin request.headers:
      let auth = request.address.username & ":" & request.address.password
      let header = "Basic " &
                   Base64Pad.encode(auth.toOpenArrayByte(0, len(auth) - 1))
      request.headers.add(AuthorizationHeader, header)

  # Here we perform automatic detection: if request was created with non-zero
  # body and `Content-Length` header is missing we will create one with size
  # of body stored in request.
  if ContentLengthHeader notin request.headers:
    if len(request.buffer) > 0:
      let slength = Base10.toString(uint64(len(request.buffer)))
      request.headers.add(ContentLengthHeader, slength)

  request.bodyFlag =
    if ContentLengthHeader in request.headers:
      HttpClientBodyFlag.Sized
    else:
      if request.hasChunkedEncoding():
        HttpClientBodyFlag.Chunked
      else:
        HttpClientBodyFlag.Custom

  let entity =
    block:
      var res =
        if len(request.address.path) > 0:
          request.address.path
        else:
          "/"
      if len(request.address.query) > 0:
        res.add("?")
        res.add(request.address.query)
      if len(request.address.anchor) > 0:
        res.add("#")
        res.add(request.address.anchor)
      res

  var res = $request.meth
  res.add(" ")
  res.add(entity)
  res.add(" ")
  res.add($request.version)
  res.add("\r\n")
  for k, v in request.headers.stringItems():
    if len(v) > 0:
      res.add(normalizeHeaderName(k))
      res.add(": ")
      res.add(v)
      res.add("\r\n")
  res.add("\r\n")
  res

proc send*(request: HttpClientRequestRef): Future[HttpClientResponseRef] {.
     async.} =
  doAssert(request.state == HttpClientRequestState.Created)
  request.setState(HttpClientRequestState.Connecting)
  request.connection =
    try:
      await request.session.acquireConnection(request.address)
    except CancelledError as exc:
      request.setError(newHttpInterruptError())
      raise exc
    except HttpError as exc:
      request.setError(exc)
      raise exc

  let headers = request.prepareRequest()

  try:
    request.setState(HttpClientRequestState.HeadersSending)
    await request.connection.writer.write(headers)
    request.setState(HttpClientRequestState.HeadersSent)
    request.setState(HttpClientRequestState.BodySending)
    if len(request.buffer) > 0:
      await request.connection.writer.write(request.buffer)
    request.setState(HttpClientRequestState.BodySent)
  except CancelledError as exc:
    request.setError(newHttpInterruptError())
    raise exc
  except AsyncStreamError as exc:
    let error = newHttpWriteError("Could not send request headers")
    request.setError(error)
    raise error

  let resp =
    try:
      await request.getResponse()
    except CancelledError as exc:
      request.setError(newHttpInterruptError())
      raise exc
    except HttpError as exc:
      request.setError(exc)
      raise exc
  return resp

proc open*(request: HttpClientRequestRef): Future[HttpBodyWriter] {.
     async.} =
  ## Start sending request's headers and return `HttpBodyWriter`, which can be
  ## used to send request's body.
  doAssert(request.state == HttpClientRequestState.Created)
  doAssert(len(request.buffer) == 0,
           "Request should not have static body content (len(buffer) == 0)")
  request.setState(HttpClientRequestState.Connecting)
  request.connection =
    try:
      await request.session.acquireConnection(request.address)
    except CancelledError as exc:
      request.setError(newHttpInterruptError())
      raise exc
    except HttpError as exc:
      request.setError(exc)
      raise exc

  let headers = request.prepareRequest()

  try:
    request.setState(HttpClientRequestState.HeadersSending)
    await request.connection.writer.write(headers)
    request.setState(HttpClientRequestState.HeadersSent)
  except CancelledError as exc:
    request.setError(newHttpInterruptError())
    raise exc
  except AsyncStreamError as exc:
    let error = newHttpWriteError("Could not send request headers")
    request.setError(error)
    raise error

  let writer =
    case request.bodyFlag
    of HttpClientBodyFlag.Sized:
      let size = Base10.decode(uint64,
                               request.headers.getString("content-length"))
      let writer = newBoundedStreamWriter(request.connection.writer, size.get())
      newHttpBodyWriter([AsyncStreamWriter(writer)])
    of HttpClientBodyFlag.Chunked:
      let writer = newChunkedStreamWriter(request.connection.writer)
      newHttpBodyWriter([AsyncStreamWriter(writer)])
    of HttpClientBodyFlag.Custom:
      let writer = newAsyncStreamWriter(request.connection.writer)
      newHttpBodyWriter([writer])

  request.writer = writer
  request.setState(HttpClientRequestState.BodySending)
  return writer

proc finish*(request: HttpClientRequestRef): Future[HttpClientResponseRef] {.
     async.} =
  ## Finish sending request and receive response.
  doAssert(request.state == HttpClientRequestState.BodySending)
  doAssert(request.connection.state ==
           HttpClientConnectionState.RequestBodySending)
  doAssert(request.writer.closed())
  request.setState(HttpClientRequestState.BodySent)
  let resp =
    try:
      await request.getResponse()
    except CancelledError as exc:
      request.setError(newHttpInterruptError())
      raise exc
    except HttpError as exc:
      request.setError(exc)
      raise exc
  return resp

proc getNewLocation*(resp: HttpClientResponseRef): HttpResult[HttpAddress] =
  ## Returns new address according to response's `Location` header value.
  if "location" in resp.headers:
    let location = resp.headers.getString("location")
    if len(location) > 0:
      resp.session.redirect(resp.address, parseUri(location))
    else:
      err("Location header with empty value")
  else:
    err("Location header is missing")

proc getBodyReader*(resp: HttpClientResponseRef): HttpBodyReader =
  ## Returns stream's reader instance which can be used to read response's body.
  ##
  ## Streams which was obtained using this procedure must be closed to avoid
  ## leaks.
  doAssert(resp.state in {
           HttpClientResponseState.HeadersReceived,
           HttpClientResponseState.BodyReceiving})
  doAssert(resp.connection.state in {
           HttpClientConnectionState.ResponseHeadersReceived,
           HttpClientConnectionState.ResponseBodyReceiving})
  if isNil(resp.reader):
    let reader =
      case resp.bodyFlag
      of HttpClientBodyFlag.Sized:
        let bstream = newBoundedStreamReader(resp.connection.reader,
                                             resp.contentLength)
        newHttpBodyReader(bstream)
      of HttpClientBodyFlag.Chunked:
        newHttpBodyReader(newChunkedStreamReader(resp.connection.reader))
      of HttpClientBodyFlag.Custom:
        newHttpBodyReader(newAsyncStreamReader(resp.connection.reader))
    resp.setState(HttpClientResponseState.BodyReceiving)
    resp.reader = reader
  resp.reader

proc finish*(resp: HttpClientResponseRef) {.async.} =
  ## Finish receiving response.
  doAssert(resp.state == HttpClientResponseState.BodyReceiving,
           $resp.state)
  doAssert(resp.connection.state ==
           HttpClientConnectionState.ResponseBodyReceiving,
           $resp.connection.state)
  doAssert(resp.reader.closed())
  resp.setState(HttpClientResponseState.BodyReceived)
  resp.connection.state = HttpClientConnectionState.Ready

proc getBodyBytes*(response: HttpClientResponseRef): Future[seq[byte]] {.
     async.} =
  ## Read all bytes from response ``response``.
  doAssert(response.state == HttpClientResponseState.HeadersReceived,
           $response.state)
  doAssert(response.connection.state ==
           HttpClientConnectionState.ResponseHeadersReceived,
           $response.connection.state)
  var reader = response.getBodyReader()
  try:
    let data = await reader.read()
    await reader.closeWait()
    reader = nil
    await response.finish()
    return data
  except CancelledError as exc:
    if not(isNil(reader)):
      awaitrc reader.closeWait()
    response.setError(newHttpInterruptError())
    raise exc
  except AsyncStreamError as exc:
    if not(isNil(reader)):
      awaitrc reader.closeWait()
    let error = newHttpReadError("Could not read response")
    response.setError(error)
    raise error

proc getBodyBytes*(response: HttpClientResponseRef,
                   nbytes: int): Future[seq[byte]] {.async.} =
  ## Read all bytes (nbytes <= 0) or exactly `nbytes` bytes from response
  ## ``response``.
  doAssert(response.state == HttpClientResponseState.HeadersReceived)
  doAssert(response.connection.state ==
           HttpClientConnectionState.ResponseHeadersReceived)
  var reader = response.getBodyReader()
  try:
    let data = await reader.read(nbytes)
    await reader.closeWait()
    reader = nil
    await response.finish()
    return data
  except CancelledError as exc:
    if not(isNil(reader)):
      awaitrc reader.closeWait()
    response.setError(newHttpInterruptError())
    raise exc
  except AsyncStreamError as exc:
    if not(isNil(reader)):
      awaitrc reader.closeWait()
    let error = newHttpReadError("Could not read response")
    response.setError(error)
    raise error

proc consumeBody*(response: HttpClientResponseRef): Future[int] {.async.} =
  ## Consume/discard response and return number of bytes consumed.
  doAssert(response.state == HttpClientResponseState.HeadersReceived)
  doAssert(response.connection.state ==
           HttpClientConnectionState.ResponseHeadersReceived)
  var reader = response.getBodyReader()
  try:
    let res = await reader.consume()
    await reader.closeWait()
    reader = nil
    await response.finish()
    return res
  except CancelledError as exc:
    if not(isNil(reader)):
      awaitrc reader.closeWait()
    response.setError(newHttpInterruptError())
    raise exc
  except AsyncStreamError as exc:
    if not(isNil(reader)):
      awaitrc reader.closeWait()
    let error = newHttpReadError("Could not read response")
    response.setError(error)
    raise error

proc redirect*(request: HttpClientRequestRef,
               ha: HttpAddress): HttpResult[HttpClientRequestRef] =
  ## Create new request object using original request object ``request`` and
  ## new redirected address ``ha``.
  ##
  ## This procedure could return an error if number of redirects exceeded
  ## maximum allowed number of redirects in request's session.
  let redirectCount = request.redirectCount + 1
  if redirectCount > request.session.maxRedirections:
    err("Maximum number of redirects exceeded")
  else:
    var res = HttpClientRequestRef.new(request.session, ha, request.meth,
      request.version, request.flags, request.headers.toList(), request.buffer)
    res.redirectCount = redirectCount
    ok(res)

proc redirect*(request: HttpClientRequestRef,
               uri: Uri): HttpResult[HttpClientRequestRef] =
  ## Create new request object using original request object ``request`` and
  ## redirected URL ``uri``.
  ##
  ## This procedure could return an error if number of redirects exceeded
  ## maximum allowed number of redirects in request's session or ``uri`` is
  ## incorrect or not supported.
  let redirectCount = request.redirectCount + 1
  if redirectCount > request.session.maxRedirections:
    err("Maximum number of redirects exceeded")
  else:
    let address = ? request.session.redirect(request.address, uri)
    var res = HttpClientRequestRef.new(request.session, address, request.meth,
      request.version, request.flags, request.headers.toList(), request.buffer)
    res.redirectCount = redirectCount
    ok(res)

proc fetch*(request: HttpClientRequestRef): Future[HttpResponseTuple] {.
     async.} =
  var response: HttpClientResponseRef
  try:
    response = await request.send()
    let buffer = await response.getBodyBytes()
    let status = response.status
    await response.closeWait()
    response = nil
    return (status, buffer)
  except HttpError as exc:
    if not(isNil(response)):
      awaitrc response.closeWait()
    raise exc
  except CancelledError as exc:
    if not(isNil(response)):
      awaitrc response.closeWait()
    raise exc

proc fetch*(session: HttpSessionRef, url: Uri): Future[HttpResponseTuple] {.
     async.} =
  ## Fetch resource pointed by ``url`` using HTTP GET method and ``session``
  ## parameters.
  ##
  ## This procedure supports HTTP redirections.
  let address =
    block:
      let res = session.getAddress(url)
      if res.isErr():
        raiseHttpAddressError(res.error())
      res.get()

  var
    request = HttpClientRequestRef.new(session, address)
    response: HttpClientResponseRef = nil
    redirect: HttpClientRequestRef = nil

  while true:
    try:
      response = await request.send()
      if response.status >= 300 and response.status < 400:
        redirect =
          block:
            if "location" in response.headers:
              let location = response.headers.getString("location")
              if len(location) > 0:
                let res = request.redirect(parseUri(location))
                if res.isErr():
                  raiseHttpRedirectError(res.error())
                res.get()
              else:
                raiseHttpRedirectError("Location header with an empty value")
            else:
              raiseHttpRedirectError("Location header missing")
        discard await response.consumeBody()
        await request.closeWait()
        request = nil
        await response.closeWait()
        response = nil
        request = redirect
        redirect = nil
      else:
        let data = await response.getBodyBytes()
        let code = response.status
        await request.closeWait()
        request = nil
        await response.closeWait()
        response = nil
        return (code, data)
    except CancelledError as exc:
      if not(isNil(request)):
        awaitrc closeWait(request)
      if not(isNil(redirect)):
        awaitrc closeWait(redirect)
      if not(isNil(response)):
        awaitrc closeWait(response)
      raise exc
    except HttpError as exc:
      if not(isNil(request)):
        awaitrc closeWait(request)
      if not(isNil(redirect)):
        awaitrc closeWait(redirect)
      if not(isNil(response)):
        awaitrc closeWait(response)
      raise exc
