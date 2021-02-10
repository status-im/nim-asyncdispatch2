#
#          Chronos Asynchronous TLS Stream
#             (c) Copyright 2019-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

## This module implements Transport Layer Security (TLS) stream. This module
## uses sources of BearSSL <https://www.bearssl.org> by Thomas Pornin.
import bearssl, bearssl/cacert
import ../asyncloop, ../timer, ../asyncsync
import asyncstream, ../transports/stream, ../transports/common

type
  TLSStreamKind {.pure.} = enum
    Client, Server

  TLSVersion* {.pure.} = enum
    TLS10 = 0x0301, TLS11 = 0x0302, TLS12 = 0x0303

  TLSFlags* {.pure.} = enum
    NoVerifyHost,         # Client: Skip remote certificate check
    NoVerifyServerName,   # Client: Skip Server Name Indication (SNI) check
    EnforceServerPref,    # Server: Enforce server preferences
    NoRenegotiation,      # Server: Reject renegotiations requests
    TolerateNoClientAuth, # Server: Disable strict client authentication
    FailOnAlpnMismatch    # Server: Fail on application protocol mismatch

  TLSKeyType {.pure.} = enum
    RSA, EC

  TLSPrivateKey* = ref object
    case kind: TLSKeyType
    of RSA:
      rsakey: RsaPrivateKey
    of EC:
      eckey: EcPrivateKey
    storage: seq[byte]

  TLSCertificate* = ref object
    certs: seq[X509Certificate]
    storage: seq[byte]

  TLSSessionCache* = ref object
    storage: seq[byte]
    context: SslSessionCacheLru

  PEMElement* = object
    name*: string
    data*: seq[byte]

  PEMContext = ref object
    data: seq[byte]

  TLSStreamWriter* = ref object of AsyncStreamWriter
    case kind: TLSStreamKind
    of TLSStreamKind.Client:
      ccontext: ptr SslClientContext
    of TLSStreamKind.Server:
      scontext: ptr SslServerContext
    stream*: TLSAsyncStream
    switchToReader*: AsyncEvent
    switchToWriter*: AsyncEvent
    handshaked*: bool
    handshakeFut*: Future[void]

  TLSStreamReader* = ref object of AsyncStreamReader
    case kind: TLSStreamKind
    of TLSStreamKind.Client:
      ccontext: ptr SslClientContext
    of TLSStreamKind.Server:
      scontext: ptr SslServerContext
    stream*: TLSAsyncStream
    switchToReader*: AsyncEvent
    switchToWriter*: AsyncEvent
    handshaked*: bool
    handshakeFut*: Future[void]

  TLSAsyncStream* = ref object of RootRef
    xwc*: X509NoAnchorContext
    ccontext*: SslClientContext
    scontext*: SslServerContext
    sbuffer*: seq[byte]
    x509*: X509MinimalContext
    reader*: TLSStreamReader
    writer*: TLSStreamWriter

  SomeTLSStreamType* = TLSStreamReader|TLSStreamWriter|TLSAsyncStream

  TLSStreamError* = object of AsyncStreamError
  TLSStreamProtocolError* = object of TLSStreamError
    errCode*: int

template newTLSStreamProtocolError[T](message: T): ref TLSStreamProtocolError =
  var msg = ""
  var code = 0
  when T is string:
    msg.add(message)
  elif T is cint:
    msg.add(sslErrorMsg(message) & " (code: " & $int(message) & ")")
    code = int(message)
  elif T is int:
    msg.add(sslErrorMsg(message) & " (code: " & $message & ")")
    code = message
  else:
    msg.add("Internal Error")
  var err = newException(TLSStreamProtocolError, msg)
  err.errCode = code
  err

template raiseTLSStreamProtoError*[T](message: T) =
  raise newTLSStreamProtocolError(message)

proc tlsWriteLoop(stream: AsyncStreamWriter) {.async.} =
  var wstream = cast[TLSStreamWriter](stream)
  var engine: ptr SslEngineContext
  var error: ref AsyncStreamError

  if wstream.kind == TLSStreamKind.Server:
    engine = addr wstream.scontext.eng
  else:
    engine = addr wstream.ccontext.eng

  wstream.state = AsyncStreamState.Running

  while true:
    var item: WriteItem
    try:
      var state = engine.sslEngineCurrentState()
      if (state and SSL_CLOSED) == SSL_CLOSED:
        wstream.state = AsyncStreamState.Finished
      else:
        if (state and (SSL_RECVREC or SSL_RECVAPP)) != 0:
          if not(wstream.switchToReader.isSet()):
            wstream.switchToReader.fire()
        if (state and (SSL_SENDREC or SSL_SENDAPP)) == 0:
          await wstream.switchToWriter.wait()
          wstream.switchToWriter.clear()
          # We need to refresh `state` because we just returned from readerLoop.
        else:
          if (state and SSL_SENDREC) == SSL_SENDREC:
            # TLS record needs to be sent over stream.
            var length = 0'u
            var buf = sslEngineSendrecBuf(engine, length)
            doAssert(length != 0 and not isNil(buf))
            await wstream.wsource.write(buf, int(length))
            sslEngineSendrecAck(engine, length)
          elif (state and SSL_SENDAPP) == SSL_SENDAPP:
            # Application data can be sent over stream.
            if not(wstream.handshaked):
              wstream.stream.reader.handshaked = true
              wstream.handshaked = true
              if not(isNil(wstream.handshakeFut)):
                wstream.handshakeFut.complete()
            item = await wstream.queue.get()
            if item.size > 0:
              var length = 0'u
              var buf = sslEngineSendappBuf(engine, length)
              let toWrite = min(int(length), item.size)
              copyOut(buf, item, toWrite)
              if int(length) >= item.size:
                # BearSSL is ready to accept whole item size.
                sslEngineSendappAck(engine, uint(item.size))
                sslEngineFlush(engine, 0)
                item.future.complete()
              else:
                # BearSSL is not ready to accept whole item, so we will send
                # only part of item and adjust offset.
                item.offset = item.offset + int(length)
                item.size = item.size - int(length)
                wstream.queue.addFirstNoWait(item)
                sslEngineSendappAck(engine, length)
            else:
              # Zero length item means finish, so we going to trigger TLS
              # closure protocol.
              sslEngineClose(engine)
    except CancelledError:
      wstream.state = AsyncStreamState.Stopped
      error = newAsyncStreamUseClosedError()
    except AsyncStreamError as exc:
      wstream.state = AsyncStreamState.Error
      error = exc

    if wstream.state != AsyncStreamState.Running:
      if wstream.state == AsyncStreamState.Finished:
        error = newAsyncStreamUseClosedError()
      else:
        if not(isNil(item.future)):
          if not(item.future.finished()):
            item.future.fail(error)
      while not(wstream.queue.empty()):
        let pitem = wstream.queue.popFirstNoWait()
        if not(pitem.future.finished()):
          pitem.future.fail(error)
      wstream.stream = nil
      break

proc tlsReadLoop(stream: AsyncStreamReader) {.async.} =
  var rstream = cast[TLSStreamReader](stream)
  var engine: ptr SslEngineContext

  if rstream.kind == TLSStreamKind.Server:
    engine = addr rstream.scontext.eng
  else:
    engine = addr rstream.ccontext.eng

  rstream.state = AsyncStreamState.Running

  while true:
    try:
      var state = engine.sslEngineCurrentState()
      if (state and SSL_CLOSED) == SSL_CLOSED:
        let err = engine.sslEngineLastError()
        if err != 0:
          rstream.error = newTLSStreamProtocolError(err)
          rstream.state = AsyncStreamState.Error
        else:
          rstream.state = AsyncStreamState.Finished
      else:
        if (state and (SSL_SENDREC or SSL_SENDAPP)) != 0:
          if not(rstream.switchToWriter.isSet()):
            rstream.switchToWriter.fire()
        if (state and (SSL_RECVREC or SSL_RECVAPP)) == 0:
          await rstream.switchToReader.wait()
          rstream.switchToReader.clear()
          # We need to refresh `state` because we just returned from writerLoop.
        else:
          if (state and SSL_RECVREC) == SSL_RECVREC:
            # TLS records required for further processing
            var length = 0'u
            var buf = sslEngineRecvrecBuf(engine, length)
            let res = await rstream.rsource.readOnce(buf, int(length))
            if res > 0:
              sslEngineRecvrecAck(engine, uint(res))
            else:
              # readOnce() returns `0` if stream is at EOF, so we initiate TLS
              # closure procedure.
              sslEngineClose(engine)
          elif (state and SSL_RECVAPP) == SSL_RECVAPP:
            # Application data can be recovered.
            var length = 0'u
            var buf = sslEngineRecvappBuf(engine, length)
            await upload(addr rstream.buffer, buf, int(length))
            sslEngineRecvappAck(engine, length)
    except CancelledError:
      rstream.state = AsyncStreamState.Stopped
    except AsyncStreamError as exc:
      rstream.error = exc
      rstream.state = AsyncStreamState.Error
      if not(rstream.handshaked):
        rstream.handshaked = true
        rstream.stream.writer.handshaked = true
        if not(isNil(rstream.handshakeFut)):
          rstream.handshakeFut.fail(rstream.error)
        rstream.switchToWriter.fire()

    if rstream.state != AsyncStreamState.Running:
      # Perform TLS cleanup procedure
      if rstream.state != AsyncStreamState.Finished:
        sslEngineClose(engine)
      rstream.buffer.forget()
      rstream.stream = nil
      break

proc getSignerAlgo(xc: X509Certificate): int =
  ## Get certificate's signing algorithm.
  var dc: X509DecoderContext
  x509DecoderInit(addr dc, nil, nil)
  x509DecoderPush(addr dc, xc.data, xc.dataLen)
  let err = x509DecoderLastError(addr dc)
  if err != 0:
    -1
  else:
    int(x509DecoderGetSignerKeyType(addr dc))

proc newTLSClientAsyncStream*(rsource: AsyncStreamReader,
                              wsource: AsyncStreamWriter,
                              serverName: string,
                              bufferSize = SSL_BUFSIZE_BIDI,
                              minVersion = TLSVersion.TLS11,
                              maxVersion = TLSVersion.TLS12,
                              flags: set[TLSFlags] = {}): TLSAsyncStream =
  ## Create new TLS asynchronous stream for outbound (client) connections
  ## using reading stream ``rsource`` and writing stream ``wsource``.
  ##
  ## You can specify remote server name using ``serverName``, if while
  ## handshake server reports different name you will get an error. If
  ## ``serverName`` is empty string, remote server name checking will be
  ## disabled.
  ##
  ## ``bufferSize`` - is SSL/TLS buffer which is used for encoding/decoding
  ## incoming data.
  ##
  ## ``minVersion`` and ``maxVersion`` are TLS versions which will be used
  ## for handshake with remote server. If server's version will be lower then
  ## ``minVersion`` of bigger then ``maxVersion`` you will get an error.
  ##
  ## ``flags`` - custom TLS connection flags.
  let switchToWriter = newAsyncEvent()
  let switchToReader = newAsyncEvent()
  var res = TLSAsyncStream()
  var reader = TLSStreamReader(
    kind: TLSStreamKind.Client,
    stream: res,
    switchToReader: switchToReader,
    switchToWriter: switchToWriter,
    ccontext: addr res.ccontext
  )
  var writer = TLSStreamWriter(
    kind: TLSStreamKind.Client,
    stream: res,
    switchToReader: switchToReader,
    switchToWriter: switchToWriter,
    ccontext: addr res.ccontext
  )
  res.reader = reader
  res.writer = writer

  if TLSFlags.NoVerifyHost in flags:
    sslClientInitFull(addr res.ccontext, addr res.x509, nil, 0)
    initNoAnchor(addr res.xwc, addr res.x509.vtable)
    sslEngineSetX509(addr res.ccontext.eng, addr res.xwc.vtable)
  else:
    sslClientInitFull(addr res.ccontext, addr res.x509,
                      unsafeAddr MozillaTrustAnchors[0],
                      len(MozillaTrustAnchors))

  let size = max(SSL_BUFSIZE_BIDI, bufferSize)
  res.sbuffer = newSeq[byte](size)
  sslEngineSetBuffer(addr res.ccontext.eng, addr res.sbuffer[0],
                     uint(len(res.sbuffer)), 1)
  sslEngineSetVersions(addr res.ccontext.eng, uint16(minVersion),
                       uint16(maxVersion))

  if TLSFlags.NoVerifyServerName in flags:
    let err = sslClientReset(addr res.ccontext, "", 0)
    if err == 0:
      raise newException(TLSStreamError, "Could not initialize TLS layer")
  else:
    if len(serverName) == 0:
      raise newException(TLSStreamError, "serverName must not be empty string")

    let err = sslClientReset(addr res.ccontext, serverName, 0)
    if err == 0:
      raise newException(TLSStreamError, "Could not initialize TLS layer")

  init(cast[AsyncStreamWriter](res.writer), wsource, tlsWriteLoop,
       bufferSize)
  init(cast[AsyncStreamReader](res.reader), rsource, tlsReadLoop,
       bufferSize)
  res

proc newTLSServerAsyncStream*(rsource: AsyncStreamReader,
                              wsource: AsyncStreamWriter,
                              privateKey: TLSPrivateKey,
                              certificate: TLSCertificate,
                              bufferSize = SSL_BUFSIZE_BIDI,
                              minVersion = TLSVersion.TLS11,
                              maxVersion = TLSVersion.TLS12,
                              cache: TLSSessionCache = nil,
                              flags: set[TLSFlags] = {}): TLSAsyncStream =
  ## Create new TLS asynchronous stream for inbound (server) connections
  ## using reading stream ``rsource`` and writing stream ``wsource``.
  ##
  ## You need to specify local private key ``privateKey`` and certificate
  ## ``certificate``.
  ##
  ## ``bufferSize`` - is SSL/TLS buffer which is used for encoding/decoding
  ## incoming data.
  ##
  ## ``minVersion`` and ``maxVersion`` are TLS versions which will be used
  ## for handshake with remote server. If server's version will be lower then
  ## ``minVersion`` of bigger then ``maxVersion`` you will get an error.
  ##
  ## ``flags`` - custom TLS connection flags.
  if isNil(privateKey) or privateKey.kind notin {TLSKeyType.RSA, TLSKeyType.EC}:
    raiseTLSStreamProtoError("Incorrect private key")
  if isNil(certificate) or len(certificate.certs) == 0:
    raiseTLSStreamProtoError("Incorrect certificate")

  let switchToWriter = newAsyncEvent()
  let switchToReader = newAsyncEvent()

  var res = TLSAsyncStream()
  var reader = TLSStreamReader(
    kind: TLSStreamKind.Server,
    stream: res,
    switchToReader: switchToReader,
    switchToWriter: switchToWriter,
    scontext: addr res.scontext
  )
  var writer = TLSStreamWriter(
    kind: TLSStreamKind.Server,
    stream: res,
    switchToReader: switchToReader,
    switchToWriter: switchToWriter,
    scontext: addr res.scontext
  )
  res.reader = reader
  res.writer = writer

  if privateKey.kind == TLSKeyType.EC:
    let algo = getSignerAlgo(certificate.certs[0])
    if algo == -1:
      raiseTLSStreamProtoError("Could not decode certificate")
    sslServerInitFullEc(addr res.scontext, addr certificate.certs[0],
                        len(certificate.certs), cuint(algo),
                        addr privateKey.eckey)
  elif privateKey.kind == TLSKeyType.RSA:
    sslServerInitFullRsa(addr res.scontext, addr certificate.certs[0],
                         len(certificate.certs), addr privateKey.rsakey)

  let size = max(SSL_BUFSIZE_BIDI, bufferSize)
  res.sbuffer = newSeq[byte](size)
  sslEngineSetBuffer(addr res.scontext.eng, addr res.sbuffer[0],
                     uint(len(res.sbuffer)), 1)
  sslEngineSetVersions(addr res.scontext.eng, uint16(minVersion),
                       uint16(maxVersion))

  if not isNil(cache):
    sslServerSetCache(addr res.scontext, addr cache.context.vtable)

  if TLSFlags.EnforceServerPref in flags:
    sslEngineAddFlags(addr res.scontext.eng, OPT_ENFORCE_SERVER_PREFERENCES)
  if TLSFlags.NoRenegotiation in flags:
    sslEngineAddFlags(addr res.scontext.eng, OPT_NO_RENEGOTIATION)
  if TLSFlags.TolerateNoClientAuth in flags:
    sslEngineAddFlags(addr res.scontext.eng, OPT_TOLERATE_NO_CLIENT_AUTH)
  if TLSFlags.FailOnAlpnMismatch in flags:
    sslEngineAddFlags(addr res.scontext.eng, OPT_FAIL_ON_ALPN_MISMATCH)

  let err = sslServerReset(addr res.scontext)
  if err == 0:
    raise newException(TLSStreamError, "Could not initialize TLS layer")

  init(cast[AsyncStreamWriter](res.writer), wsource, tlsWriteLoop,
       bufferSize)
  init(cast[AsyncStreamReader](res.reader), rsource, tlsReadLoop,
       bufferSize)
  res

proc copyKey(src: RsaPrivateKey): TLSPrivateKey =
  ## Creates copy of RsaPrivateKey ``src``.
  var offset = 0
  let keySize = src.plen + src.qlen + src.dplen + src.dqlen + src.iqlen
  var res = TLSPrivateKey(kind: TLSKeyType.RSA, storage: newSeq[byte](keySize))
  copyMem(addr res.storage[offset], src.p, src.plen)
  res.rsakey.p = cast[ptr cuchar](addr res.storage[offset])
  res.rsakey.plen = src.plen
  offset = offset + src.plen
  copyMem(addr res.storage[offset], src.q, src.qlen)
  res.rsakey.q = cast[ptr cuchar](addr res.storage[offset])
  res.rsakey.qlen = src.qlen
  offset = offset + src.qlen
  copyMem(addr res.storage[offset], src.dp, src.dplen)
  res.rsakey.dp = cast[ptr cuchar](addr res.storage[offset])
  res.rsakey.dplen = src.dplen
  offset = offset + src.dplen
  copyMem(addr res.storage[offset], src.dq, src.dqlen)
  res.rsakey.dq = cast[ptr cuchar](addr res.storage[offset])
  res.rsakey.dqlen = src.dqlen
  offset = offset + src.dqlen
  copyMem(addr res.storage[offset], src.iq, src.iqlen)
  res.rsakey.iq = cast[ptr cuchar](addr res.storage[offset])
  res.rsakey.iqlen = src.iqlen
  res.rsakey.nBitlen = src.nBitlen
  res

proc copyKey(src: EcPrivateKey): TLSPrivateKey =
  ## Creates copy of EcPrivateKey ``src``.
  var offset = 0
  let keySize = src.xlen
  var res = TLSPrivateKey(kind: TLSKeyType.EC, storage: newSeq[byte](keySize))
  copyMem(addr res.storage[offset], src.x, src.xlen)
  res.eckey.x = cast[ptr cuchar](addr res.storage[offset])
  res.eckey.xlen = src.xlen
  res.eckey.curve = src.curve
  res

proc init*(tt: typedesc[TLSPrivateKey], data: openarray[byte]): TLSPrivateKey =
  ## Initialize TLS private key from array of bytes ``data``.
  ##
  ## This procedure initializes private key using raw, DER-encoded format,
  ## or wrapped in an unencrypted PKCS#8 archive (again DER-encoded).
  var ctx: SkeyDecoderContext
  if len(data) == 0:
    raiseTLSStreamProtoError("Incorrect private key")
  skeyDecoderInit(addr ctx)
  skeyDecoderPush(addr ctx, cast[pointer](unsafeAddr data[0]), len(data))
  let err = skeyDecoderLastError(addr ctx)
  if err != 0:
    raiseTLSStreamProtoError(err)
  let keyType = skeyDecoderKeyType(addr ctx)
  let res =
    if keyType == KEYTYPE_RSA:
      copyKey(ctx.key.rsa)
    elif keyType == KEYTYPE_EC:
      copyKey(ctx.key.ec)
    else:
      raiseTLSStreamProtoError("Unknown key type (" & $keyType & ")")
  res

proc pemDecode*(data: openarray[char]): seq[PEMElement] =
  ## Decode PEM encoded string and get array of binary blobs.
  if len(data) == 0:
    raiseTLSStreamProtoError("Empty PEM message")
  var ctx: PemDecoderContext
  var pctx = new PEMContext
  var res = newSeq[PEMElement]()
  pemDecoderInit(addr ctx)

  proc itemAppend(ctx: pointer, pbytes: pointer, nbytes: int) {.cdecl.} =
    var p = cast[PEMContext](ctx)
    var o = len(p.data)
    p.data.setLen(o + nbytes)
    copyMem(addr p.data[o], pbytes, nbytes)

  var length = len(data)
  var offset = 0
  var inobj = false
  var elem: PEMElement

  while length > 0:
    var tlen = pemDecoderPush(addr ctx,
                              cast[pointer](unsafeAddr data[offset]), length)
    offset = offset + tlen
    length = length - tlen

    let event = pemDecoderEvent(addr ctx)
    if event == PEM_BEGIN_OBJ:
      inobj = true
      elem.name = $pemDecoderName(addr ctx)
      pctx.data = newSeq[byte]()
      pemDecoderSetdest(addr ctx, itemAppend, cast[pointer](pctx))
    elif event == PEM_END_OBJ:
      if inobj:
        elem.data = pctx.data
        res.add(elem)
        inobj = false
      else:
        break
    else:
      raiseTLSStreamProtoError("Invalid PEM encoding")
  res

proc init*(tt: typedesc[TLSPrivateKey], data: openarray[char]): TLSPrivateKey =
  ## Initialize TLS private key from string ``data``.
  ##
  ## This procedure initializes private key using unencrypted PKCS#8 PEM
  ## encoded string.
  ##
  ## Note that PKCS#1 PEM encoded objects are not supported.
  var res: TLSPrivateKey
  var items = pemDecode(data)
  for item in items:
    if item.name == "PRIVATE KEY":
      res = TLSPrivateKey.init(item.data)
      break
  if isNil(res):
    raiseTLSStreamProtoError("Could not find private key")
  res

proc init*(tt: typedesc[TLSCertificate],
           data: openarray[char]): TLSCertificate =
  ## Initialize TLS certificates from string ``data``.
  ##
  ## This procedure initializes array of certificates from PEM encoded string.
  var items = pemDecode(data)
  var res = TLSCertificate()
  for item in items:
    if item.name == "CERTIFICATE" and len(item.data) > 0:
      let offset = len(res.storage)
      res.storage.add(item.data)
      let cert = X509Certificate(
        data: cast[ptr cuchar](addr res.storage[offset]),
        dataLen: len(item.data)
      )
      let ares = getSignerAlgo(cert)
      if ares == -1:
        raiseTLSStreamProtoError("Could not decode certificate")
      elif ares != KEYTYPE_RSA and ares != KEYTYPE_EC:
        raiseTLSStreamProtoError("Unsupported signing key type in certificate")
      res.certs.add(cert)
  if len(res.storage) == 0:
    raiseTLSStreamProtoError("Could not find any certificates")
  res

proc init*(tt: typedesc[TLSSessionCache], size: int = 4096): TLSSessionCache =
  ## Create new TLS session cache with size ``size``.
  ##
  ## One cached item is near 100 bytes size.
  var rsize = min(size, 4096)
  var res = TLSSessionCache(storage: newSeq[byte](rsize))
  sslSessionCacheLruInit(addr res.context, addr res.storage[0], rsize)
  res

proc handshake*(rws: SomeTLSStreamType): Future[void] =
  ## Wait until initial TLS handshake will be successfully performed.
  var retFuture = newFuture[void]("tlsstream.handshake")
  when rws is TLSStreamReader:
    if rws.handshaked:
      retFuture.complete()
    else:
      rws.handshakeFut = retFuture
      rws.stream.writer.handshakeFut = retFuture
  elif rws is TLSStreamWriter:
    if rws.handshaked:
      retFuture.complete()
    else:
      rws.handshakeFut = retFuture
      rws.stream.reader.handshakeFut = retFuture
  elif rws is TLSAsyncStream:
    if rws.reader.handshaked:
      retFuture.complete()
    else:
      rws.reader.handshakeFut = retFuture
      rws.writer.handshakeFut = retFuture
  retFuture
