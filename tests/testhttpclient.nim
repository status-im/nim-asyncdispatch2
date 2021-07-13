#                Chronos Test Suite
#            (c) Copyright 2021-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import std/[strutils, strutils, sha1]
import unittest2
import ../chronos, ../chronos/apps/http/[httpserver, shttpserver, httpclient]
import stew/base10

when defined(nimHasUsed): {.used.}

# To create self-signed certificate and key you can use openssl
# openssl req -new -x509 -sha256 -newkey rsa:2048 -nodes \
# -keyout example-com.key.pem -days 3650 -out example-com.cert.pem
const HttpsSelfSignedRsaKey = """
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCn7tXGLKMIMzOG
tVzUixax1/ftlSLcpEAkZMORuiCCnYjtIJhGZdzRFZC8fBlfAJZpLIAOfX2L2f1J
ZuwpwDkOIvNqKMBrl5Mvkl5azPT0rtnjuwrcqN5NFtbmZPKFYvbjex2aXGqjl5MW
nQIs/ZA++DVEXmaN9oDxcZsvRMDKfrGQf9iLeoVL47Gx9KpqNqD/JLIn4LpieumV
yYidm6ukTOqHRvrWm36y6VvKW4TE97THacULmkeahtTf8zDJbbh4EO+gifgwgJ2W
BUS0+5hMcWu8111mXmanlOVlcoW8fH8RmPjL1eK1Z3j3SVHEf7oWZtIVW5gGA0jQ
nfA4K51RAgMBAAECggEANZ7/R13tWKrwouy6DWuz/WlWUtgx333atUQvZhKmWs5u
cDjeJmxUC7b1FhoSB9GqNT7uTLIpKkSaqZthgRtNnIPwcU890Zz+dEwqMJgNByvl
it+oYjjRco/+YmaNQaYN6yjelPE5Y678WlYb4b29Fz4t0/zIhj/VgEKkKH2tiXpS
TIicoM7pSOscEUfaW3yp5bS5QwNU6/AaF1wws0feBACd19ZkcdPvr52jopbhxlXw
h3XTV/vXIJd5zWGp0h/Jbd4xcD4MVo2GjfkeORKY6SjDaNzt8OGtePcKnnbUVu8b
2XlDxukhDQXqJ3g0sHz47mhvo4JeIM+FgymRm+3QmQKBgQDTawrEA3Zy9WvucaC7
Zah02oE9nuvpF12lZ7WJh7+tZ/1ss+Fm7YspEKaUiEk7nn1CAVFtem4X4YCXTBiC
Oqq/o+ipv1yTur0ae6m4pwLm5wcMWBh3H5zjfQTfrClNN8yjWv8u3/sq8KesHPnT
R92/sMAptAChPgTzQphWbxFiYwKBgQDLWFaBqXfZYVnTyUvKX8GorS6jGWc6Eh4l
lAFA+2EBWDICrUxsDPoZjEXrWCixdqLhyehaI3KEFIx2bcPv6X2c7yx3IG5lA/Gx
TZiKlY74c6jOTstkdLW9RJbg1VUHUVZMf/Owt802YmEfUI5S5v7jFmKW6VG+io+K
+5KYeHD1uwKBgQDMf53KPA82422jFwYCPjLT1QduM2q97HwIomhWv5gIg63+l4BP
rzYMYq6+vZUYthUy41OAMgyLzPQ1ZMXQMi83b7R9fTxvKRIBq9xfYCzObGnE5vHD
SDDZWvR75muM5Yxr9nkfPkgVIPMO6Hg+hiVYZf96V0LEtNjU9HWmJYkLQQKBgQCQ
ULGUdGHKtXy7AjH3/t3CiKaAupa4cANVSCVbqQy/l4hmvfdu+AbH+vXkgTzgNgKD
nHh7AI1Vj//gTSayLlQn/Nbh9PJkXtg5rYiFUn+VdQBo6yMOuIYDPZqXFtCx0Nge
kvCwisHpxwiG4PUhgS+Em259DDonsM8PJFx2OYRx4QKBgEQpGhg71Oi9MhPJshN7
dYTowaMS5eLTk2264ARaY+hAIV7fgvUa+5bgTVaWL+Cfs33hi4sMRqlEwsmfds2T
cnQiJ4cU20Euldfwa5FLnk6LaWdOyzYt/ICBJnKFRwfCUbS4Bu5rtMEM+3t0wxnJ
IgaD04WhoL9EX0Qo3DC1+0kG
-----END PRIVATE KEY-----
"""

# This SSL certificate will expire 13 October 2030.
const HttpsSelfSignedRsaCert = """
-----BEGIN CERTIFICATE-----
MIIDnzCCAoegAwIBAgIUUdcusjDd3XQi3FPM8urdFG3qI+8wDQYJKoZIhvcNAQEL
BQAwXzELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDEYMBYGA1UEAwwPMTI3LjAuMC4xOjQz
ODA4MB4XDTIwMTAxMjIxNDUwMVoXDTMwMTAxMDIxNDUwMVowXzELMAkGA1UEBhMC
QVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVybmV0IFdpZGdp
dHMgUHR5IEx0ZDEYMBYGA1UEAwwPMTI3LjAuMC4xOjQzODA4MIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAp+7VxiyjCDMzhrVc1IsWsdf37ZUi3KRAJGTD
kboggp2I7SCYRmXc0RWQvHwZXwCWaSyADn19i9n9SWbsKcA5DiLzaijAa5eTL5Je
Wsz09K7Z47sK3KjeTRbW5mTyhWL243sdmlxqo5eTFp0CLP2QPvg1RF5mjfaA8XGb
L0TAyn6xkH/Yi3qFS+OxsfSqajag/ySyJ+C6YnrplcmInZurpEzqh0b61pt+sulb
yluExPe0x2nFC5pHmobU3/MwyW24eBDvoIn4MICdlgVEtPuYTHFrvNddZl5mp5Tl
ZXKFvHx/EZj4y9XitWd490lRxH+6FmbSFVuYBgNI0J3wOCudUQIDAQABo1MwUTAd
BgNVHQ4EFgQUBKha84woY5WkFxKw7qx1cONg1H8wHwYDVR0jBBgwFoAUBKha84wo
Y5WkFxKw7qx1cONg1H8wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOC
AQEAHZMYt9Ry+Xj3vTbzpGFQzYQVTJlfJWSN6eWNOivRFQE5io9kOBEe5noa8aLo
dLkw6ztxRP2QRJmlhGCO9/HwS17ckrkgZp3EC2LFnzxcBmoZu+owfxOT1KqpO52O
IKOl8eVohi1pEicE4dtTJVcpI7VCMovnXUhzx1Ci4Vibns4a6H+BQa19a1JSpifN
tO8U5jkjJ8Jprs/VPFhJj2O3di53oDHaYSE5eOrm2ZO14KFHSk9cGcOGmcYkUv8B
nV5vnGadH5Lvfxb/BCpuONabeRdOxMt9u9yQ89vNpxFtRdZDCpGKZBCfmUP+5m3m
N8r5CwGcIX/XPC3lKazzbZ8baA==
-----END CERTIFICATE-----
"""

suite "HTTP client testing suite":

  proc createBigMessage(message: string, size: int): seq[byte] =
    var res = newSeq[byte](size)
    for i in 0 ..< len(res):
      res[i] = byte(message[i mod len(message)])
    res

  proc createServer(address: TransportAddress,
                    process: HttpProcessCallback, secure: bool): HttpServerRef =
    let socketFlags = {ServerFlags.TcpNoDelay, ServerFlags.ReuseAddr}
    if secure:
      let secureKey = TLSPrivateKey.init(HttpsSelfSignedRsaKey)
      let secureCert = TLSCertificate.init(HttpsSelfSignedRsaCert)
      let res = SecureHttpServerRef.new(address, process,
                                        socketFlags = socketFlags,
                                        tlsPrivateKey = secureKey,
                                        tlsCertificate = secureCert)
      HttpServerRef(res.get())
    else:
      let res = HttpServerRef.new(address, process, socketFlags = socketFlags)
      res.get()

  proc createSession(secure: bool,
                     maxRedirections = HttpMaxRedirections): HttpSessionRef =
    if secure:
      HttpSessionRef.new({HttpClientFlag.NoVerifyHost,
                          HttpClientFlag.NoVerifyServerName},
                         maxRedirections = maxRedirections)
    else:
      HttpSessionRef.new(maxRedirections = maxRedirections)

  proc testMethods(address: TransportAddress,
                   secure: bool): Future[int] {.async.} =
    let RequestTests = [
      (MethodGet, "/test/get"),
      (MethodPost, "/test/post"),
      (MethodHead, "/test/head"),
      (MethodPut, "/test/put"),
      (MethodDelete, "/test/delete"),
      (MethodTrace, "/test/trace"),
      (MethodOptions, "/test/options"),
      (MethodConnect, "/test/connect"),
      (MethodPatch, "/test/patch")
    ]
    proc process(r: RequestFence): Future[HttpResponseRef] {.
         async.} =
      if r.isOk():
        let request = r.get()
        case request.uri.path
        of "/test/get", "/test/post", "/test/head", "/test/put",
           "/test/delete", "/test/trace", "/test/options", "/test/connect",
           "/test/patch", "/test/error":
          return await request.respond(Http200, request.uri.path)
        else:
          return await request.respond(Http404, "Page not found")
      else:
        return dumbResponse()

    var server = createServer(address, process, secure)
    server.start()
    var counter = 0

    var session = createSession(secure)

    for item in RequestTests:
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, item[1])
        else:
          getAddress(address, HttpClientScheme.NonSecure, item[1])
      var req = HttpClientRequestRef.new(session, ha, item[0])
      let response = await fetch(req)
      if response.status == 200:
        let data = cast[string](response.data)
        if data == item[1]:
          inc(counter)
      await req.closeWait()
    await session.closeWait()

    for item in RequestTests:
      var session = createSession(secure)
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, item[1])
        else:
          getAddress(address, HttpClientScheme.NonSecure, item[1])
      var req = HttpClientRequestRef.new(session, ha, item[0])
      let response = await fetch(req)
      if response.status == 200:
        let data = cast[string](response.data)
        if data == item[1]:
          inc(counter)
      await req.closeWait()
      await session.closeWait()

    await server.stop()
    await server.closeWait()
    return counter

  proc testResponseStreamReadingTest(address: TransportAddress,
                                     secure: bool): Future[int] {.async.} =
    let ResponseTests = [
      (MethodGet, "/test/short_size_response", 65600, 1024,
       "SHORTSIZERESPONSE"),
      (MethodGet, "/test/long_size_response", 262400, 1024,
       "LONGSIZERESPONSE"),
      (MethodGet, "/test/short_chunked_response", 65600, 1024,
       "SHORTCHUNKRESPONSE"),
      (MethodGet, "/test/long_chunked_response", 262400, 1024,
       "LONGCHUNKRESPONSE")
    ]
    proc process(r: RequestFence): Future[HttpResponseRef] {.
         async.} =
      if r.isOk():
        let request = r.get()
        case request.uri.path
        of "/test/short_size_response":
          var response = request.getResponse()
          var data = createBigMessage(ResponseTests[0][4], ResponseTests[0][2])
          response.status = Http200
          await response.sendBody(data)
          return response
        of "/test/long_size_response":
          var response = request.getResponse()
          var data = createBigMessage(ResponseTests[1][4], ResponseTests[1][2])
          response.status = Http200
          await response.sendBody(data)
          return response
        of "/test/short_chunked_response":
          var response = request.getResponse()
          var data = createBigMessage(ResponseTests[2][4], ResponseTests[2][2])
          response.status = Http200
          await response.prepare()
          var offset = 0
          while true:
            if len(data) == offset:
              break
            let toWrite = min(1024, len(data) - offset)
            await response.sendChunk(addr data[offset], toWrite)
            offset = offset + toWrite
          await response.finish()
          return response
        of "/test/long_chunked_response":
          var response = request.getResponse()
          var data = createBigMessage(ResponseTests[3][4], ResponseTests[3][2])
          response.status = Http200
          await response.prepare()
          var offset = 0
          while true:
            if len(data) == offset:
              break
            let toWrite = min(1024, len(data) - offset)
            await response.sendChunk(addr data[offset], toWrite)
            offset = offset + toWrite
          await response.finish()
          return response
        else:
          return await request.respond(Http404, "Page not found")
      else:
        return dumbResponse()

    var server = createServer(address, process, secure)
    server.start()
    var counter = 0

    var session = createSession(secure)
    for item in ResponseTests:
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, item[1])
        else:
          getAddress(address, HttpClientScheme.NonSecure, item[1])
      var req = HttpClientRequestRef.new(session, ha, item[0])
      var response = await send(req)
      if response.status == 200:
        var reader = response.getBodyReader()
        var res: seq[byte]
        while true:
          var data = await reader.read(item[3])
          res.add(data)
          if len(data) != item[3]:
            break
        await reader.closeWait()
        if len(res) == item[2]:
          let expect = createBigMessage(item[4], len(res))
          if expect == res:
            inc(counter)
      await response.closeWait()
      await req.closeWait()
    await session.closeWait()

    for item in ResponseTests:
      var session = createSession(secure)
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, item[1])
        else:
          getAddress(address, HttpClientScheme.NonSecure, item[1])
      var req = HttpClientRequestRef.new(session, ha, item[0])
      var response = await send(req)
      if response.status == 200:
        var reader = response.getBodyReader()
        var res: seq[byte]
        while true:
          var data = await reader.read(item[3])
          res.add(data)
          if len(data) != item[3]:
            break
        await reader.closeWait()
        if len(res) == item[2]:
          let expect = createBigMessage(item[4], len(res))
          if expect == res:
            inc(counter)
      await response.closeWait()
      await req.closeWait()
      await session.closeWait()

    await server.stop()
    await server.closeWait()
    return counter

  proc testRequestSizeStreamWritingTest(address: TransportAddress,
                                        secure: bool): Future[int] {.async.} =
    let RequestTests = [
      (MethodPost, "/test/big_request", 65600),
      (MethodPost, "/test/big_request", 262400)
    ]
    proc process(r: RequestFence): Future[HttpResponseRef] {.
         async.} =
      if r.isOk():
        let request = r.get()
        case request.uri.path
        of "/test/big_request":
          if request.hasBody():
            let body = await request.getBody()
            let digest = $secureHash(cast[string](body))
            return await request.respond(Http200, digest)
          else:
            return await request.respond(Http400, "Missing content body")
        else:
          return await request.respond(Http404, "Page not found")
      else:
        return dumbResponse()

    var server = createServer(address, process, secure)
    server.start()
    var counter = 0

    var session = createSession(secure)
    for item in RequestTests:
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, item[1])
        else:
          getAddress(address, HttpClientScheme.NonSecure, item[1])
      var data = createBigMessage("REQUESTSTREAMMESSAGE", item[2])
      let headers = [
        ("Content-Type", "application/octet-stream"),
        ("Content-Length", Base10.toString(uint64(len(data))))
      ]
      var request = HttpClientRequestRef.new(
        session, ha, item[0], headers = headers
      )

      var expectDigest = $secureHash(cast[string](data))
      # Sending big request by 1024bytes long chunks
      var writer = await open(request)
      var offset = 0
      while true:
        if len(data) == offset:
          break
        let toWrite = min(1024, len(data) - offset)
        await writer.write(addr data[offset], toWrite)
        offset = offset + toWrite
      await writer.finish()
      await writer.closeWait()
      var response = await request.finish()

      if response.status == 200:
        var res = await response.getBodyBytes()
        if cast[string](res) == expectDigest:
          inc(counter)
      await response.closeWait()
      await request.closeWait()
    await session.closeWait()

    await server.stop()
    await server.closeWait()
    return counter

  proc testRequestChunkedStreamWritingTest(address: TransportAddress,
                                          secure: bool): Future[int] {.async.} =
    let RequestTests = [
      (MethodPost, "/test/big_chunk_request", 65600),
      (MethodPost, "/test/big_chunk_request", 262400)
    ]
    proc process(r: RequestFence): Future[HttpResponseRef] {.
         async.} =
      if r.isOk():
        let request = r.get()
        case request.uri.path
        of "/test/big_chunk_request":
          if request.hasBody():
            let body = await request.getBody()
            let digest = $secureHash(cast[string](body))
            return await request.respond(Http200, digest)
          else:
            return await request.respond(Http400, "Missing content body")
        else:
          return await request.respond(Http404, "Page not found")
      else:
        return dumbResponse()

    var server = createServer(address, process, secure)
    server.start()
    var counter = 0

    var session = createSession(secure)
    for item in RequestTests:
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, item[1])
        else:
          getAddress(address, HttpClientScheme.NonSecure, item[1])
      var data = createBigMessage("REQUESTSTREAMMESSAGE", item[2])
      let headers = [
        ("Content-Type", "application/octet-stream"),
        ("Transfer-Encoding", "chunked")
      ]
      var request = HttpClientRequestRef.new(
        session, ha, item[0], headers = headers
      )

      var expectDigest = $secureHash(cast[string](data))
      # Sending big request by 1024bytes long chunks
      var writer = await open(request)
      var offset = 0
      while true:
        if len(data) == offset:
          break
        let toWrite = min(1024, len(data) - offset)
        await writer.write(addr data[offset], toWrite)
        offset = offset + toWrite
      await writer.finish()
      await writer.closeWait()
      var response = await request.finish()

      if response.status == 200:
        var res = await response.getBodyBytes()
        if cast[string](res) == expectDigest:
          inc(counter)
      await response.closeWait()
      await request.closeWait()
    await session.closeWait()

    await server.stop()
    await server.closeWait()
    return counter

  proc testRequestPostUrlEncodedTest(address: TransportAddress,
                                     secure: bool): Future[int] {.async.} =
    let PostRequests = [
      ("/test/post/urlencoded_size",
       "field1=value1&field2=value2&field3=value3", "value1:value2:value3"),
      ("/test/post/urlencoded_chunked",
       "field1=longlonglongvalue1&field2=longlonglongvalue2&" &
       "field3=longlonglongvalue3", "longlonglongvalue1:longlonglongvalue2:" &
       "longlonglongvalue3")
    ]

    proc process(r: RequestFence): Future[HttpResponseRef] {.
         async.} =
      if r.isOk():
        let request = r.get()
        case request.uri.path
        of "/test/post/urlencoded_size", "/test/post/urlencoded_chunked":
          if request.hasBody():
            var postTable = await request.post()
            let body = postTable.getString("field1") & ":" &
                       postTable.getString("field2") & ":" &
                       postTable.getString("field3")
            return await request.respond(Http200, body)
          else:
            return await request.respond(Http400, "Missing content body")
        else:
          return await request.respond(Http404, "Page not found")
      else:
        return dumbResponse()

    var server = createServer(address, process, secure)
    server.start()
    var counter = 0

    ## Sized url-encoded form
    block:
      var session = createSession(secure)
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, PostRequests[0][0])
        else:
          getAddress(address, HttpClientScheme.NonSecure, PostRequests[0][0])
      let headers = [
        ("Content-Type", "application/x-www-form-urlencoded"),
      ]
      var request = HttpClientRequestRef.new(
        session, ha, MethodPost, headers = headers,
        body = cast[seq[byte]](PostRequests[0][1]))
      var response = await send(request)

      if response.status == 200:
        var res = await response.getBodyBytes()
        if cast[string](res) == PostRequests[0][2]:
          inc(counter)
      await response.closeWait()
      await request.closeWait()
      await session.closeWait()

    ## Chunked url-encoded form
    block:
      var session = createSession(secure)
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, PostRequests[1][0])
        else:
          getAddress(address, HttpClientScheme.NonSecure, PostRequests[1][0])
      let headers = [
        ("Content-Type", "application/x-www-form-urlencoded"),
        ("Transfer-Encoding", "chunked")
      ]
      var request = HttpClientRequestRef.new(
        session, ha, MethodPost, headers = headers)

      var data = PostRequests[1][1]

      var writer = await open(request)
      var offset = 0
      while true:
        if len(data) == offset:
          break
        let toWrite = min(16, len(data) - offset)
        await writer.write(addr data[offset], toWrite)
        offset = offset + toWrite
      await writer.finish()
      await writer.closeWait()
      var response = await request.finish()
      if response.status == 200:
        var res = await response.getBodyBytes()
        if cast[string](res) == PostRequests[1][2]:
          inc(counter)
      await response.closeWait()
      await request.closeWait()
      await session.closeWait()

    await server.stop()
    await server.closeWait()
    return counter

  proc testRequestPostMultipartTest(address: TransportAddress,
                                    secure: bool): Future[int] {.async.} =
    let PostRequests = [
      ("/test/post/multipart_size", "some-part-boundary",
       [("field1", "value1"), ("field2", "value2"), ("field3", "value3")],
       "value1:value2:value3"),
      ("/test/post/multipart_chunked", "some-part-boundary",
       [("field1", "longlonglongvalue1"), ("field2", "longlonglongvalue2"),
        ("field3", "longlonglongvalue3")],
       "longlonglongvalue1:longlonglongvalue2:longlonglongvalue3")
    ]

    proc process(r: RequestFence): Future[HttpResponseRef] {.
         async.} =
      if r.isOk():
        let request = r.get()
        case request.uri.path
        of "/test/post/multipart_size", "/test/post/multipart_chunked":
          if request.hasBody():
            var postTable = await request.post()
            let body = postTable.getString("field1") & ":" &
                       postTable.getString("field2") & ":" &
                       postTable.getString("field3")
            return await request.respond(Http200, body)
          else:
            return await request.respond(Http400, "Missing content body")
        else:
          return await request.respond(Http404, "Page not found")
      else:
        return dumbResponse()

    var server = createServer(address, process, secure)
    server.start()
    var counter = 0

    ## Sized multipart form
    block:
      var mp = MultiPartWriter.init(PostRequests[0][1])
      mp.begin()
      for item in PostRequests[0][2]:
        mp.beginPart(item[0], "", HttpTable.init())
        mp.write(item[1])
        mp.finishPart()
      let data = mp.finish()

      var session = createSession(secure)
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, PostRequests[0][0])
        else:
          getAddress(address, HttpClientScheme.NonSecure, PostRequests[0][0])
      let headers = [
        ("Content-Type", "multipart/form-data; boundary=" & PostRequests[0][1]),
      ]
      var request = HttpClientRequestRef.new(
        session, ha, MethodPost, headers = headers, body = data)
      var response = await send(request)
      if response.status == 200:
        var res = await response.getBodyBytes()
        if cast[string](res) == PostRequests[0][3]:
          inc(counter)
      await response.closeWait()
      await request.closeWait()
      await session.closeWait()

    ## Chunked multipart form
    block:
      var session = createSession(secure)
      let ha =
        if secure:
          getAddress(address, HttpClientScheme.Secure, PostRequests[0][0])
        else:
          getAddress(address, HttpClientScheme.NonSecure, PostRequests[0][0])
      let headers = [
        ("Content-Type", "multipart/form-data; boundary=" & PostRequests[1][1]),
        ("Transfer-Encoding", "chunked")
      ]
      var request = HttpClientRequestRef.new(
        session, ha, MethodPost, headers = headers)
      var writer = await open(request)
      var mpw = MultiPartWriterRef.new(writer, PostRequests[1][1])
      await mpw.begin()
      for item in PostRequests[1][2]:
        await mpw.beginPart(item[0], "", HttpTable.init())
        await mpw.write(item[1])
        await mpw.finishPart()
      await mpw.finish()
      await writer.finish()
      await writer.closeWait()
      let response = await request.finish()
      if response.status == 200:
        var res = await response.getBodyBytes()
        if cast[string](res) == PostRequests[1][3]:
          inc(counter)
      await response.closeWait()
      await request.closeWait()
      await session.closeWait()

    await server.stop()
    await server.closeWait()
    return counter

  proc testRequestRedirectTest(address: TransportAddress,
                               secure: bool,
                               max: int): Future[string] {.async.} =
    var session = createSession(secure, maxRedirections = max)

    let ha =
      if secure:
        getAddress(address, HttpClientScheme.Secure, "/")
      else:
        getAddress(address, HttpClientScheme.NonSecure, "/")
    let lastAddress = ha.getUri().combine(parseUri("/final/5"))

    proc process(r: RequestFence): Future[HttpResponseRef] {.
         async.} =
      if r.isOk():
        let request = r.get()
        case request.uri.path
        of "/":
          return await request.redirect(Http302, "/redirect/1")
        of "/redirect/1":
          return await request.redirect(Http302, "/next/redirect/2")
        of "/next/redirect/2":
          return await request.redirect(Http302, "redirect/3")
        of "/next/redirect/redirect/3":
          return await request.redirect(Http302, "next/redirect/4")
        of "/next/redirect/redirect/next/redirect/4":
          return await request.redirect(Http302, lastAddress)
        of "/final/5":
          return await request.respond(Http200, "ok-5")
        else:
          return await request.respond(Http404, "Page not found")
      else:
        return dumbResponse()

    var server = createServer(address, process, secure)
    server.start()
    if session.maxRedirections >= 5:
      let (code, data) = await session.fetch(ha.getUri())
      await session.closeWait()
      await server.stop()
      await server.closeWait()
      return data.bytesToString() & "-" & $code
    else:
      let res =
        try:
          let (code {.used.}, data {.used.}) = await session.fetch(ha.getUri())
          false
        except HttpRedirectError:
          true
        except CatchableError:
          false
      await session.closeWait()
      await server.stop()
      await server.closeWait()
      return "redirect-" & $res

  proc testJigsawBasicAuthorization(): Future[bool] {.async.} =
    var session = createSession(true, maxRedirections = 10)
    let url = parseUri("https://guest:guest@jigsaw.w3.org/HTTP/Basic/")
    let resp = await session.fetch(url)
    await session.closeWait()
    if (resp.status == 200) and
       ("Your browser made it!" in cast[string](resp.data)):
      return true
    else:
      return false

  proc testJigsawRedirect301(): Future[bool] {.async.} =
    var session = createSession(true, maxRedirections = 10)
    let url = parseUri("https://jigsaw.w3.org/HTTP/300/301.html")
    let resp = await session.fetch(url)
    await session.closeWait()
    if (resp.status == 200) and
       ("A set of HTTP/1.1 redirect codes" in cast[string](resp.data)):
      return true
    else:
      return false

  test "HTTP all request methods test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testMethods(address, false)) == 18

  test "HTTP(S) all request methods test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testMethods(address, true)) == 18

  test "HTTP client response streaming test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testResponseStreamReadingTest(address, false)) == 8

  test "HTTP(S) client response streaming test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testResponseStreamReadingTest(address, true)) == 8

  test "HTTP client (size) request streaming test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestSizeStreamWritingTest(address, false)) == 2

  test "HTTP(S) client (size) request streaming test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestSizeStreamWritingTest(address, true)) == 2

  test "HTTP client (chunked) request streaming test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestChunkedStreamWritingTest(address, false)) == 2

  test "HTTP(S) client (chunked) request streaming test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestChunkedStreamWritingTest(address, true)) == 2

  test "HTTP client (size + chunked) url-encoded POST test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestPostUrlEncodedTest(address, false)) == 2

  test "HTTP(S) client (size + chunked) url-encoded POST test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestPostUrlEncodedTest(address, true)) == 2

  test "HTTP client (size + chunked) multipart POST test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestPostMultipartTest(address, false)) == 2

  test "HTTP(S) client (size + chunked) multipart POST test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestPostMultipartTest(address, true)) == 2

  test "HTTP client redirection test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestRedirectTest(address, false, 5)) == "ok-5-200"

  test "HTTP(S) client redirection test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestRedirectTest(address, true, 5)) == "ok-5-200"

  test "HTTP client maximum redirections test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestRedirectTest(address, false, 4)) == "redirect-true"

  test "HTTP(S) client maximum redirections test":
    let address = initTAddress("127.0.0.1:30080")
    check waitFor(testRequestRedirectTest(address, true, 4)) == "redirect-true"

  test "HTTPS jigsaw basic authorization test":
    check waitFor(testJigsawBasicAuthorization()) == true

  test "HTTPS jigsaw 301 redirection test":
    check waitFor(testJigsawRedirect301()) == true

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
      getTrackerLeaks("http.body.reader") == false
      getTrackerLeaks("http.body.writer") == false
      getTrackerLeaks("httpclient.connection") == false
      getTrackerLeaks("httpclient.request") == false
      getTrackerLeaks("httpclient.response") == false
      getTrackerLeaks("async.stream.reader") == false
      getTrackerLeaks("async.stream.writer") == false
      getTrackerLeaks("stream.server") == false
      getTrackerLeaks("stream.transport") == false
