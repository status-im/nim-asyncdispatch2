#
#            Chronos Transport Common Types
#             (c) Copyright 2018-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import os, strutils, nativesockets, net
import ../asyncloop, ../osapi
export net, osapi

when defined(windows):
  import winlean
else:
  import posix

const
  DefaultStreamBufferSize* = 4096    ## Default buffer size for stream
                                     ## transports
  DefaultDatagramBufferSize* = 65536 ## Default buffer size for datagram
                                     ## transports
type
  ServerFlags* = enum
    ## Server's flags
    ReuseAddr, ReusePort, TcpNoDelay, NoAutoRead, GCUserData, FirstPipe,
    NoPipeFlash, Broadcast

  AddressFamily* {.pure.} = enum
    None, IPv4, IPv6, Unix

  TransportAddress* = object
    ## Transport network address
    case family*: AddressFamily
    of AddressFamily.None:
      discard
    of AddressFamily.IPv4:
      address_v4*: array[4, uint8]
    of AddressFamily.IPv6:
      address_v6*: array[16, uint8]
    of AddressFamily.Unix:
      address_un*: array[108, uint8]
    port*: Port                   # Port number

  ServerCommand* = enum
    ## Server's commands
    Start,                        # Start server
    Pause,                        # Pause server
    Stop                          # Stop server

  ServerStatus* = enum
    ## Server's statuses
    Starting,                     # Server created
    Stopped,                      # Server stopped
    Running,                      # Server running
    Closed                        # Server closed

when defined(windows):
  type
    SocketServer* = ref object of RootRef
      ## Socket server object
      sock*: AsyncFD                # Socket
      local*: TransportAddress      # Address
      status*: ServerStatus         # Current server status
      udata*: pointer               # User-defined pointer
      flags*: set[ServerFlags]      # Flags
      bufferSize*: int              # Size of internal transports' buffer
      loopFuture*: Future[void]     # Server's main Future
      domain*: Domain               # Current server domain (IPv4 or IPv6)
      apending*: bool
      asock*: AsyncFD               # Current AcceptEx() socket
      abuffer*: array[128, byte]    # Windows AcceptEx() buffer
      aovl*: CustomOverlapped       # AcceptEx OVERLAPPED structure
else:
  type
    SocketServer* = ref object of RootRef
      ## Socket server object
      sock*: AsyncFD                # Socket
      local*: TransportAddress      # Address
      status*: ServerStatus         # Current server status
      udata*: pointer               # User-defined pointer
      flags*: set[ServerFlags]      # Flags
      bufferSize*: int              # Size of internal transports' buffer
      loopFuture*: Future[void]     # Server's main Future

type
  TransportError* = object of AsyncError
    ## Transport's specific exception
  TransportOsError* = object of TransportError
    ## Transport's OS specific exception
    code*: OSErrorCode
  TransportIncompleteError* = object of TransportError
    ## Transport's `incomplete data received` exception
  TransportLimitError* = object of TransportError
    ## Transport's `data limit reached` exception
  TransportAddressError* = object of TransportError
    ## Transport's address specific exception
    code*: OSErrorCode
  TransportNoSupport* = object of TransportError
    ## Transport's capability not supported exception

  TransportState* = enum
    ## Transport's state
    ReadPending,                  # Read operation pending (Windows)
    ReadPaused,                   # Read operations paused
    ReadClosed,                   # Read operations closed
    ReadEof,                      # Read at EOF
    ReadError,                    # Read error
    WritePending,                 # Writer operation pending (Windows)
    WritePaused,                  # Writer operations paused
    WriteClosed,                  # Writer operations closed
    WriteEof,                     # Remote peer disconnected
    WriteError                    # Write error

var
  AnyAddress* = TransportAddress(family: AddressFamily.IPv4, port: Port(0))
    ## Default INADDR_ANY address for IPv4
  AnyAddress6* = TransportAddress(family: AddressFamily.IPv6, port: Port(0))
    ## Default INADDR_ANY address for IPv6

proc `==`*(lhs, rhs: TransportAddress): bool =
  ## Compare two transport addresses ``lhs`` and ``rhs``. Return ``true`` if
  ## addresses are equal.
  if lhs.family != lhs.family:
    return false
  if lhs.family == AddressFamily.IPv4:
    result = equalMem(unsafeAddr lhs.address_v4[0],
                      unsafeAddr rhs.address_v4[0], sizeof(lhs.address_v4)) and
             (lhs.port == rhs.port)
  elif lhs.family == AddressFamily.IPv6:
    result = equalMem(unsafeAddr lhs.address_v6[0],
                      unsafeAddr rhs.address_v6[0], sizeof(lhs.address_v6)) and
             (lhs.port == rhs.port)
  elif lhs.family == AddressFamily.Unix:
    result = equalMem(unsafeAddr lhs.address_un[0],
                      unsafeAddr rhs.address_un[0], sizeof(lhs.address_un))

proc getDomain*(address: TransportAddress): Domain =
  ## Returns OS specific Domain from TransportAddress.
  case address.family
  of AddressFamily.IPv4:
    result = Domain.AF_INET
  of AddressFamily.IPv6:
    result = Domain.AF_INET6
  of AddressFamily.Unix:
    when defined(windows):
      result = cast[Domain](1)
    else:
      result = Domain.AF_UNIX
  else:
    result = cast[Domain](0)

proc `$`*(address: TransportAddress): string =
  ## Returns string representation of ``address``.
  case address.family
  of AddressFamily.IPv4:
    var a = IpAddress(
      family: IpAddressFamily.IPv4,
      address_v4: address.address_v4
    )
    result = $a
    result.add(":")
    result.add($int(address.port))
  of AddressFamily.IPv6:
    var a = IpAddress(family: IpAddressFamily.IPv6,
                      address_v6: address.address_v6)
    result = "[" & $a & "]:"
    result.add($(int(address.port)))
  of AddressFamily.Unix:
    const length = sizeof(address.address_un) + 1
    var buffer: array[length, char]
    if not equalMem(addr buffer[0], unsafeAddr address.address_un[0],
                    sizeof(address.address_un)):
      copyMem(addr buffer[0], unsafeAddr address.address_un[0],
              sizeof(address.address_un))
      result = $cast[cstring](addr buffer)
    else:
      result = ""
  else:
    raise newException(TransportAddressError, "Unknown address family!")

proc initTAddress*(address: string): TransportAddress =
  ## Parses string representation of ``address``. ``address`` can be IPv4, IPv6
  ## or Unix domain address.
  ##
  ## IPv4 transport address format is ``a.b.c.d:port``.
  ## IPv6 transport address format is ``[::]:port``.
  ## Unix transport address format is ``/address``.
  if len(address) > 0:
    if address[0] == '/':
      result = TransportAddress(family: AddressFamily.Unix, port: Port(1))
      let size = if len(address) < (sizeof(result.address_un) - 1): len(address)
                   else: (sizeof(result.address_un) - 1)
      copyMem(addr result.address_un[0], unsafeAddr address[0], size)
    else:
      var port: int
      var parts = address.rsplit(":", maxsplit = 1)
      if len(parts) != 2:
        raise newException(TransportAddressError,
                           "Format is <address>:<port> or </address>!")
      try:
        port = parseInt(parts[1])
      except:
        raise newException(TransportAddressError, "Illegal port number!")
      if port < 0 or port >= 65536:
        raise newException(TransportAddressError, "Illegal port number!")
      try:
        var ipaddr: IpAddress
        if parts[0][0] == '[' and parts[0][^1] == ']':
          ipaddr = parseIpAddress(parts[0][1..^2])
        else:
          ipaddr = parseIpAddress(parts[0])
        if ipaddr.family == IpAddressFamily.IPv4:
          result = TransportAddress(family: AddressFamily.IPv4)
          result.address_v4 = ipaddr.address_v4
        elif ipaddr.family == IpAddressFamily.IPv6:
          result = TransportAddress(family: AddressFamily.IPv6)
          result.address_v6 = ipaddr.address_v6
        else:
          raise newException(TransportAddressError, "Incorrect address family!")
        result.port = Port(port)
      except:
        raise newException(TransportAddressError, getCurrentException().msg)
  else:
    result = TransportAddress(family: AddressFamily.Unix)

proc initTAddress*(address: string, port: Port): TransportAddress =
  ## Initialize ``TransportAddress`` with IP (IPv4 or IPv6) address ``address``
  ## and port number ``port``.
  try:
    var ipaddr = parseIpAddress(address)
    if ipaddr.family == IpAddressFamily.IPv4:
      result = TransportAddress(family: AddressFamily.IPv4, port: port)
      result.address_v4 = ipaddr.address_v4
    elif ipaddr.family == IpAddressFamily.IPv6:
      result = TransportAddress(family: AddressFamily.IPv6, port: port)
      result.address_v6 = ipaddr.address_v6
    else:
      raise newException(TransportAddressError, "Incorrect address family!")
  except:
    raise newException(TransportAddressError, getCurrentException().msg)

proc initTAddress*(address: string, port: int): TransportAddress {.inline.} =
  ## Initialize ``TransportAddress`` with IP (IPv4 or IPv6) address ``address``
  ## and port number ``port``.
  if port < 0 or port >= 65536:
    raise newException(TransportAddressError, "Illegal port number!")
  else:
    result = initTAddress(address, Port(port))

proc initTAddress*(address: IpAddress, port: Port): TransportAddress =
  ## Initialize ``TransportAddress`` with net.nim ``IpAddress`` and
  ## port number ``port``.
  if address.family == IpAddressFamily.IPv4:
    result = TransportAddress(family: AddressFamily.IPv4, port: port)
    result.address_v4 = address.address_v4
  elif address.family == IpAddressFamily.IPv6:
    result = TransportAddress(family: AddressFamily.IPv6, port: port)
    result.address_v6 = address.address_v6
  else:
    raise newException(TransportAddressError, "Incorrect address family!")

proc getAddrInfo(address: string, port: Port, domain: Domain,
                 sockType: SockType = SockType.SOCK_STREAM,
                 protocol: Protocol = Protocol.IPPROTO_TCP): ptr AddrInfo =
  ## We have this one copy of ``getAddrInfo()`` because of AI_V4MAPPED in
  ## ``net.nim:getAddrInfo()``, which is not cross-platform.
  var hints: AddrInfo
  result = nil
  hints.ai_family = toInt(domain)
  hints.ai_socktype = toInt(sockType)
  hints.ai_protocol = toInt(protocol)
  var gaiResult = getaddrinfo(address, $port, addr(hints), result)
  if gaiResult != 0'i32:
    when defined(windows):
      raise newException(TransportAddressError, osErrorMsg(osLastError()))
    else:
      raise newException(TransportAddressError, $gai_strerror(gaiResult))

proc fromSAddr*(sa: ptr Sockaddr_storage, sl: Socklen,
                address: var TransportAddress) =
  ## Set transport address ``address`` with value from OS specific socket
  ## address storage.
  if int(sa.ss_family) == toInt(Domain.AF_INET) and
     int(sl) == sizeof(Sockaddr_in):
    address = TransportAddress(family: AddressFamily.IPv4)
    let s = cast[ptr Sockaddr_in](sa)
    copyMem(addr address.address_v4[0], addr s.sin_addr,
            sizeof(address.address_v4))
    address.port = Port(nativesockets.ntohs(s.sin_port))
  elif int(sa.ss_family) == toInt(Domain.AF_INET6) and
       int(sl) == sizeof(Sockaddr_in6):
    address = TransportAddress(family: AddressFamily.IPv6)
    let s = cast[ptr Sockaddr_in6](sa)
    copyMem(addr address.address_v6[0], addr s.sin6_addr,
            sizeof(address.address_v6))
    address.port = Port(nativesockets.ntohs(s.sin6_port))
  elif int(sa.ss_family) == toInt(Domain.AF_UNIX):
    when not defined(windows):
      address = TransportAddress(family: AddressFamily.Unix)
      if int(sl) > sizeof(sa.ss_family):
        var length = int(sl) - sizeof(sa.ss_family)
        if length > (sizeof(address.address_un) - 1):
          length = sizeof(address.address_un) - 1
        let s = cast[ptr Sockaddr_un](sa)
        copyMem(addr address.address_un[0], addr s.sun_path[0], length)
        address.port = Port(1)
    else:
      discard

proc toSAddr*(address: TransportAddress, sa: var Sockaddr_storage,
             sl: var Socklen) =
  ## Set socket OS specific socket address storage with address from transport
  ## address ``address``.
  case address.family
  of AddressFamily.IPv4:
    sl = Socklen(sizeof(Sockaddr_in))
    let s = cast[ptr Sockaddr_in](addr sa)
    s.sin_family = type(s.sin_family)(toInt(Domain.AF_INET))
    s.sin_port = nativesockets.htons(uint16(address.port))
    copyMem(addr s.sin_addr, unsafeAddr address.address_v4[0],
            sizeof(s.sin_addr))
  of AddressFamily.IPv6:
    sl = Socklen(sizeof(Sockaddr_in6))
    let s = cast[ptr Sockaddr_in6](addr sa)
    s.sin6_family = type(s.sin6_family)(toInt(Domain.AF_INET6))
    s.sin6_port = nativesockets.htons(uint16(address.port))
    copyMem(addr s.sin6_addr, unsafeAddr address.address_v6[0],
            sizeof(s.sin6_addr))
  of AddressFamily.Unix:
    when not defined(windows):
      if address.port == Port(0):
        sl = Socklen(sizeof(sa.ss_family))
      else:
        let s = cast[ptr Sockaddr_un](addr sa)
        var name = cast[cstring](unsafeAddr address.address_un[0])
        sl = Socklen(sizeof(sa.ss_family) + len(name) + 1)
        s.sun_family = type(s.sun_family)(toInt(Domain.AF_UNIX))
        copyMem(addr s.sun_path, unsafeAddr address.address_un[0],
                len(name) + 1)
  else:
    discard

proc address*(ta: TransportAddress): IpAddress =
  ## Converts ``TransportAddress`` to ``net.IpAddress`` object.
  ##
  ## Note its impossible to convert ``TransportAddress`` of ``Unix`` family,
  ## because ``IpAddress`` supports only IPv4, IPv6 addresses.
  if ta.family == AddressFamily.IPv4:
    result = IpAddress(family: IpAddressFamily.IPv4)
    result.address_v4 = ta.address_v4
  elif ta.family == AddressFamily.IPv6:
    result = IpAddress(family: IpAddressFamily.IPv6)
    result.address_v6 = ta.address_v6
  else:
    raise newException(ValueError, "IpAddress supports only IPv4/IPv6!")

proc resolveTAddress*(address: string,
                      family = AddressFamily.IPv4): seq[TransportAddress] =
  ## Resolve string representation of ``address``.
  ##
  ## Supported formats are:
  ## IPv4 numeric address ``a.b.c.d:port``
  ## IPv6 numeric address ``[::]:port``
  ## Hostname address ``hostname:port``
  ##
  ## If hostname address is detected, then network address translation via DNS
  ## will be performed.
  var
    hostname: string
    port: int

  doAssert(family in {AddressFamily.IPv4, AddressFamily.IPv6})

  result = newSeq[TransportAddress]()
  var parts = address.rsplit(":", maxsplit = 1)
  if len(parts) != 2:
    raise newException(TransportAddressError, "Format is <address>:<port>!")

  try:
    port = parseInt(parts[1])
  except:
    raise newException(TransportAddressError, "Illegal port number!")

  if port < 0 or port >= 65536:
    raise newException(TransportAddressError, "Illegal port number!")

  if parts[0][0] == '[' and parts[0][^1] == ']':
    # IPv6 numeric addresses must be enclosed with `[]`.
    hostname = parts[0][1..^2]
  else:
    hostname = parts[0]

  var domain = if family == AddressFamily.IPv4: Domain.AF_INET else:
                 Domain.AF_INET6
  var aiList = getAddrInfo(hostname, Port(port), domain)
  var it = aiList
  while it != nil:
    var ta: TransportAddress
    fromSAddr(cast[ptr Sockaddr_storage](it.ai_addr),
              SockLen(it.ai_addrlen), ta)
    # For some reason getAddrInfo() sometimes returns duplicate addresses,
    # for example getAddrInfo(`localhost`) returns `127.0.0.1` twice.
    if ta notin result:
      result.add(ta)
    it = it.ai_next
  freeAddrInfo(aiList)

proc resolveTAddress*(address: string, port: Port,
                      family = AddressFamily.IPv4): seq[TransportAddress] =
  ## Resolve string representation of ``address``.
  ##
  ## ``address`` could be dot IPv4/IPv6 address or hostname.
  ##
  ## If hostname address is detected, then network address translation via DNS
  ## will be performed.
  doAssert(family in {AddressFamily.IPv4, AddressFamily.IPv6})

  result = newSeq[TransportAddress]()
  var domain = if family == AddressFamily.IPv4: Domain.AF_INET else:
                 Domain.AF_INET6
  var aiList = getAddrInfo(address, port, domain)
  var it = aiList
  while it != nil:
    var ta: TransportAddress
    fromSAddr(cast[ptr Sockaddr_storage](it.ai_addr),
              SockLen(it.ai_addrlen), ta)
    # For some reason getAddrInfo() sometimes returns duplicate addresses,
    # for example getAddrInfo(`localhost`) returns `127.0.0.1` twice.
    if ta notin result:
      result.add(ta)
    it = it.ai_next
  freeAddrInfo(aiList)

proc resolveTAddress*(address: string,
                      family: IpAddressFamily): seq[TransportAddress] {.
     deprecated.} =
  if family == IpAddressFamily.IPv4:
    result = resolveTAddress(address, AddressFamily.IPv4)
  elif family == IpAddressFamily.IPv6:
    result = resolveTAddress(address, AddressFamily.IPv6)

proc resolveTAddress*(address: string, port: Port,
                      family: IpAddressFamily): seq[TransportAddress] {.
     deprecated.} =
  if family == IpAddressFamily.IPv4:
    result = resolveTAddress(address, port, AddressFamily.IPv4)
  elif family == IpAddressFamily.IPv6:
    result = resolveTAddress(address, port, AddressFamily.IPv6)

template checkClosed*(t: untyped) =
  if (ReadClosed in (t).state) or (WriteClosed in (t).state):
    raise newException(TransportError, "Transport is already closed!")

template checkClosed*(t: untyped, future: untyped) =
  if (ReadClosed in (t).state) or (WriteClosed in (t).state):
    future.fail(newException(TransportError, "Transport is already closed!"))
    return future

template getError*(t: untyped): ref Exception =
  var err = (t).error
  (t).error = nil
  err

template getTransportOsError*(err: OSErrorCode): ref TransportOsError =
  var msg = "(" & $int(err) & ") " & osErrorMsg(err)
  var tre = newException(TransportOsError, msg)
  tre.code = err
  tre

template getTransportOsError*(err: cint): ref TransportOsError =
  getTransportOsError(OSErrorCode(err))

proc raiseTransportOsError*(err: OSErrorCode) =
  ## Raises transport specific OS error.
  raise getTransportOsError(err)

type
  SeqHeader = object
    length, reserved: int

proc isLiteral*(s: string): bool {.inline.} =
  (cast[ptr SeqHeader](s).reserved and (1 shl (sizeof(int) * 8 - 2))) != 0

proc isLiteral*[T](s: seq[T]): bool {.inline.} =
  (cast[ptr SeqHeader](s).reserved and (1 shl (sizeof(int) * 8 - 2))) != 0
