#
#                     Chronos
#
#           (c) Copyright 2015 Dominik Picheta
#  (c) Copyright 2018-Present Status Research & Development GmbH
#
#                Licensed under either of
#    Apache License, version 2.0, (LICENSE-APACHEv2)
#                MIT license (LICENSE-MIT)

include "system/inclrtl"

import os, tables, strutils, heapqueue, lists, options
import timer
import asyncfutures2 except callSoon

import nativesockets, net, deques

export Port, SocketFlag
export asyncfutures2, timer

#{.injectStmt: newGcInvariant().}

## AsyncDispatch
## *************
##
## This module implements asynchronous IO. This includes a dispatcher,
## a ``Future`` type implementation, and an ``async`` macro which allows
## asynchronous code to be written in a synchronous style with the ``await``
## keyword.
##
## The dispatcher acts as a kind of event loop. You must call ``poll`` on it
## (or a function which does so for you such as ``waitFor`` or ``runForever``)
## in order to poll for any outstanding events. The underlying implementation
## is based on epoll on Linux, IO Completion Ports on Windows and select on
## other operating systems.
##
## The ``poll`` function will not, on its own, return any events. Instead
## an appropriate ``Future`` object will be completed. A ``Future`` is a
## type which holds a value which is not yet available, but which *may* be
## available in the future. You can check whether a future is finished
## by using the ``finished`` function. When a future is finished it means that
## either the value that it holds is now available or it holds an error instead.
## The latter situation occurs when the operation to complete a future fails
## with an exception. You can distinguish between the two situations with the
## ``failed`` function.
##
## Future objects can also store a callback procedure which will be called
## automatically once the future completes.
##
## Futures therefore can be thought of as an implementation of the proactor
## pattern. In this
## pattern you make a request for an action, and once that action is fulfilled
## a future is completed with the result of that action. Requests can be
## made by calling the appropriate functions. For example: calling the ``recv``
## function will create a request for some data to be read from a socket. The
## future which the ``recv`` function returns will then complete once the
## requested amount of data is read **or** an exception occurs.
##
## Code to read some data from a socket may look something like this:
##
##   .. code-block::nim
##      var future = socket.recv(100)
##      future.addCallback(
##        proc () =
##          echo(future.read)
##      )
##
## All asynchronous functions returning a ``Future`` will not block. They
## will not however return immediately. An asynchronous function will have
## code which will be executed before an asynchronous request is made, in most
## cases this code sets up the request.
##
## In the above example, the ``recv`` function will return a brand new
## ``Future`` instance once the request for data to be read from the socket
## is made. This ``Future`` instance will complete once the requested amount
## of data is read, in this case it is 100 bytes. The second line sets a
## callback on this future which will be called once the future completes.
## All the callback does is write the data stored in the future to ``stdout``.
## The ``read`` function is used for this and it checks whether the future
## completes with an error for you (if it did it will simply raise the
## error), if there is no error however it returns the value of the future.
##
## Asynchronous procedures
## -----------------------
##
## Asynchronous procedures remove the pain of working with callbacks. They do
## this by allowing you to write asynchronous code the same way as you would
## write synchronous code.
##
## An asynchronous procedure is marked using the ``{.async.}`` pragma.
## When marking a procedure with the ``{.async.}`` pragma it must have a
## ``Future[T]`` return type or no return type at all. If you do not specify
## a return type then ``Future[void]`` is assumed.
##
## Inside asynchronous procedures ``await`` can be used to call any
## procedures which return a
## ``Future``; this includes asynchronous procedures. When a procedure is
## "awaited", the asynchronous procedure it is awaited in will
## suspend its execution
## until the awaited procedure's Future completes. At which point the
## asynchronous procedure will resume its execution. During the period
## when an asynchronous procedure is suspended other asynchronous procedures
## will be run by the dispatcher.
##
## The ``await`` call may be used in many contexts. It can be used on the right
## hand side of a variable declaration: ``var data = await socket.recv(100)``,
## in which case the variable will be set to the value of the future
## automatically. It can be used to await a ``Future`` object, and it can
## be used to await a procedure returning a ``Future[void]``:
## ``await socket.send("foobar")``.
##
## If an awaited future completes with an error, then ``await`` will re-raise
## this error. To avoid this, you can use the ``yield`` keyword instead of
## ``await``. The following section shows different ways that you can handle
## exceptions in async procs.
##
## Handling Exceptions
## ~~~~~~~~~~~~~~~~~~~
##
## The most reliable way to handle exceptions is to use ``yield`` on a future
## then check the future's ``failed`` property. For example:
##
##   .. code-block:: Nim
##     var future = sock.recv(100)
##     yield future
##     if future.failed:
##       # Handle exception
##
## The ``async`` procedures also offer limited support for the try statement.
##
##    .. code-block:: Nim
##      try:
##        let data = await sock.recv(100)
##        echo("Received ", data)
##      except:
##        # Handle exception
##
## Unfortunately the semantics of the try statement may not always be correct,
## and occasionally the compilation may fail altogether.
## As such it is better to use the former style when possible.
##
##
## Discarding futures
## ------------------
##
## Futures should **never** be discarded. This is because they may contain
## errors. If you do not care for the result of a Future then you should
## use the ``asyncCheck`` procedure instead of the ``discard`` keyword.
##
## Examples
## --------
##
## For examples take a look at the documentation for the modules implementing
## asynchronous IO. A good place to start is the
## `asyncnet module <asyncnet.html>`_.
##
## Limitations/Bugs
## ----------------
##
## * The effect system (``raises: []``) does not work with async procedures.
## * Can't await in a ``except`` body
## * Forward declarations for async procs are broken,
##   link includes workaround: https://github.com/nim-lang/Nim/issues/3182.

# TODO: Check if yielded future is nil and throw a more meaningful exception

type
  AsyncError* = object of Exception
    ## Generic async exception
  AsyncTimeoutError* = object of AsyncError
    ## Timeout exception

  TimerCallback* = object
    finishAt*: Moment
    function*: AsyncCallback

  PDispatcherBase = ref object of RootRef
    timers*: HeapQueue[TimerCallback]
    callbacks*: Deque[AsyncCallback]

proc `<`(a, b: TimerCallback): bool =
  result = a.finishAt < b.finishAt

proc callSoon(cbproc: CallbackFunc, data: pointer = nil) {.gcsafe.}

proc initCallSoonProc() =
  if asyncfutures2.getCallSoonProc().isNil:
    asyncfutures2.setCallSoonProc(callSoon)

template processTimersGetTimeout(loop, timeout: untyped) =
  var count = len(loop.timers)
  if count > 0:
    var lastFinish = curTime
    while count > 0:
      lastFinish = loop.timers[0].finishAt
      if curTime < lastFinish:
        break
      loop.callbacks.addLast(loop.timers.pop().function)
      dec(count)
    if count > 0:
      timeout = (lastFinish - curTime).getAsyncTimestamp()

  if timeout == 0:
    if len(loop.callbacks) == 0:
      when defined(windows):
        timeout = INFINITE
      else:
        timeout = -1
  else:
    if len(loop.callbacks) != 0:
      timeout = 0

template processTimers(loop: untyped) =
  var curTime = Moment.now()
  var count = len(loop.timers)
  if count > 0:
    while count > 0:
      if curTime < loop.timers[0].finishAt:
        break
      loop.callbacks.addLast(loop.timers.pop().function)
      dec(count)

template processCallbacks(loop: untyped) =
  var count = len(loop.callbacks)
  for i in 0..<count:
    # This is mostly workaround for people which are using `waitFor` where
    # it must be used `await`. While using `waitFor` inside of callbacks
    # dispatcher's callback list is got decreased and length of
    # `loop.callbacks` become not equal to `count`, its why `IndexError`
    # can be generated.
    if len(loop.callbacks) == 0: break
    let callable = loop.callbacks.popFirst()
    if not isNil(callable.function):
      callable.function(callable.udata)

when defined(windows) or defined(nimdoc):
  import winlean, sets, hashes
  type
    WSAPROC_TRANSMITFILE = proc(hSocket: SocketHandle, hFile: Handle,
                                nNumberOfBytesToWrite: DWORD,
                                nNumberOfBytesPerSend: DWORD,
                                lpOverlapped: POVERLAPPED,
                                lpTransmitBuffers: pointer,
                                dwReserved: DWORD): cint {.
                                stdcall.}

    CompletionKey = ULONG_PTR

    CompletionData* = object
      fd*: AsyncFD
      cb*: CallbackFunc
      errCode*: OSErrorCode
      bytesCount*: int32
      udata*: pointer

    CustomOverlapped* = object of OVERLAPPED
      data*: CompletionData

    PDispatcher* = ref object of PDispatcherBase
      ioPort: Handle
      handles: HashSet[AsyncFD]
      connectEx*: WSAPROC_CONNECTEX
      acceptEx*: WSAPROC_ACCEPTEX
      getAcceptExSockAddrs*: WSAPROC_GETACCEPTEXSOCKADDRS
      transmitFile*: WSAPROC_TRANSMITFILE

    PtrCustomOverlapped* = ptr CustomOverlapped

    RefCustomOverlapped* = ref CustomOverlapped

    AsyncFD* = distinct int

  proc hash(x: AsyncFD): Hash {.borrow.}
  proc `==`*(x: AsyncFD, y: AsyncFD): bool {.borrow.}

  proc newDispatcher*(): PDispatcher =
    ## Creates a new Dispatcher instance.
    new result
    result.ioPort = createIoCompletionPort(INVALID_HANDLE_VALUE, 0, 0, 1)
    result.handles = initSet[AsyncFD]()
    result.timers.newHeapQueue()
    result.callbacks = initDeque[AsyncCallback](64)

  var gDisp{.threadvar.}: PDispatcher ## Global dispatcher

  proc setGlobalDispatcher*(disp: PDispatcher) =
    ## Set current thread's dispatcher instance to ``disp``.
    if not gDisp.isNil:
      doAssert gDisp.callbacks.len == 0
    gDisp = disp
    initCallSoonProc()

  proc getGlobalDispatcher*(): PDispatcher =
    ## Returns current thread's dispatcher instance.
    if gDisp.isNil:
      setGlobalDispatcher(newDispatcher())
    result = gDisp

  proc getIoHandler*(disp: PDispatcher): Handle =
    ## Returns the underlying IO Completion Port handle (Windows) or selector
    ## (Unix) for the specified dispatcher.
    return disp.ioPort

  proc register*(fd: AsyncFD) =
    ## Register file descriptor ``fd`` in thread's dispatcher.
    let loop = getGlobalDispatcher()
    if createIoCompletionPort(fd.Handle, loop.ioPort,
                              cast[CompletionKey](fd), 1) == 0:
      raiseOSError(osLastError())
    loop.handles.incl(fd)

  proc poll*() =
    ## Perform single asynchronous step.
    let loop = getGlobalDispatcher()
    var curTime = Moment.now()
    var curTimeout = DWORD(0)

    # Moving expired timers to `loop.callbacks` and calculate timeout
    loop.processTimersGetTimeout(curTimeout)

    # Processing handles
    var lpNumberOfBytesTransferred: Dword
    var lpCompletionKey: ULONG_PTR
    var customOverlapped: PtrCustomOverlapped

    let res = getQueuedCompletionStatus(
      loop.ioPort, addr lpNumberOfBytesTransferred,
      addr lpCompletionKey, cast[ptr POVERLAPPED](addr customOverlapped),
      curTimeout).bool

    if res:
      customOverlapped.data.bytesCount = lpNumberOfBytesTransferred
      customOverlapped.data.errCode = OSErrorCode(-1)
      let acb = AsyncCallback(function: customOverlapped.data.cb,
                              udata: cast[pointer](customOverlapped))
      loop.callbacks.addLast(acb)
    else:
      let errCode = osLastError()
      if customOverlapped != nil:
        customOverlapped.data.errCode = errCode
        let acb = AsyncCallback(function: customOverlapped.data.cb,
                                udata: cast[pointer](customOverlapped))
        loop.callbacks.addLast(acb)
      else:
        if int32(errCode) != WAIT_TIMEOUT:
          raiseOSError(errCode)

    # Moving expired timers to `loop.callbacks`.
    loop.processTimers()

    # All callbacks which will be added in process will be processed on next
    # poll() call.
    loop.processCallbacks()

  proc getFunc(s: SocketHandle, fun: var pointer, guid: var GUID): bool =
    var bytesRet: DWORD
    fun = nil
    result = WSAIoctl(s, SIO_GET_EXTENSION_FUNCTION_POINTER, addr guid,
                      sizeof(GUID).DWORD, addr fun, sizeof(pointer).DWORD,
                      addr bytesRet, nil, nil) == 0

  proc initAPI() =
    var
      WSAID_TRANSMITFILE = GUID(
        D1: 0xb5367df0'i32, D2: 0xcbac'i16, D3: 0x11cf'i16,
        D4: [0x95'i8, 0xca'i8, 0x00'i8, 0x80'i8,
             0x5f'i8, 0x48'i8, 0xa1'i8, 0x92'i8])

    let loop = getGlobalDispatcher()

    var wsa: WSAData
    if wsaStartup(0x0202'i16, addr wsa) != 0:
      raiseOSError(osLastError())

    let sock = winlean.socket(winlean.AF_INET, 1, 6)
    if sock == INVALID_SOCKET:
      raiseOSError(osLastError())

    var funcPointer: pointer = nil
    if not getFunc(sock, funcPointer, WSAID_CONNECTEX):
      let err = osLastError()
      close(sock)
      raiseOSError(err)
    loop.connectEx = cast[WSAPROC_CONNECTEX](funcPointer)
    if not getFunc(sock, funcPointer, WSAID_ACCEPTEX):
      let err = osLastError()
      close(sock)
      raiseOSError(err)
    loop.acceptEx = cast[WSAPROC_ACCEPTEX](funcPointer)
    if not getFunc(sock, funcPointer, WSAID_GETACCEPTEXSOCKADDRS):
      let err = osLastError()
      close(sock)
      raiseOSError(err)
    loop.getAcceptExSockAddrs = cast[WSAPROC_GETACCEPTEXSOCKADDRS](funcPointer)
    if not getFunc(sock, funcPointer, WSAID_TRANSMITFILE):
      let err = osLastError()
      close(sock)
      raiseOSError(err)
    loop.transmitFile = cast[WSAPROC_TRANSMITFILE](funcPointer)
    close(sock)

  proc closeSocket*(fd: AsyncFD, aftercb: CallbackFunc = nil) =
    ## Closes a socket and ensures that it is unregistered.
    let loop = getGlobalDispatcher()
    loop.handles.excl(fd)
    close(SocketHandle(fd))
    if not isNil(aftercb):
      var acb = AsyncCallback(function: aftercb)
      loop.callbacks.addLast(acb)

  proc closeHandle*(fd: AsyncFD, aftercb: CallbackFunc = nil) =
    ## Closes a (pipe/file) handle and ensures that it is unregistered.
    let loop = getGlobalDispatcher()
    loop.handles.excl(fd)
    doAssert closeHandle(Handle(fd)) == 1
    if not isNil(aftercb):
      var acb = AsyncCallback(function: aftercb)
      loop.callbacks.addLast(acb)

  proc unregister*(fd: AsyncFD) =
    ## Unregisters ``fd``.
    getGlobalDispatcher().handles.excl(fd)

  proc contains*(disp: PDispatcher, fd: AsyncFD): bool =
    ## Returns ``true`` if ``fd`` is registered in thread's dispatcher.
    return fd in disp.handles

else:
  import selectors
  from posix import EINTR, EAGAIN, EINPROGRESS, EWOULDBLOCK, MSG_PEEK,
                    MSG_NOSIGNAL
  type
    AsyncFD* = distinct cint

    CompletionData* = object
      fd*: AsyncFD
      udata*: pointer

    PCompletionData* = ptr CompletionData

    SelectorData* = object
      reader*: AsyncCallback
      rdata*: CompletionData
      writer*: AsyncCallback
      wdata*: CompletionData

    PDispatcher* = ref object of PDispatcherBase
      selector: Selector[SelectorData]
      keys: seq[ReadyKey]

  proc `==`*(x, y: AsyncFD): bool {.borrow.}

  proc newDispatcher*(): PDispatcher =
    ## Create new dispatcher.
    new result
    result.selector = newSelector[SelectorData]()
    result.timers.newHeapQueue()
    result.callbacks = initDeque[AsyncCallback](64)
    result.keys = newSeq[ReadyKey](64)

  var gDisp{.threadvar.}: PDispatcher ## Global dispatcher

  proc setGlobalDispatcher*(disp: PDispatcher) =
    ## Set current thread's dispatcher instance to ``disp``.
    if not gDisp.isNil:
      doAssert gDisp.callbacks.len == 0
    gDisp = disp
    initCallSoonProc()

  proc getGlobalDispatcher*(): PDispatcher =
    ## Returns current thread's dispatcher instance.
    if gDisp.isNil:
      setGlobalDispatcher(newDispatcher())
    result = gDisp

  proc getIoHandler*(disp: PDispatcher): Selector[SelectorData] =
    ## Returns system specific OS queue.
    return disp.selector

  proc register*(fd: AsyncFD) =
    ## Register file descriptor ``fd`` in thread's dispatcher.
    let loop = getGlobalDispatcher()
    var data: SelectorData
    data.rdata.fd = fd
    data.wdata.fd = fd
    loop.selector.registerHandle(int(fd), {}, data)

  proc unregister*(fd: AsyncFD) =
    ## Unregister file descriptor ``fd`` from thread's dispatcher.
    getGlobalDispatcher().selector.unregister(int(fd))

  proc contains*(disp: PDispatcher, fd: AsyncFd): bool {.inline.} =
    ## Returns ``true`` if ``fd`` is registered in thread's dispatcher.
    result = int(fd) in disp.selector

  proc addReader*(fd: AsyncFD, cb: CallbackFunc, udata: pointer = nil) =
    ## Start watching the file descriptor ``fd`` for read availability and then
    ## call the callback ``cb`` with specified argument ``udata``.
    let loop = getGlobalDispatcher()
    var newEvents = {Event.Read}
    withData(loop.selector, int(fd), adata) do:
      let acb = AsyncCallback(function: cb, udata: addr adata.rdata)
      adata.reader = acb
      adata.rdata = CompletionData(fd: fd, udata: udata)
      newEvents.incl(Event.Read)
      if not isNil(adata.writer.function): newEvents.incl(Event.Write)
    do:
      raise newException(ValueError, "File descriptor not registered.")
    loop.selector.updateHandle(int(fd), newEvents)

  proc removeReader*(fd: AsyncFD) =
    ## Stop watching the file descriptor ``fd`` for read availability.
    let loop = getGlobalDispatcher()
    var newEvents: set[Event]
    withData(loop.selector, int(fd), adata) do:
      # We need to clear `reader` data, because `selectors` don't do it
      adata.reader.function = nil
      # adata.rdata = CompletionData()
      if not isNil(adata.writer.function): newEvents.incl(Event.Write)
    do:
      raise newException(ValueError, "File descriptor not registered.")
    loop.selector.updateHandle(int(fd), newEvents)

  proc addWriter*(fd: AsyncFD, cb: CallbackFunc, udata: pointer = nil) =
    ## Start watching the file descriptor ``fd`` for write availability and then
    ## call the callback ``cb`` with specified argument ``udata``.
    let loop = getGlobalDispatcher()
    var newEvents = {Event.Write}
    withData(loop.selector, int(fd), adata) do:
      let acb = AsyncCallback(function: cb, udata: addr adata.wdata)
      adata.writer = acb
      adata.wdata = CompletionData(fd: fd, udata: udata)
      newEvents.incl(Event.Write)
      if not isNil(adata.reader.function): newEvents.incl(Event.Read)
    do:
      raise newException(ValueError, "File descriptor not registered.")
    loop.selector.updateHandle(int(fd), newEvents)

  proc removeWriter*(fd: AsyncFD) =
    ## Stop watching the file descriptor ``fd`` for write availability.
    let loop = getGlobalDispatcher()
    var newEvents: set[Event]
    withData(loop.selector, int(fd), adata) do:
      # We need to clear `writer` data, because `selectors` don't do it
      adata.writer.function = nil
      # adata.wdata = CompletionData()
      if not isNil(adata.reader.function): newEvents.incl(Event.Read)
    do:
      raise newException(ValueError, "File descriptor not registered.")
    loop.selector.updateHandle(int(fd), newEvents)

  proc closeSocket*(fd: AsyncFD, aftercb: CallbackFunc = nil) =
    ## Close asynchronous socket.
    ##
    ## Please note, that socket is not closed immediately. To avoid bugs with
    ## closing socket, while operation pending, socket will be closed as
    ## soon as all pending operations will be notified.
    ## You can execute ``aftercb`` before actual socket close operation.
    let loop = getGlobalDispatcher()

    proc continuation(udata: pointer) =
      unregister(fd)
      close(SocketHandle(fd))
      if not isNil(aftercb):
        aftercb(nil)

    withData(loop.selector, int(fd), adata) do:
      # We are scheduling reader and writer callbacks to be called
      # explicitly, so they can get an error and continue work.
      if not isNil(adata.reader.function):
        if not adata.reader.deleted:
          loop.callbacks.addLast(adata.reader)
      if not isNil(adata.writer.function):
        if not adata.writer.deleted:
          loop.callbacks.addLast(adata.writer)
      # Mark callbacks as deleted, we don't need to get REAL notifications
      # from system queue for this reader and writer.
      adata.reader.deleted = true
      adata.writer.deleted = true

    # We can't unregister file descriptor from system queue here, because
    # in such case processing queue will stuck on poll() call, because there
    # can be no file descriptors registered in system queue.
    var acb = AsyncCallback(function: continuation)
    loop.callbacks.addLast(acb)

  when ioselSupportedPlatform:
    proc addSignal*(signal: int, cb: CallbackFunc,
                    udata: pointer = nil): int =
      ## Start watching signal ``signal``, and when signal appears, call the
      ## callback ``cb`` with specified argument ``udata``. Returns signal
      ## identifier code, which can be used to remove signal callback
      ## via ``removeSignal``.
      let loop = getGlobalDispatcher()
      var data: SelectorData
      result = loop.selector.registerSignal(signal, data)
      withData(loop.selector, result, adata) do:
        adata.reader = AsyncCallback(function: cb, udata: addr adata.rdata)
        adata.rdata.fd = AsyncFD(result)
        adata.rdata.udata = udata
      do:
        raise newException(ValueError, "File descriptor not registered.")

    proc removeSignal*(sigfd: int) =
      ## Remove watching signal ``signal``.
      let loop = getGlobalDispatcher()
      loop.selector.unregister(sigfd)

  proc poll*() =
    ## Perform single asynchronous step.
    let loop = getGlobalDispatcher()
    var curTime = Moment.now()
    var curTimeout = 0

    when ioselSupportedPlatform:
      let customSet = {Event.Timer, Event.Signal, Event.Process,
                       Event.Vnode}

    # Moving expired timers to `loop.callbacks` and calculate timeout.
    loop.processTimersGetTimeout(curTimeout)

    # Processing IO descriptors and all hardware events.
    var count = loop.selector.selectInto(curTimeout, loop.keys)
    for i in 0..<count:
      let fd = loop.keys[i].fd
      let events = loop.keys[i].events

      withData(loop.selector, fd, adata) do:
        if Event.Read in events or events == {Event.Error}:
          if not adata.reader.deleted:
            loop.callbacks.addLast(adata.reader)

        if Event.Write in events or events == {Event.Error}:
          if not adata.writer.deleted:
            loop.callbacks.addLast(adata.writer)

        if Event.User in events:
          if not adata.reader.deleted:
            loop.callbacks.addLast(adata.reader)

        when ioselSupportedPlatform:
          if customSet * events != {}:
            if not adata.reader.deleted:
              loop.callbacks.addLast(adata.reader)

    # Moving expired timers to `loop.callbacks`.
    loop.processTimers()

    # All callbacks which will be added in process, will be processed on next
    # poll() call.
    loop.processCallbacks()

  proc initAPI() =
    discard getGlobalDispatcher()

proc addTimer*(at: Moment, cb: CallbackFunc, udata: pointer = nil) =
  ## Arrange for the callback ``cb`` to be called at the given absolute
  ## timestamp ``at``. You can also pass ``udata`` to callback.
  let loop = getGlobalDispatcher()
  var tcb = TimerCallback(finishAt: at,
                          function: AsyncCallback(function: cb, udata: udata))
  loop.timers.push(tcb)

proc addTimer*(at: int64, cb: CallbackFunc, udata: pointer = nil) {.
     inline, deprecated: "Use addTimer(Duration, cb, udata)".} =
  addTimer(Moment.init(at, Millisecond), cb, udata)

proc removeTimer*(at: Moment, cb: CallbackFunc, udata: pointer = nil) =
  ## Remove timer callback ``cb`` with absolute timestamp ``at`` from waiting
  ## queue.
  let loop = getGlobalDispatcher()
  var list = cast[seq[TimerCallback]](loop.timers)
  var index = -1
  for i in 0..<len(list):
    if list[i].finishAt == at and list[i].function.function == cb and
       list[i].function.udata == udata:
      index = i
      break
  if index != -1:
    loop.timers.del(index)

proc removeTimer*(at: int64, cb: CallbackFunc, udata: pointer = nil) {.
     inline, deprecated: "Use removeTimer(Duration, cb, udata)".} =
  removeTimer(Moment.init(at, Millisecond), cb, udata)

proc sleepAsync*(ms: Duration): Future[void] =
  ## Suspends the execution of the current async procedure for the next
  ## ``ms`` milliseconds.
  var retFuture = newFuture[void]("sleepAsync")
  proc completion(data: pointer) =
    if not retFuture.finished:
      retFuture.complete()
  addTimer(Moment.fromNow(ms), completion, cast[pointer](retFuture))
  return retFuture

proc sleepAsync*(ms: int): Future[void] {.
     inline, deprecated: "Use sleepAsync(Duration)".} =
  result = sleepAsync(ms.milliseconds())

proc withTimeout*[T](fut: Future[T], timeout: Duration): Future[bool] =
  ## Returns a future which will complete once ``fut`` completes or after
  ## ``timeout`` milliseconds has elapsed.
  ##
  ## If ``fut`` completes first the returned future will hold true,
  ## otherwise, if ``timeout`` milliseconds has elapsed first, the returned
  ## future will hold false.
  var retFuture = newFuture[bool]("asyncdispatch.`withTimeout`")
  proc continuation(udata: pointer) {.gcsafe.} =
    if not retFuture.finished:
      if isNil(udata):
        fut.removeCallback(continuation)
        retFuture.complete(false)
      else:
        if not retFuture.finished:
          retFuture.complete(true)
  addTimer(Moment.fromNow(timeout), continuation, nil)
  fut.addCallback(continuation)
  return retFuture

proc withTimeout*[T](fut: Future[T], timeout: int): Future[bool] {.
     inline, deprecated: "Use withTimeout(Future[T], Duration)".} =
  result = withTimeout(fut, timeout.milliseconds())

proc wait*[T](fut: Future[T], timeout = InfiniteDuration): Future[T] =
  ## Returns a future which will complete once future ``fut`` completes
  ## or if timeout of ``timeout`` milliseconds has been expired.
  ##
  ## If ``timeout`` is ``-1``, then statement ``await wait(fut)`` is
  ## equal to ``await fut``.
  var retFuture = newFuture[T]("asyncdispatch.wait")
  proc continuation(udata: pointer) {.gcsafe.} =
    if not retFuture.finished:
      if isNil(udata):
        fut.removeCallback(continuation)
        retFuture.fail(newException(AsyncTimeoutError, ""))
      else:
        if not retFuture.finished:
          if fut.failed:
            retFuture.fail(fut.error)
          else:
            retFuture.complete(fut.read())
  if timeout.isInfinite():
    retFuture = fut
  elif timeout.isZero():
    if fut.finished:
      if fut.failed:
        retFuture.fail(fut.error)
      else:
        retFuture.complete(fut.read())
    else:
      retFuture.fail(newException(AsyncTimeoutError, ""))
  else:
    addTimer(Moment.fromNow(timeout), continuation, nil)
    fut.addCallback(continuation)
  return retFuture

proc wait*[T](fut: Future[T], timeout = -1): Future[T] {.
     inline, deprecated: "Use wait(Future[T], Duration)".} =
  if timeout == -1:
    wait(fut, InfiniteDuration)
  elif timeout == 0:
    wait(fut, ZeroDuration)
  else:
    wait(fut, timeout.milliseconds())

include asyncmacro2

proc callSoon(cbproc: CallbackFunc, data: pointer = nil) =
  ## Schedule `cbproc` to be called as soon as possible.
  ## The callback is called when control returns to the event loop.
  doAssert cbproc != nil
  let acb = AsyncCallback(function: cbproc, udata: data)
  getGlobalDispatcher().callbacks.addLast(acb)

proc runForever*() =
  ## Begins a never ending global dispatcher poll loop.
  while true:
    poll()

proc waitFor*[T](fut: Future[T]): T =
  ## **Blocks** the current thread until the specified future completes.
  while not fut.finished:
    poll()

  fut.read

# Global API and callSoon() initialization.
initAPI()
