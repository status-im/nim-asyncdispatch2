#
#            Chronos synchronization primitives
#
#           (c) Copyright 2018-Present Eugene Kabanov
#  (c) Copyright 2018-Present Status Research & Development GmbH
#
#                Licensed under either of
#    Apache License, version 2.0, (LICENSE-APACHEv2)
#                MIT license (LICENSE-MIT)

## This module implements some core synchronization primitives
import os, deques
import asyncloop, osapi, handles, timer

const hasThreadSupport* = compileOption("threads")

when hasThreadSupport:
  import locks

when defined(windows):
  import winlean
else:
  import posix

type
  AsyncLock* = ref object of RootRef
    ## A primitive lock is a synchronization primitive that is not owned by
    ## a particular coroutine when locked. A primitive lock is in one of two
    ## states, ``locked`` or ``unlocked``.
    ##
    ## When more than one coroutine is blocked in ``acquire()`` waiting for
    ## the state to turn to unlocked, only ``ONE`` coroutine proceeds when a
    ## ``release()`` call resets the state to unlocked; first coroutine which
    ## is blocked in ``acquire()`` is being processed.
    locked: bool
    waiters: seq[Future[void]]

  AsyncEvent* = ref object of RootRef
    ## A primitive event object.
    ##
    ## An event manages a flag that can be set to `true` with the ``fire()``
    ## procedure and reset to `false` with the ``clear()`` procedure.
    ## The ``wait()`` coroutine blocks until the flag is `false`.
    ##
    ## If more than one coroutine blocked in ``wait()`` waiting for event
    ## state to be signaled, when event get fired, then ``ALL`` coroutines
    ## continue proceeds in order, they have entered waiting state.
    flag: bool
    waiters: seq[Future[void]]

  AsyncQueue*[T] = ref object of RootRef
    ## A queue, useful for coordinating producer and consumer coroutines.
    ##
    ## If ``maxsize`` is less than or equal to zero, the queue size is
    ## infinite. If it is an integer greater than ``0``, then "await put()"
    ## will block when the queue reaches ``maxsize``, until an item is
    ## removed by "await get()".
    getters: seq[Future[void]]
    putters: seq[Future[void]]
    queue: Deque[T]
    maxsize: int

  AsyncThreadEventImpl = object
    when defined(linux):
      efd: AsyncFD
    elif defined(windows):
      event: Handle
    else:
      when hasThreadSupport:
        # We need this lock to have behavior equal to Windows' event object and
        # Linux' eventfd descriptor. Otherwise our Event becomes Semaphore on
        # BSD/MacOS/Solaris.
        lock: Lock
      flag: bool
      rfd: AsyncFD
      wfd: AsyncFD

  AsyncThreadEvent* = ptr AsyncThreadEventImpl
    ## A primitive event object which can be shared between threads.
    ##
    ## An event manages a flag that can be set to `true` with the ``fire()``
    ## procedure.
    ## The ``wait()`` coroutine blocks until the flag is `false`.
    ## The ``waitSync()`` procedure blocks until the flag is `false`.
    ##
    ## If more than one coroutine blocked in ``wait()`` waiting for event state
    ## to be signalled, when event get fired, only ``ONE`` coroutine proceeds.

  WaitResult* = enum
    WaitSuccess, WaitTimeout, WaitFailed

  AsyncQueueEmptyError* = object of Exception
    ## ``AsyncQueue`` is empty.
  AsyncQueueFullError* = object of Exception
    ## ``AsyncQueue`` is full.
  AsyncLockError* = object of Exception
    ## ``AsyncLock`` is either locked or unlocked.

proc newAsyncLock*(): AsyncLock =
  ## Creates new asynchronous lock ``AsyncLock``.
  ##
  ## Lock is created in the unlocked state. When the state is unlocked,
  ## ``acquire()`` changes the state to locked and returns immediately.
  ## When the state is locked, ``acquire()`` blocks until a call to
  ## ``release()`` in another coroutine changes it to unlocked.
  ##
  ## The ``release()`` procedure changes the state to unlocked and returns
  ## immediately.

  # Workaround for callSoon() not worked correctly before
  # getGlobalDispatcher() call.
  discard getGlobalDispatcher()
  result = new AsyncLock
  result.waiters = newSeq[Future[void]]()
  result.locked = false

proc wakeUpFirst(lock: AsyncLock) {.inline.} =
  ## Wake up the first waiter if it isn't done.
  for fut in lock.waiters.mitems():
    if not(fut.finished()):
      fut.complete()
      break

proc checkAll(lock: AsyncLock): bool {.inline.} =
  ## Returns ``true`` if waiters array is empty or full of cancelled futures.
  result = true
  for fut in lock.waiters.mitems():
    if not(fut.cancelled()):
      result = false
      break

proc removeWaiter(lock: AsyncLock, waiter: Future[void]) {.inline.} =
  ## Removes ``waiter`` from list of waiters in ``lock``.
  lock.waiters.delete(lock.waiters.find(waiter))

proc acquire*(lock: AsyncLock) {.async.} =
  ## Acquire a lock ``lock``.
  ##
  ## This procedure blocks until the lock ``lock`` is unlocked, then sets it
  ## to locked and returns.
  if not(lock.locked) and lock.checkAll():
    lock.locked = true
  else:
    var w = newFuture[void]("AsyncLock.acquire")
    lock.waiters.add(w)
    try:
      try:
        await w
      finally:
        lock.removeWaiter(w)
    except CancelledError:
      if not(lock.locked):
        lock.wakeUpFirst()
      raise
    lock.locked = true

proc locked*(lock: AsyncLock): bool =
  ## Return `true` if the lock ``lock`` is acquired, `false` otherwise.
  result = lock.locked

proc release*(lock: AsyncLock) =
  ## Release a lock ``lock``.
  ##
  ## When the ``lock`` is locked, reset it to unlocked, and return. If any
  ## other coroutines are blocked waiting for the lock to become unlocked,
  ## allow exactly one of them to proceed.
  if lock.locked:
    lock.locked = false
    lock.wakeUpFirst()
  else:
    raise newException(AsyncLockError, "AsyncLock is not acquired!")

proc newAsyncEvent*(): AsyncEvent =
  ## Creates new asyncronous event ``AsyncEvent``.
  ##
  ## An event manages a flag that can be set to `true` with the `fire()`
  ## procedure and reset to `false` with the `clear()` procedure.
  ## The `wait()` procedure blocks until the flag is `true`. The flag is
  ## initially `false`.

  # Workaround for callSoon() not worked correctly before
  # getGlobalDispatcher() call.
  discard getGlobalDispatcher()
  result = new AsyncEvent
  result.waiters = newSeq[Future[void]]()
  result.flag = false

proc removeWaiter(event: AsyncEvent, waiter: Future[void]) {.inline.} =
  ## Removes ``waiter`` from list of waiters in ``lock``.
  event.waiters.delete(event.waiters.find(waiter))

proc wait*(event: AsyncEvent) {.async.} =
  ## Block until the internal flag of ``event`` is `true`.
  ## If the internal flag is `true` on entry, return immediately. Otherwise,
  ## block until another task calls `fire()` to set the flag to `true`,
  ## then return.
  if not(event.flag):
    var w = newFuture[void]("AsyncEvent.wait")
    event.waiters.add(w)
    try:
      await w
    finally:
      event.removeWaiter(w)

proc fire*(event: AsyncEvent) =
  ## Set the internal flag of ``event`` to `true`. All tasks waiting for it
  ## to become `true` are awakened. Task that call `wait()` once the flag is
  ## `true` will not block at all.
  if not(event.flag):
    event.flag = true
    for fut in event.waiters:
      if not(fut.finished()):
        fut.complete()

proc clear*(event: AsyncEvent) =
  ## Reset the internal flag of ``event`` to `false`. Subsequently, tasks
  ## calling `wait()` will block until `fire()` is called to set the internal
  ## flag to `true` again.
  event.flag = false

proc isSet*(event: AsyncEvent): bool =
  ## Return `true` if and only if the internal flag of ``event`` is `true`.
  result = event.flag

proc newAsyncQueue*[T](maxsize: int = 0): AsyncQueue[T] =
  ## Creates a new asynchronous queue ``AsyncQueue``.

  # Workaround for callSoon() not worked correctly before
  # getGlobalDispatcher() call.
  discard getGlobalDispatcher()
  result = new AsyncQueue[T]
  result.getters = newSeq[Future[void]]()
  result.putters = newSeq[Future[void]]()
  result.queue = initDeque[T]()
  result.maxsize = maxsize

proc wakeupNext(waiters: var seq[Future[void]]) {.inline.} =
  var i = 0
  while i < len(waiters):
    var waiter = waiters[i]
    if not(waiter.finished()):
      let length = len(waiters) - (i + 1)
      let offset = len(waiters) - length
      if length > 0:
        for k in 0..<length:
          shallowCopy(waiters[k], waiters[k + offset])
      waiters.setLen(length)
      waiter.complete()
      break
    inc(i)

proc removeWaiter(waiters: var seq[Future[void]],
                  waiter: Future[void]) {.inline.} =
  ## Safely remove ``waiter`` from list of waiters in ``waiters``. This
  ## procedure will not raise if ``waiter`` is not in the list of waiters.
  var index = waiters.find(waiter)
  if index >= 0:
    waiters.delete(index)

proc removeWaiter(waiters: var Deque[Future[void]],
                  fut: Future[void]) {.inline.} =
  var nwaiters = initDeque[Future[void]]()
  while len(waiters) > 0:
    var waiter = waiters.popFirst()
    if waiter != fut:
      nwaiters.addFirst(waiter)

proc full*[T](aq: AsyncQueue[T]): bool {.inline.} =
  ## Return ``true`` if there are ``maxsize`` items in the queue.
  ##
  ## Note: If the ``aq`` was initialized with ``maxsize = 0`` (default),
  ## then ``full()`` is never ``true``.
  if aq.maxsize <= 0:
    result = false
  else:
    result = (len(aq.queue) >= aq.maxsize)

proc empty*[T](aq: AsyncQueue[T]): bool {.inline.} =
  ## Return ``true`` if the queue is empty, ``false`` otherwise.
  result = (len(aq.queue) == 0)

proc addFirstNoWait*[T](aq: AsyncQueue[T], item: T) =
  ## Put an item ``item`` to the beginning of the queue ``aq`` immediately.
  ##
  ## If queue ``aq`` is full, then ``AsyncQueueFullError`` exception raised.
  var w: Future[void]
  if aq.full():
    raise newException(AsyncQueueFullError, "AsyncQueue is full!")
  aq.queue.addFirst(item)
  aq.getters.wakeupNext()

proc addLastNoWait*[T](aq: AsyncQueue[T], item: T) =
  ## Put an item ``item`` at the end of the queue ``aq`` immediately.
  ##
  ## If queue ``aq`` is full, then ``AsyncQueueFullError`` exception raised.
  if aq.full():
    raise newException(AsyncQueueFullError, "AsyncQueue is full!")
  aq.queue.addLast(item)
  aq.getters.wakeupNext()

proc popFirstNoWait*[T](aq: AsyncQueue[T]): T =
  ## Get an item from the beginning of the queue ``aq`` immediately.
  ##
  ## If queue ``aq`` is empty, then ``AsyncQueueEmptyError`` exception raised.
  if aq.empty():
    raise newException(AsyncQueueEmptyError, "AsyncQueue is empty!")
  result = aq.queue.popFirst()
  aq.putters.wakeupNext()

proc popLastNoWait*[T](aq: AsyncQueue[T]): T =
  ## Get an item from the end of the queue ``aq`` immediately.
  ##
  ## If queue ``aq`` is empty, then ``AsyncQueueEmptyError`` exception raised.
  var w: Future[void]
  if aq.empty():
    raise newException(AsyncQueueEmptyError, "AsyncQueue is empty!")
  result = aq.queue.popLast()
  aq.putters.wakeupNext()

proc addFirst*[T](aq: AsyncQueue[T], item: T) {.async.} =
  ## Put an ``item`` to the beginning of the queue ``aq``. If the queue is full,
  ## wait until a free slot is available before adding item.
  while aq.full():
    var putter = newFuture[void]("AsyncQueue.addFirst")
    aq.putters.add(putter)
    try:
      await putter
    except:
      aq.putters.removeWaiter(putter)
      if not aq.full() and not(putter.cancelled()):
        aq.putters.wakeupNext()
      raise
  aq.addFirstNoWait(item)

proc addLast*[T](aq: AsyncQueue[T], item: T) {.async.} =
  ## Put an ``item`` to the end of the queue ``aq``. If the queue is full,
  ## wait until a free slot is available before adding item.
  while aq.full():
    var putter = newFuture[void]("AsyncQueue.addLast")
    aq.putters.add(putter)
    try:
      await putter
    except:
      aq.putters.removeWaiter(putter)
      if not aq.full() and not(putter.cancelled()):
        aq.putters.wakeupNext()
      raise
  aq.addLastNoWait(item)

proc popFirst*[T](aq: AsyncQueue[T]): Future[T] {.async.} =
  ## Remove and return an ``item`` from the beginning of the queue ``aq``.
  ## If the queue is empty, wait until an item is available.
  while aq.empty():
    var getter = newFuture[void]("AsyncQueue.popFirst")
    aq.getters.add(getter)
    try:
      await getter
    except:
      aq.getters.removeWaiter(getter)
      if not(aq.empty()) and not(getter.cancelled()):
        aq.getters.wakeupNext()
      raise
  result = aq.popFirstNoWait()

proc popLast*[T](aq: AsyncQueue[T]): Future[T] {.async.} =
  ## Remove and return an ``item`` from the end of the queue ``aq``.
  ## If the queue is empty, wait until an item is available.
  while aq.empty():
    var getter = newFuture[void]("AsyncQueue.popLast")
    aq.getters.add(getter)
    try:
      await getter
    except:
      aq.getters.removeWaiter(getter)
      if not(aq.empty()) and not(getter.cancelled()):
        aq.getters.wakeupNext()
      raise
  result = aq.popLastNoWait()

proc putNoWait*[T](aq: AsyncQueue[T], item: T) {.inline.} =
  ## Alias of ``addLastNoWait()``.
  aq.addLastNoWait(item)

proc getNoWait*[T](aq: AsyncQueue[T]): T {.inline.} =
  ## Alias of ``popFirstNoWait()``.
  result = aq.popFirstNoWait()

proc put*[T](aq: AsyncQueue[T], item: T): Future[void] {.inline.} =
  ## Alias of ``addLast()``.
  result = aq.addLast(item)

proc get*[T](aq: AsyncQueue[T]): Future[T] {.inline.} =
  ## Alias of ``popFirst()``.
  result = aq.popFirst()

proc clear*[T](aq: AsyncQueue[T]) {.inline.} =
  ## Clears all elements of queue ``aq``.
  aq.queue.clear()

proc len*[T](aq: AsyncQueue[T]): int {.inline.} =
  ## Return the number of elements in ``aq``.
  result = len(aq.queue)

proc size*[T](aq: AsyncQueue[T]): int {.inline.} =
  ## Return the maximum number of elements in ``aq``.
  result = len(aq.maxsize)

proc `[]`*[T](aq: AsyncQueue[T], i: Natural) : T {.inline.} =
  ## Access the i-th element of ``aq`` by order from first to last.
  ## ``aq[0]`` is the first element, ``aq[^1]`` is the last element.
  result = aq.queue[i]

proc `[]`*[T](aq: AsyncQueue[T], i: BackwardsIndex) : T {.inline.} =
  ## Access the i-th element of ``aq`` by order from first to last.
  ## ``aq[0]`` is the first element, ``aq[^1]`` is the last element.
  result = aq.queue[len(aq.queue) - int(i)]

proc `[]=`* [T](aq: AsyncQueue[T], i: Natural, item: T) {.inline.} =
  ## Change the i-th element of ``aq``.
  aq.queue[i] = item

proc `[]=`* [T](aq: AsyncQueue[T], i: BackwardsIndex, item: T) {.inline.} =
  ## Change the i-th element of ``aq``.
  aq.queue[len(aq.queue) - int(i)] = item

iterator items*[T](aq: AsyncQueue[T]): T {.inline.} =
  ## Yield every element of ``aq``.
  for item in aq.queue.items():
    yield item

iterator mitems*[T](aq: AsyncQueue[T]): var T {.inline.} =
  ## Yield every element of ``aq``.
  for mitem in aq.queue.mitems():
    yield mitem

iterator pairs*[T](aq: AsyncQueue[T]): tuple[key: int, val: T] {.inline.} =
  ## Yield every (position, value) of ``aq``.
  for pair in aq.queue.pairs():
    yield pair

proc contains*[T](aq: AsyncQueue[T], item: T): bool {.inline.} =
  ## Return true if ``item`` is in ``aq`` or false if not found. Usually used
  ## via the ``in`` operator.
  for e in aq.queue.items():
    if e == item: return true
  return false

proc `$`*[T](aq: AsyncQueue[T]): string =
  ## Turn an async queue ``aq`` into its string representation.
  result = "["
  for item in aq.queue.items():
    if result.len > 1: result.add(", ")
    result.addQuoted(item)
  result.add("]")

proc newAsyncThreadEvent*(): AsyncThreadEvent =
  ## Create new AsyncThreadEvent event.
  when defined(linux):
    # On Linux we are using `eventfd`.
    let fd = eventfd(0, 0)
    if fd == -1:
      raiseOSError(osLastError())
    if not(setSocketBlocking(SocketHandle(fd), false)):
      raiseOSError(osLastError())
    result = cast[AsyncThreadEvent](allocShared0(sizeof(AsyncThreadEventImpl)))
    result.efd = AsyncFD(fd)
  elif defined(windows):
    # On Windows we are using kernel Event object.
    let event = osapi.createEvent(nil, DWORD(0), DWORD(0), nil)
    if event == Handle(0):
      raiseOSError(osLastError())
    result = cast[AsyncThreadEvent](allocShared0(sizeof(AsyncThreadEventImpl)))
    result.event = event
  else:
    # On all other posix systems we are using anonymous pipe.
    var (rfd, wfd) = createAsyncPipe()
    if rfd == asyncInvalidPipe or wfd == asyncInvalidPipe:
      raiseOSError(osLastError())
    if not(setSocketBlocking(SocketHandle(wfd), true)):
      raiseOSError(osLastError())
    result = cast[AsyncThreadEvent](allocShared0(sizeof(AsyncThreadEventImpl)))
    result.rfd = rfd
    result.wfd = wfd
    result.flag = false
    when hasThreadSupport:
      initLock(result.lock)

proc close*(event: AsyncThreadEvent) =
  ## Close AsyncThreadEvent ``event`` and free all the resources.
  when defined(linux):
    let loop = getGlobalDispatcher()
    if event.efd in loop:
      unregister(event.efd)
    discard posix.close(cint(event.efd))
  elif defined(windows):
    discard winlean.closeHandle(event.event)
  else:
    let loop = getGlobalDispatcher()
    when hasThreadSupport:
      acquire(event.lock)
    if event.rfd in loop:
      unregister(event.rfd)
    discard posix.close(cint(event.rfd))
    discard posix.close(cint(event.wfd))
    when hasThreadSupport:
      deinitLock(event.lock)
  deallocShared(event)

proc fire*(event: AsyncThreadEvent) =
  ## Set state of AsyncThreadEvent ``event`` to signalled.
  when defined(linux):
    var data = 1'u64
    while true:
      if posix.write(cint(event.efd), addr data, sizeof(uint64)) == -1:
        let err = osLastError()
        if cint(err) == posix.EINTR:
          continue
        raiseOSError(osLastError())
      break
  elif defined(windows):
    if setEvent(event.event) == 0:
      raiseOSError(osLastError())
  else:
    var data = 1'u64
    when hasThreadSupport:
      acquire(event.lock)
      try:
        if not(event.flag):
          while true:
            if posix.write(cint(event.wfd), addr data, sizeof(uint64)) == -1:
              let err = osLastError()
              if cint(err) == posix.EINTR:
                continue
              raiseOSError(osLastError())
            break
          event.flag = true
      finally:
        release(event.lock)
    else:
      if not(event.flag):
        while true:
          if posix.write(cint(event.wfd), addr data, sizeof(uint64)) == -1:
            let err = osLastError()
            if cint(err) == posix.EINTR:
              continue
            raiseOSError(osLastError())
          break
        event.flag = true

when defined(windows):
  proc wait*(event: AsyncThreadEvent,
           timeout: Duration = InfiniteDuration): Future[WaitResult] {.async.} =
    ## Block until the internal flag of ``event`` is `true`. This procedure is
    ## coroutine.
    ##
    ## Procedure returns ``WaitSuccess`` when internal event's state is
    ## signaled. Returns ``WaitTimeout`` when timeout interval elapsed, and the
    ## event's state is nonsignaled. Returns ``WaitFailed`` if error happens
    ## while waiting.
    try:
      let res = await awaitForSingleObject(event.event, timeout)
      if res:
        result = WaitSuccess
      else:
        result = WaitTimeout
    except OSError:
      result = WaitFailed
    except AsyncError:
      result = WaitFailed

  proc waitSync*(event: AsyncThreadEvent,
                 timeout: Duration = InfiniteDuration): WaitResult =
    ## Block until the internal flag of ``event`` is `true`. This procedure is
    ## ``NOT`` coroutine, so it is actually blocks, but this procedure do not
    ## need asynchronous event loop to be present.
    ##
    ## Procedure returns ``WaitSuccess`` when internal event's state is
    ## signaled. Returns ``WaitTimeout`` when timeout interval elapsed, and the
    ## event's state is nonsignaled. Returns ``WaitFailed`` if error happens
    ## while waiting.
    var timeoutWin: DWORD
    if timeout.isInfinite():
      timeoutWin = INFINITE
    else:
      timeoutWin = DWORD(timeout.milliseconds)
    let res = waitForSingleObject(event.event, timeoutWin)
    if res == WAIT_OBJECT_0:
      result = WaitSuccess
    elif res == winlean.WAIT_TIMEOUT:
      result = WaitTimeout
    else:
      result = WaitFailed
else:
  proc wait*(event: AsyncThreadEvent,
             timeout: Duration = InfiniteDuration): Future[WaitResult] =
    ## Block until the internal flag of ``event`` is `true`.
    ##
    ## Procedure returns ``WaitSuccess`` when internal event's state is
    ## signaled. Returns ``WaitTimeout`` when timeout interval elapsed, and the
    ## event's state is nonsignaled. Returns ``WaitFailed`` if error happens
    ## while waiting.
    var moment: Moment
    var retFuture = newFuture[WaitResult]("mtevent.wait")
    let loop = getGlobalDispatcher()

    when defined(linux):
      let fd = AsyncFD(event.efd)
    else:
      let fd = AsyncFD(event.rfd)

    proc contiunuation(udata: pointer) {.gcsafe.} =
      if not(retFuture.finished()):
        var data: uint64 = 0
        if isNil(udata):
          removeReader(fd)
          retFuture.complete(WaitTimeout)
        else:
          while true:
            if posix.read(cint(fd), addr data,
                          sizeof(uint64)) != sizeof(uint64):
              let err = osLastError()
              if cint(err) == posix.EINTR:
                # This error happens when interrupt signal was received by
                # process so we need to repeat `read` syscall.
                continue
              elif cint(err) == posix.EAGAIN or
                   cint(err) == posix.EWOULDBLOCK:
                # This error happens when there already pending `read` syscall
                # in different thread for this descriptor. This is race
                # condition, so to avoid it we will wait for another `read`
                # event from system queue.
                break
              else:
                # All other errors
                removeReader(fd)
              retFuture.complete(WaitFailed)
            else:
              removeReader(fd)
              when not(defined(linux)):
                when hasThreadSupport:
                  acquire(event.lock)
                event.flag = false
                when hasThreadSupport:
                  release(event.lock)
              retFuture.complete(WaitSuccess)
            break

    proc cancel(udata: pointer) {.gcsafe.} =
      if not(retFuture.finished()):
        removeTimer(moment, contiunuation, nil)
        removeReader(fd)

    if fd notin loop:
      register(fd)
    addReader(fd, contiunuation, cast[pointer](retFuture))
    if not(timeout.isInfinite()):
      moment = Moment.fromNow(timeout)
      addTimer(moment, contiunuation, nil)

    retFuture.cancelCallback = cancel
    return retFuture

  proc waitReady(fd: int, timeout: var Duration): WaitResult {.inline.} =
    var tv: Timeval
    var ptv: ptr Timeval = addr tv
    var rset: TFdSet
    posix.FD_ZERO(rset)
    posix.FD_SET(SocketHandle(fd), rset)
    if timeout.isInfinite():
      ptv = nil
    else:
      tv = timeout.toTimeval()
    while true:
      let nfd = fd + 1
      var smoment = Moment.now()
      let res = posix.select(cint(nfd), addr rset, nil, nil, ptv)
      var emoment = Moment.now()
      if res == 1:
        result = WaitSuccess
        if not(timeout.isInfinite()):
          timeout = timeout - (emoment - smoment)
        break
      elif res == 0:
        result = WaitTimeout
        if not(timeout.isInfinite()):
          timeout = ZeroDuration
        break
      elif res == -1:
        let err = osLastError()
        if int(err) == EINTR:
          if not(timeout.isInfinite()):
            tv = (emoment - smoment).toTimeval()
          continue

  proc waitSync*(event: AsyncThreadEvent,
                 timeout: Duration = InfiniteDuration): WaitResult =
    ## Block until the internal flag of ``event`` is `true`. This procedure is
    ## ``NOT`` coroutine, so it is actually blocks, but this procedure do not
    ## need asynchronous event loop to be present.
    ##
    ## Procedure returns ``WaitSuccess`` when internal event's state is
    ## signaled. Returns ``WaitTimeout`` when timeout interval elapsed, and the
    ## event's state is nonsignaled. Returns ``WaitFailed`` if error happens
    ## while waiting.
    var data = 0'u64

    when defined(linux):
      var fd = int(event.efd)
    else:
      var fd = int(event.rfd)

    var curtimeout = timeout

    while true:
      var repeat = false
      let res = waitReady(fd, curtimeout)
      if res == WaitSuccess:
        # Updating timeout value for next iteration.
        when defined(linux):
          while true:
            if posix.read(cint(fd), addr data,
                          sizeof(uint64)) != sizeof(uint64):
              let err = osLastError()
              if cint(err) == posix.EINTR:
                continue
              elif cint(err) == posix.EAGAIN or
                   cint(err) == posix.EWOULDBLOCK:
                # This error happens when there already pending `read` syscall
                # in different thread for this descriptor.
                repeat = true
                break
              result = WaitFailed
            else:
              result = WaitSuccess
            break
        else:
          when hasThreadSupport:
            acquire(event.lock)

          while true:
            if posix.read(cint(fd), addr data,
                          sizeof(uint64)) != sizeof(uint64):
              let err = osLastError()
              if cint(err) == posix.EINTR:
                continue
              elif cint(err) == posix.EAGAIN or
                   cint(err) == posix.EWOULDBLOCK:
                # This error happens when there already pending `read` syscall
                # in different thread for this descriptor.
                repeat = true
                break
              else:
                result = WaitFailed
            else:
              result = WaitSuccess
            break

          if repeat:
            when hasThreadSupport:
              release(event.lock)
            discard
          else:
            event.flag = false
            when hasThreadSupport:
              release(event.lock)
      else:
        result = res

      if not(repeat):
        break
