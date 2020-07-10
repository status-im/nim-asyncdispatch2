# Chronos - An efficient library for asynchronous programming

[![Build Status (Travis)](https://img.shields.io/travis/status-im/nim-chronos/master.svg?label=Linux%20/%20macOS "Linux/macOS build status (Travis)")](https://travis-ci.org/status-im/nim-chronos)
[![Windows build status (AppVeyor)](https://img.shields.io/appveyor/ci/nimbus/nim-asyncdispatch2/master.svg?label=Windows "Windows build status (Appveyor)")](https://ci.appveyor.com/project/nimbus/nim-asyncdispatch2)
[![License: Apache](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![Stability: experimental](https://img.shields.io/badge/stability-experimental-orange.svg)

## Introduction

Chronos is an [asyncdispatch](https://nim-lang.org/docs/asyncdispatch.html)
fork with a unified callback type, FIFO processing order for Future callbacks and [many other changes](https://github.com/status-im/nim-chronos/wiki/AsyncDispatch-comparison) that diverged from upstream's philosophy.

## Installation
You can use Nim's official package manager Nimble to install Chronos:

```
$ nimble install https://github.com/status-im/nim-chronos.git
```

## Documentation

### Concepts

Chronos implements the async/await paradigm in a self-contained library, using
macros, with no specific helpers from the compiler.

Our event loop is called a "dispatcher" and a single instance per thread is
created, as soon as one is needed.

To trigger a dispatcher's processing step, we need to call `poll()` - either
directly or through a wrapper like `runForever()` or `waitFor()`. This step
handles any file descriptors, timers and callbacks that are ready to be
processed.

`Future` objects encapsulate the result of an async procedure, upon successful
completion, and a list of callbacks to be scheduled after completion or
cancellation. (These explicit callbacks are rarely used outside Chronos, being
replaced with the implicit ones generated by `await` chaining.)

Async procedures (those using the `{.async.}` pragma) return `Future` objects.

Inside an async procedure, you can `await` the future returned by another async
procedure. At this point, control will be handled to the event loop until that
future is completed.

### Dispatcher

You can run the "dispatcher" event loop forever, with `runForever()` which is defined as:

```nim
proc runForever*() =
  while true:
    poll()
```

You can also run it until a certain future is completed, with `waitFor()` which
will also call `Future.read()` on it:

```nim
proc p(): Future[int] {.async.} =
  await sleepAsync(100.milliseconds)
  return 1

echo waitFor p() # prints "1"
```

`waitFor()` is defined like this:

```nim
proc waitFor*[T](fut: Future[T]): T =
  while not(fut.finished()):
    poll()
  return fut.read()
```

### Async procedures and methods

The `{.async.}` pragma will transform a procedure (or a method) returning a
specialised `Future` type into a closure iterator. If there is no return type
specified, a `Future[void]` is returned.

```nim
proc p() {.async.} =
  await sleepAsync(100.milliseconds)

echo p().type # prints "Future[system.void]"
```

Whenever `await` is encountered inside an async procedure, control is passed
back to the dispatcher for as many steps as it's necessary for the awaited
future to complete (or be cancelled). `await` calls the equivalent of
`Future.read()` on the completed future and returns the encapsulated value.

## TODO
  * Pipe/Subprocess Transports.
  * Multithreading Stream/Datagram servers
  * Future[T] cancellation

## Contributing

When submitting pull requests, please add test cases for any new features or fixes and make sure `nimble test` is still able to execute the entire test suite successfully.

## License

Licensed and distributed under either of

* MIT license: [LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT

or

* Apache License, Version 2.0, ([LICENSE-APACHEv2](LICENSE-APACHEv2) or http://www.apache.org/licenses/LICENSE-2.0)

at your option. These files may not be copied, modified, or distributed except according to those terms.

