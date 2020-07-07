# Chronos - An efficient library for asynchronous programming

[![Build Status (Travis)](https://img.shields.io/travis/status-im/nim-chronos/master.svg?label=Linux%20/%20macOS "Linux/macOS build status (Travis)")](https://travis-ci.org/status-im/nim-chronos)
[![Windows build status (Appveyor)](https://img.shields.io/appveyor/ci/nimbus/nim-asyncdispatch2/master.svg?label=Windows "Windows build status (Appveyor)")](https://ci.appveyor.com/project/nimbus/nim-asyncdispatch2)
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

## TODO
  * Pipe/Subprocess Transports.
  * Multithreading Stream/Datagram servers
  * Future[T] cancelation

## Contributing

When submitting pull requests, please add test cases for any new features or fixes and make sure `nimble test` is still able to execute the entire test suite successfully.

## License

Licensed and distributed under either of

* MIT license: [LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT

or

* Apache License, Version 2.0, ([LICENSE-APACHEv2](LICENSE-APACHEv2) or http://www.apache.org/licenses/LICENSE-2.0)

at your option. These files may not be copied, modified, or distributed except according to those terms.

