#
#               Chronos OS API declarations
#              (c) Copyright 2018-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

## This module implements a small wrapper for some needed Windows/*nix API
## procedures, which are not defined in Nim stdlib modules, or its definition
## is wrong.
when defined(windows):
  import winlean

  const
    TCP_NODELAY* = 1
    IPPROTO_TCP* = 6
    PIPE_TYPE_BYTE* = 0x00000000'i32
    PIPE_READMODE_BYTE* = 0x00000000'i32
    PIPE_WAIT* = 0x00000000'i32
    DEFAULT_PIPE_SIZE* = 65536'i32
    ERROR_PIPE_CONNECTED* = 535
    ERROR_PIPE_BUSY* = 231
    ERROR_OPERATION_ABORTED* = 995
    ERROR_SUCCESS* = 0
    ERROR_CONNECTION_REFUSED* = 1225
    PIPE_TYPE_MESSAGE* = 0x4
    PIPE_READMODE_MESSAGE* = 0x2
    PIPE_UNLIMITED_INSTANCES* = 255
    ERROR_BROKEN_PIPE* = 109
    ERROR_PIPE_NOT_CONNECTED* = 233
    ERROR_NO_DATA* = 232
    ERROR_CONNECTION_ABORTED* = 1236

  proc createEvent*(lpEventAttributes: ptr SECURITY_ATTRIBUTES,
                    bManualReset: DWORD, bInitialState: DWORD,
                    lpName: ptr Utf16Char): Handle
       {.stdcall, dynlib: "kernel32", importc: "CreateEventW".}

  proc connectNamedPipe*(hNamedPipe: Handle, lpOverlapped: pointer): WINBOOL
       {.importc: "ConnectNamedPipe", stdcall, dynlib: "kernel32".}

  proc cancelIo*(hFile: HANDLE): WINBOOL
       {.stdcall, dynlib: "kernel32", importc: "CancelIo".}

  proc disconnectNamedPipe*(hPipe: HANDLE): WINBOOL
       {.stdcall, dynlib: "kernel32", importc: "DisconnectNamedPipe".}

  proc setNamedPipeHandleState*(hPipe: HANDLE, lpMode, lpMaxCollectionCount,
                                lpCollectDataTimeout: ptr DWORD): WINBOOL
       {.stdcall, dynlib: "kernel32", importc: "SetNamedPipeHandleState".}

  proc resetEvent*(hEvent: HANDLE): WINBOOL
       {.stdcall, dynlib: "kernel32", importc: "ResetEvent".}

else:
  import posix

  const
    TCP_NODELAY* = 1
    IPPROTO_TCP* = 6

when defined(linux):
  proc eventfd*(count: cuint, flags: cint): cint
               {.cdecl, importc: "eventfd", header: "<sys/eventfd.h>".}
