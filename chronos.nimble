packageName   = "chronos"
version       = "2.2.9"
author        = "Status Research & Development GmbH"
description   = "Chronos"
license       = "Apache License 2.0 or MIT"
skipDirs      = @["tests"]

### Dependencies

requires "nim > 0.18.0"

task test, "Run all tests":
  var commands = [
    "nim c -r -d:useSysAssert -d:useGcAssert tests/testall",
    "nim c -r tests/testall",
    "nim c -r -d:release tests/testall"
  ]
  echo "\n" & commands[0]
  exec commands[0]
  echo "\n" & commands[1]
  exec commands[1]
  echo "\n" & commands[2]
  exec commands[2]

task channel_helgrind, "Run the channel implementation through helgrind to detect threading or lock errors":
  var commands = [
    "nim c -d:useMalloc --threads:on tests/testchannels.nim",
    "valgrind --tool=helgrind build/achannel"
  ]
  echo "\n" & commands[0]
  exec commands[0]
  echo "\n" & commands[1]
  exec commands[1]
