packageName   = "chronos"
version       = "2.2.1"
author        = "Status Research & Development GmbH"
description   = "Chronos"
license       = "Apache License 2.0 or MIT"
skipDirs      = @["tests"]

### Dependencies

requires "nim > 0.18.0"

task test, "Run all tests":
  for tfile in @[
      "testsync",
      "testsoon",
      "testtime",
      "testfut",
      "testsignal",
      "testaddress",
      "testdatagram",
      "teststream",
      "testserver",
      "testbugs",
    ]:
    for cmd in @[
        "nim c -r -d:useSysAssert -d:useGcAssert tests/" & tfile,
        "nim c -r tests/" & tfile,
        #"nim c -r --gc:markAndSweep tests/" & tfile,
        "nim c -r -d:release tests/" & tfile
      ]:
      echo "\n" & cmd
      exec cmd
      rmFile("tests/" & tfile.toExe())

