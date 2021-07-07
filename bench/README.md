# Benchmarks

## Plaintext

Client results:

> Note: the client does pipelining and
  httpbeast does not support it

```
$ nim c -r -d:release bench/plaintext/asyncdispatch2server.nim
$ nim c -r -d:release bench/plaintext/asyncnetserver.nim
$ nim c -r -d:release bench/plaintext/client.nim
asyncdispatch2
Requests/sec: 205310.18
asyncnet
Requests/sec: 194804.60
```

[Wrk](https://github.com/wg/wrk) results:

```
# asyncdipatch2
$ wrk -c 256 -t 3 http://127.0.0.1:8885/
Running 10s test @ http://127.0.0.1:8885/
  3 threads and 256 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     3.59ms  492.01us   8.11ms   92.41%
    Req/Sec    23.68k   593.09    29.46k    79.67%
  707085 requests in 10.02s, 36.41MB read
Requests/sec:  70566.86
Transfer/sec:      3.63MB

# asyncnet
$ wrk -c 256 -t 3 http://127.0.0.1:8886/
Running 10s test @ http://127.0.0.1:8886/
  3 threads and 256 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     5.80ms    7.54ms 264.18ms   99.51%
    Req/Sec    15.07k   676.57    17.67k    78.00%
  449947 requests in 10.02s, 23.17MB read
Requests/sec:  44916.34
Transfer/sec:      2.31MB

# httpbeast
$ wrk -c 256 -t 3 http://127.0.0.1:8887/
Running 10s test @ http://127.0.0.1:8887/
  3 threads and 256 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.50ms  170.96us   7.41ms   92.11%
    Req/Sec    33.99k     1.41k   52.30k    90.67%
  1014932 requests in 10.03s, 52.27MB read
Requests/sec: 101185.22
Transfer/sec:      5.21MB
```
