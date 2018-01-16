# mio-pool

[![Crates.io](https://img.shields.io/crates/v/mio-pool.svg)](https://crates.io/crates/mio-pool)
[![Documentation](https://docs.rs/mio-pool/badge.svg)](https://docs.rs/mio-pool/)
[![Build Status](https://travis-ci.org/jonhoo/mio-pool.svg?branch=master)](https://travis-ci.org/jonhoo/mio-pool)

A worker pool collectively handling a set of connections.

This crate is written for the use-case where a server is listening for connections, and wants
to spread the load of handling accepted connections across multiple threads. Specifically, this
crate implements a worker pool that shares a single `mio::Poll` instance, and collectively
accept new connections and handle events for existing ones.

Users will want to start with the `PoolBuilder` struct, which allows creating a new pool from
anything that can act as a `Listener` (basically, anything that can be polled and accept new
connections that can themselves be polled; e.g., `mio::net::TcpListener`).

## Examples

```rust
use std::io::prelude::*;

let addr = "127.0.0.1:0".parse().unwrap();
let server = mio::net::TcpListener::bind(&addr).unwrap();
let addr = server.local_addr().unwrap();
let pool = PoolBuilder::from(server).unwrap();
let h = pool.with_state(Vec::new()).and_return(|v| v)
    .run(1 /* # workers */, |c: &mut mio::net::TcpStream, s: &mut Vec<u8>| {
        // new data is available on the connection `c`!
        let mut buf = [0u8; 1024];

        // let's just echo back what we read
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }
        c.write_all(&buf[..n])?;

        // keep some internal state
        s.extend(&buf[..n]);

        // assume there could be more data
        Ok(false)
    });

// new clients can now connect on `addr`
use std::net::TcpStream;
let mut c = TcpStream::connect(&addr).unwrap();
c.write_all(b"hello world").unwrap();
let mut buf = [0u8; 1024];
let n = c.read(&mut buf).unwrap();
assert_eq!(&buf[..n], b"hello world");

// we can terminate the pool at any time
let results = h.terminate();
// results here contains the final state of each worker in the pool.
// that is, the final value in each `s` passed to the closure in `run`.
let result = results.into_iter().next().unwrap();
assert_eq!(&result.unwrap(), b"hello world");
```
