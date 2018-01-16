//! A worker pool collectively handling a set of connections.
//!
//! This crate is written for the use-case where a server is listening for connections, and wants
//! to spread the load of handling accepted connections across multiple threads. Specifically, this
//! crate implements a worker pool that shares a single `mio::Poll` instance, and collectively
//! accept new connections and handle events for existing ones.
//!
//! Users will want to start with the `PoolBuilder` struct, which allows creating a new pool from
//! anything that can act as a `Listener` (basically, anything that can be polled and accept new
//! connections that can themselves be polled; e.g., `mio::net::TcpListener`).
//!
//! # Examples
//!
//! ```
//! # extern crate mio;
//! # extern crate mio_pool;
//! # use mio_pool::PoolBuilder;
//! # fn main() {
//! use std::io::prelude::*;
//!
//! let addr = "127.0.0.1:0".parse().unwrap();
//! let server = mio::net::TcpListener::bind(&addr).unwrap();
//! let addr = server.local_addr().unwrap();
//! let pool = PoolBuilder::from(server).unwrap();
//! let h = pool.with_state(Vec::new()).and_return(|v| v)
//!     .run(1 /* # workers */, |c: &mut mio::net::TcpStream, s: &mut Vec<u8>| {
//!         // new data is available on the connection `c`!
//!         let mut buf = [0u8; 1024];
//!
//!         // let's just echo back what we read
//!         let n = c.read(&mut buf)?;
//!         if n == 0 {
//!             return Ok(true);
//!         }
//!         c.write_all(&buf[..n])?;
//!
//!         // keep some internal state
//!         s.extend(&buf[..n]);
//!
//!         // assume there could be more data
//!         Ok(false)
//!     });
//!
//! // new clients can now connect on `addr`
//! use std::net::TcpStream;
//! let mut c = TcpStream::connect(&addr).unwrap();
//! c.write_all(b"hello world").unwrap();
//! let mut buf = [0u8; 1024];
//! let n = c.read(&mut buf).unwrap();
//! assert_eq!(&buf[..n], b"hello world");
//!
//! // we can terminate the pool at any time
//! let results = h.terminate();
//! // results here contains the final state of each worker in the pool.
//! // that is, the final value in each `s` passed to the closure in `run`.
//! let result = results.into_iter().next().unwrap();
//! assert_eq!(&result.unwrap(), b"hello world");
//! # }
//! ```
#![deny(missing_docs)]

extern crate mio;
extern crate slab;

use std::io;
use std::thread;
use std::sync::{atomic, Arc};
use std::os::unix::io::AsRawFd;

pub(crate) const NO_EXIT: usize = 0;
pub(crate) const EXIT_IMMEDIATE: usize = 1;
pub(crate) const EXIT_EVENTUALLY: usize = 2;

pub(crate) mod poll;
mod builder;
pub use builder::PoolBuilder;
pub(crate) mod worker;

/// Types that implement `Listener` are mio-pollable, and can accept new connections that are
/// themselves mio-pollable.
pub trait Listener: AsRawFd + Sync + Send {
    /// The type of connections yielded by `accept`.
    type Connection: AsRawFd + Send;

    /// Accept a new connection.
    ///
    /// This method will only be called when `mio::Ready::readable` is raised for the `Listener` by
    /// a `poll`.
    fn accept(&self) -> io::Result<Self::Connection>;
}

impl Listener for mio::net::TcpListener {
    type Connection = mio::net::TcpStream;
    fn accept(&self) -> io::Result<Self::Connection> {
        self.accept().map(|(c, _)| c)
    }
}

/// A handle to a currently executing mio pool.
///
/// This handle can be used to terminate the running pool, and to wait for its completion.
/// See `PoolHandle::terminate` and `PoolHandle::wait` for details.
pub struct PoolHandle<R> {
    threads: Vec<thread::JoinHandle<R>>,
    exit: Arc<atomic::AtomicUsize>,
}

impl<R> PoolHandle<R> {
    /// Tell all running workers to terminate, and then wait for their completion.
    ///
    /// Note that this will *not* wait for existing connections to terminate, but termination may
    /// be delayed until the next time each worker is idle.
    pub fn terminate(self) -> Vec<thread::Result<R>> {
        self.exit.store(EXIT_IMMEDIATE, atomic::Ordering::SeqCst);
        self.wait()
    }

    /// Stop accepting connections and wait for existing connections to complete.
    ///
    /// This method will tell worker threads not to accept new connetions, and to exit once all
    /// current connections have been closed.
    ///
    /// Note that this method will *not* immediately drop the Listener, so new clients that try to
    /// connect will hang (i.e., not get a connection refused) until the workers have all exited.
    pub fn finish(self) -> Vec<thread::Result<R>> {
        self.exit.store(EXIT_EVENTUALLY, atomic::Ordering::SeqCst);
        self.wait()
    }

    /// Wait for all running workers to terminate.
    ///
    /// This method will *not* tell worker threads to exit, and so will only return once when all
    /// worker threads have crashed (which should not happen in general). Users may instead want to
    /// use `PoolHandle::terminate`.
    pub fn wait(self) -> Vec<thread::Result<R>> {
        self.threads.into_iter().map(|jh| jh.join()).collect()
    }
}
