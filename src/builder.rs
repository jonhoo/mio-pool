use std::io;
use std::sync::{atomic, Arc, Mutex};
use slab::Slab;
use std::os::unix::io::AsRawFd;

use {Listener, PoolHandle, NO_EXIT};
use worker::worker_main;
use poll::{Poll, Token};

/// Used to configure a mio pool before launching it.
///
/// Users will want to call `PoolBuilder::from` to start a new pool from a `Listener`, and then
/// `PoolBuilder::run` with an `on_ready` callback to begin accepting and handling connections.
///
/// At a high level, the resulting pool will consist of `workers` worker threads that each accept
/// new connections and handle incoming requests. Every time a connection has available data,
/// `on_ready` will be called by one of the workers. A connection will stay in the pool until
/// `on_ready` returns an error, or `Ok(true)` to indicate EOF. Unless the pool is started with
/// `run_stateless`, `on_ready` is given mutable access to worker-local state each time it is
/// invoked. This can be useful for maintaining caches and the like.
///
/// The behavior of the pool can be customized in a couple of ways, most importantly through
/// `PoolBuilder::with_finalizer` and `PoolBuilder::and_return`. The former runs every accepted
/// connection through a function before adding it to the connection pool. The latter allows for
/// returning some part of the worker state after the pool has been terminated (e.g., for
/// statistics summaries). See the relevant method documentation for more details.
///
/// # Examples
///
/// ```
/// # extern crate mio;
/// # extern crate mio_pool;
/// # use mio_pool::PoolBuilder;
/// # fn main() {
/// let addr = "127.0.0.1:0".parse().unwrap();
/// let server = mio::net::TcpListener::bind(&addr).unwrap();
/// let pool = PoolBuilder::from(server).unwrap();
/// let h = pool.run(1 /* # workers */, |c: &mut mio::net::TcpStream, s: &mut ()| {
///     use std::io::prelude::*;
///     let mut buf = [0u8; 1024];
///     let n = c.read(&mut buf)?;
///     if n == 0 {
///         return Ok(true);
///     }
///     c.write_all(&buf[..n])?;
///     Ok(false)
/// });
///
/// // ...
/// // during this period, new clients can connect
/// // ...
///
/// let results = h.terminate();
/// // results here contains the final state of each worker in the pool.
/// // that is, the final value in each `s` passed to the closure in `run`.
/// let result = results.into_iter().next().unwrap();
/// assert_eq!(result.unwrap(), ());
/// # }
/// ```
pub struct PoolBuilder<L, C, S, R>
where
    L: Listener,
{
    pub(super) listener: Arc<L>,
    pub(super) epoch: Arc<atomic::AtomicUsize>,
    pub(super) exit: Arc<atomic::AtomicUsize>,
    pub(super) poll: Arc<Poll>,

    pub(super) initial: S,
    pub(super) adapter: Arc<Fn(L::Connection) -> C + 'static + Send + Sync>,
    pub(super) finalizer: Arc<Fn(S) -> R + Send + Sync + 'static>,
    thread_name_prefix: String,
}

impl<L> PoolBuilder<L, L::Connection, (), ()>
where
    L: Listener,
{
    /// Prepare a new pool from the given listener.
    ///
    /// The pool will monitor the listener for new connections, and distribute the task of
    /// accepting them, and handling requests to accepted connections, among a pool of threads.
    pub fn from(listener: L) -> io::Result<Self> {
        let poll = Poll::new()?;
        poll.register(&listener, Token(0))?;

        Ok(PoolBuilder {
            listener: Arc::new(listener),
            epoch: Arc::new(atomic::AtomicUsize::new(1)),
            poll: Arc::new(poll),
            exit: Arc::new(atomic::AtomicUsize::new(NO_EXIT)),

            initial: (),
            adapter: Arc::new(|c| c),
            finalizer: Arc::new(|_| ()),

            thread_name_prefix: String::from("pool-"),
        })
    }
}

impl<L, C> PoolBuilder<L, C, (), ()>
where
    L: Listener,
{
    /// Set the initial state of each worker thread.
    ///
    /// Note that this method will override any finalizer that may have been set!
    pub fn with_state<S>(self, initial: S) -> PoolBuilder<L, C, S, ()>
    where
        S: Clone + Send + 'static,
    {
        PoolBuilder {
            listener: self.listener,
            epoch: self.epoch,
            exit: self.exit,
            poll: self.poll,
            adapter: self.adapter,

            initial,
            finalizer: Arc::new(|_| ()),

            thread_name_prefix: String::from("pool-"),
        }
    }
}

impl<L, C, S, R> PoolBuilder<L, C, S, R>
where
    L: Listener,
{
    /// Set the thread name prefix to use for the worker threads in this pool.
    pub fn set_thread_name_prefix(mut self, prefix: &str) -> Self {
        self.thread_name_prefix = prefix.to_string();
        self
    }

    /// Run accepted connections through an adapter before adding them to the pool of connections.
    ///
    /// This allows users to wrap something akin to an `TcpStream` into a more sophisticated
    /// connection type (e.g., by adding buffering).
    pub fn with_adapter<NA, NC>(self, adapter: NA) -> PoolBuilder<L, NC, S, R>
    where
        NA: Fn(L::Connection) -> NC + 'static + Send + Sync,
        NC: AsRawFd + Send + 'static,
    {
        PoolBuilder {
            listener: self.listener,
            epoch: self.epoch,
            exit: self.exit,
            poll: self.poll,
            initial: self.initial,
            finalizer: self.finalizer,

            adapter: Arc::new(adapter),

            thread_name_prefix: String::from("pool-"),
        }
    }

    /// Specify that a return value should be derived from the state kept by each worker.
    ///
    /// This return value is gathered up by the `PoolHandle` returned by `run`.
    pub fn and_return<NF, NR>(self, fin: NF) -> PoolBuilder<L, C, S, NR>
    where
        NF: Fn(S) -> NR + Send + Sync + 'static,
        NR: Send + 'static,
    {
        PoolBuilder {
            listener: self.listener,
            epoch: self.epoch,
            exit: self.exit,
            poll: self.poll,
            adapter: self.adapter,
            initial: self.initial,

            finalizer: Arc::new(fin),

            thread_name_prefix: String::from("pool-"),
        }
    }
}

impl<L, C> PoolBuilder<L, C, (), ()>
where
    L: Listener + 'static,
    C: AsRawFd + Send + 'static,
{
    /// Run the pool with a stateless worker callback.
    pub fn run_stateless<E>(self, workers: usize, on_ready: E) -> PoolHandle<()>
    where
        E: Fn(&mut C) -> io::Result<bool> + 'static + Send + Sync,
    {
        self.run(workers, move |c, _| on_ready(c))
    }
}

impl<L, C, S, R> PoolBuilder<L, C, S, R>
where
    L: Listener + 'static,
    C: AsRawFd + Send + 'static,
    S: Clone + Send + 'static,
    R: 'static + Send,
{
    /// Run the pool with a connection adapter, local worker state, and a state finalizer.
    pub fn run<E>(self, workers: usize, on_ready: E) -> PoolHandle<R>
    where
        E: Fn(&mut C, &mut S) -> io::Result<bool> + 'static + Send + Sync,
    {
        let truth = Arc::new(Mutex::new(Slab::new()));
        let on_ready = Arc::new(on_ready);
        let wrkrs: Vec<_> = (0..workers)
            .map(|i| {
                worker_main(
                    &*self.thread_name_prefix,
                    i,
                    &self,
                    Arc::clone(&truth),
                    Arc::clone(&on_ready),
                )
            })
            .collect();
        PoolHandle {
            threads: wrkrs,
            exit: self.exit,
        }
    }
}
