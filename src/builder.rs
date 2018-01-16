use std::io;
use std::marker::PhantomData;
use std::sync::{atomic, Arc, Mutex};
use mio::*;
use slab::Slab;

use {Listener, PoolHandle, NO_EXIT};
use worker::worker_main;

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
/// The many definitions of `PoolBuilder::run` can seem a bit daunting at first. In practice, you
/// should likely not need to worry about them; any way you construct the `PoolBuilder` *should*
/// give you a builder that you can then call `run` on. The many implementations are there to
/// ensure that you still get static dispatch for your pool no matter what combination of adapter
/// and finalizer you choose.
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
pub struct PoolBuilder<L, A = (), S = (), F = ()> {
    pub(super) listener: Arc<L>,
    pub(super) epoch: Arc<atomic::AtomicUsize>,
    pub(super) exit: Arc<atomic::AtomicUsize>,
    pub(super) poll: Arc<Poll>,

    pub(super) state: PhantomData<S>,
    pub(super) adapter: Arc<A>,
    pub(super) finalizer: Arc<F>,
}

impl<L> PoolBuilder<L, (), (), ()>
where
    L: Listener,
{
    /// Prepare a new pool from the given listener.
    ///
    /// The pool will monitor the listener for new connections, and distribute the task of
    /// accepting them, and handling requests to accepted connections, among a pool of threads.
    pub fn from(listener: L) -> io::Result<Self> {
        let poll = Poll::new()?;
        poll.register(
            &listener,
            Token(0),
            Ready::readable(),
            PollOpt::level() | PollOpt::oneshot(),
        )?;

        Ok(PoolBuilder {
            listener: Arc::new(listener),
            epoch: Arc::new(atomic::AtomicUsize::new(1)),
            poll: Arc::new(poll),
            exit: Arc::new(atomic::AtomicUsize::new(NO_EXIT)),

            state: PhantomData,
            adapter: Arc::new(()),
            finalizer: Arc::new(()),
        })
    }
}

impl<L, A, S, F> PoolBuilder<L, A, S, F>
where
    L: Listener,
{
    /// Run accepted connections through an adapter before adding them to the pool of connections.
    ///
    /// This allows users to wrap something akin to an `TcpStream` into a more sophisticated
    /// connection type (e.g., by adding buffering).
    pub fn with_adapter<NA, C>(self, adapter: NA) -> PoolBuilder<L, NA, S, F>
    where
        NA: Fn(L::Connection) -> C + 'static + Send + Sync,
        C: Evented + Send + 'static,
    {
        PoolBuilder {
            listener: self.listener,
            epoch: self.epoch,
            exit: self.exit,
            poll: self.poll,
            finalizer: self.finalizer,
            state: self.state,

            adapter: Arc::new(adapter),
        }
    }

    /// Specify that a return value should be derived from the state kept by each worker.
    ///
    /// This return value is gathered up by the `PoolHandle` returned by `run`.
    pub fn and_return<NF, NS, R>(self, fin: NF) -> PoolBuilder<L, A, NS, NF>
    where
        NS: Default,
        NF: Fn(NS) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        PoolBuilder {
            listener: self.listener,
            epoch: self.epoch,
            exit: self.exit,
            poll: self.poll,
            adapter: self.adapter,

            state: PhantomData,
            finalizer: Arc::new(fin),
        }
    }
}

impl<L> PoolBuilder<L, (), (), ()>
where
    L: Listener + 'static,
{
    /// Run the pool with a stateless worker callback.
    pub fn run_stateless<E>(self, workers: usize, on_ready: E) -> PoolHandle<()>
    where
        E: Fn(&mut L::Connection) -> io::Result<bool> + 'static + Send + Sync,
    {
        self.with_adapter(|c| c)
            .and_return(|_: ()| ())
            .run(workers, move |c, _| on_ready(c))
    }
}

impl<L, A, C> PoolBuilder<L, A, (), ()>
where
    L: Listener + 'static,
    A: Fn(L::Connection) -> C + 'static + Send + Sync,
    C: Evented + Send + 'static,
{
    /// Run the pool with a connection adapter and a stateless worker callback.
    pub fn run_stateless<E>(self, workers: usize, on_ready: E) -> PoolHandle<()>
    where
        E: Fn(&mut C) -> io::Result<bool> + 'static + Send + Sync,
    {
        self.and_return(|_: ()| ())
            .run(workers, move |c, _| on_ready(c))
    }
}

impl<L, A, S, C> PoolBuilder<L, A, S, ()>
where
    L: Listener + 'static,
    A: Fn(L::Connection) -> C + 'static + Send + Sync,
    S: Default,
    C: Evented + Send + 'static,
{
    /// Run the pool with a connection adapter.
    pub fn run<E>(self, workers: usize, on_ready: E) -> PoolHandle<()>
    where
        E: Fn(&mut C, &mut S) -> io::Result<bool> + 'static + Send + Sync,
    {
        self.and_return(|_| ()).run(workers, on_ready)
    }
}

impl<L, S> PoolBuilder<L, (), S, ()>
where
    L: Listener + 'static,
    S: Default,
{
    /// Run the pool with local worker state.
    pub fn run<E>(self, workers: usize, on_ready: E) -> PoolHandle<()>
    where
        E: Fn(&mut L::Connection, &mut S) -> io::Result<bool> + 'static + Send + Sync,
    {
        self.and_return(|_| ())
            .with_adapter(|c| c)
            .run(workers, on_ready)
    }
}

impl<L, S, F, R> PoolBuilder<L, (), S, F>
where
    L: Listener + 'static,
    S: Default,
    F: Fn(S) -> R + Send + Sync + 'static,
    R: Send + 'static,
{
    /// Run the pool with local worker state and a state finalizer.
    pub fn run<E>(self, workers: usize, on_ready: E) -> PoolHandle<R>
    where
        E: Fn(&mut L::Connection, &mut S) -> io::Result<bool> + 'static + Send + Sync,
    {
        self.with_adapter(|c| c).run(workers, on_ready)
    }
}

impl<L, A, S, F, C, R> PoolBuilder<L, A, S, F>
where
    L: Listener + 'static,
    A: Fn(L::Connection) -> C + 'static + Send + Sync,
    C: Evented + Send + 'static,
    S: Default,
    F: Fn(S) -> R + Send + Sync + 'static,
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
            .map(|i| worker_main(i, &self, Arc::clone(&truth), Arc::clone(&on_ready)))
            .collect();
        PoolHandle {
            threads: wrkrs,
            exit: self.exit,
        }
    }
}
