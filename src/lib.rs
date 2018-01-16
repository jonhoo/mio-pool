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
//! let h = pool.and_return(|v| v)
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
use std::marker::PhantomData;
use std::time::Duration;
use std::sync::{atomic, Arc, Mutex};
use mio::*;
use slab::Slab;

const NO_EXIT: usize = 0;
const EXIT_IMMEDIATE: usize = 1;
const EXIT_EVENTUALLY: usize = 2;

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
    listener: Arc<L>,
    epoch: Arc<atomic::AtomicUsize>,
    exit: Arc<atomic::AtomicUsize>,
    poll: Arc<Poll>,

    state: PhantomData<S>,
    adapter: Arc<A>,
    finalizer: Arc<F>,
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

fn worker_main<L, A, S, F, C, R, E>(
    _i: usize,
    pool: &PoolBuilder<L, A, S, F>,
    truth: Arc<Mutex<Slab<Arc<OneshotConnection<C>>>>>,
    on_ready: Arc<E>,
) -> thread::JoinHandle<R>
where
    A: Fn(L::Connection) -> C + 'static + Send + Sync,
    C: Evented + Send + 'static,
    L: 'static + Listener,
    E: Fn(&mut C, &mut S) -> io::Result<bool> + 'static + Send + Sync,
    S: Default,
    F: Fn(S) -> R + Send + Sync + 'static,
    R: 'static + Send,
{
    let mut cache_epoch;
    let listener = Arc::clone(&pool.listener);
    let mut cache = {
        let truth = truth.lock().unwrap();
        cache_epoch = pool.epoch.load(atomic::Ordering::SeqCst);
        truth.clone()
    };
    let poll = Arc::clone(&pool.poll);
    let epoch = Arc::clone(&pool.epoch);
    let exit = Arc::clone(&pool.exit);
    let adapter = Arc::clone(&pool.adapter);
    let finalizer = Arc::clone(&pool.finalizer);

    thread::spawn(move || {
        let mut worker_result = S::default();
        let mut events = Events::with_capacity(1);
        let mut status = NO_EXIT;
        while status != EXIT_IMMEDIATE {
            if let Err(e) = poll.poll(&mut events, Some(Duration::from_millis(200))) {
                if e.kind() == io::ErrorKind::Interrupted {
                    // spurious wakeup
                    continue;
                } else if e.kind() == io::ErrorKind::TimedOut {
                    // *should* be handled by mio and return Ok() with no events
                    continue;
                } else {
                    panic!("{}", e);
                }
            }

            status = exit.load(atomic::Ordering::SeqCst);

            // check our epoch -- we may have a stale truth.
            // this is important: consider the case where an old connection has been dropped,
            // retiring token t, and then a new connection has been accepted, and has been assigned
            // the same token t. we (unaware of this), then get notified about an event for token
            // t. we *cannot* re-use the old connection for t, but must instead somehow realize
            // that there is a new connection for t, and that we need to update our truth.
            //
            // keep in mind that since we're using EPOLL_ONESHOT, no other thread can currently be
            // operating on the token we get from poll(), and so as long as we see any changes from
            // *before* we polled (which is guaranteed by the atomic read), we know that the truth
            // we end up reading if the epoch has changed must at least contain any changes to t.
            let cur_epoch = epoch.load(atomic::Ordering::SeqCst);
            if cur_epoch != cache_epoch || (status == EXIT_EVENTUALLY && cache.is_empty()) {
                let truth = truth.lock().unwrap();
                cache_epoch = epoch.load(atomic::Ordering::SeqCst);
                cache = truth.clone();

                if status == EXIT_EVENTUALLY && truth.is_empty() {
                    break;
                }
            }

            for e in &events {
                let Token(t) = e.token();

                if t == 0 {
                    if status == NO_EXIT {
                        let mut truth = truth.lock().unwrap();

                        // let's assume we accept at least one connection
                        cache_epoch = 1 + epoch.fetch_add(1, atomic::Ordering::SeqCst);

                        while let Ok(c) = listener.accept() {
                            let c = adapter(c);
                            let c = Arc::new(OneshotConnection::new(c));

                            // pick a token for this new connection
                            let token = truth.insert(Arc::clone(&c));

                            // it's fine if some other thread gets notified about this, because they'll
                            // see the updated epoch, and then block trying to update their truth.
                            poll.register(
                                &*c,
                                Token(token + 1),
                                Ready::readable(),
                                PollOpt::level() | PollOpt::oneshot(),
                            ).unwrap();

                            // also update our cache while we're at it
                            cache = truth.clone();
                        }

                        // need to re-register listening thread
                        poll.reregister(
                            &*listener,
                            Token(0),
                            Ready::readable(),
                            PollOpt::level() | PollOpt::oneshot(),
                        ).unwrap()
                    }
                } else {
                    let t = t - 1;
                    let mut closed = false;
                    if let Some(c) = cache.get(t) {
                        let r = {
                            let c = unsafe { c.mut_given_epoll_oneshot() };
                            on_ready(c, &mut worker_result)
                        };
                        if let Ok(true) = r {
                            closed = true;
                        }
                        if let Err(e) = r {
                            match e.kind() {
                                io::ErrorKind::BrokenPipe
                                | io::ErrorKind::NotConnected
                                | io::ErrorKind::UnexpectedEof
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::ConnectionReset => {
                                    closed = true;
                                }
                                _ => {}
                            }
                        }

                        if !closed {
                            // need to re-register so we get later events
                            poll.reregister(
                                &**c,
                                Token(t + 1),
                                Ready::readable(),
                                PollOpt::level() | PollOpt::oneshot(),
                            ).unwrap()
                        }
                    } else {
                        // mio is waking us up on a connection that has been dropped?
                        // this shouldn't happen, but does....
                        //unreachable!();
                    }

                    if closed {
                        // connection was dropped; update truth
                        let mut truth = truth.lock().unwrap();
                        cache_epoch = 1 + epoch.fetch_add(1, atomic::Ordering::SeqCst);
                        truth.remove(t);
                        // also update our cache while we're at it
                        cache = truth.clone();
                    }
                }
            }
        }

        finalizer(worker_result)
    })
}

/// Types that implement `Listener` are mio-pollable, and can accept new connections that are
/// themselves mio-pollable.
pub trait Listener: Evented + Sync + Send {
    /// The type of connections yielded by `accept`.
    type Connection: Evented + Send;

    /// Accept a new connection.
    ///
    /// This method will only be called when `mio::Ready::readable` is raised for the `Listener` by
    /// a `poll`.
    fn accept(&self) -> io::Result<Self::Connection>;
}

impl Listener for net::TcpListener {
    type Connection = net::TcpStream;
    fn accept(&self) -> io::Result<Self::Connection> {
        self.accept().map(|(c, _)| c)
    }
}

/// This is a bit of a hack, but allows mutable access to the underlying channels.
///
/// Specifically, since EPOLL_ONESHOT (should) guarantee that only one thread is woken up when
/// there's an event on a given socket, and we ensure that no thread touches a connection after it
/// re-registers it, we know that a thread that is woken up for a given connection has exclusive
/// access to that connection. This means that we are okay to hand out an `&mut C` to the
/// `on_ready` function, since it cannot leak that mutable reference anywhere.
struct OneshotConnection<C>(Arc<C>, *mut C);

impl<C> Evented for OneshotConnection<C>
where
    C: Evented,
{
    fn register(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.0.register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.0.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.0.deregister(poll)
    }
}

impl<C> OneshotConnection<C> {
    pub fn new(conn: C) -> Self {
        let mut conn = Arc::new(conn);
        let c = { Arc::get_mut(&mut conn).unwrap() as *mut _ };
        OneshotConnection(conn, c)
    }

    unsafe fn mut_given_epoll_oneshot<'a>(&'a self) -> &'a mut C {
        &mut *self.1
    }
}

unsafe impl<C> Send for OneshotConnection<C>
where
    C: Send,
{
}

// This is *only* okay because no two threads should ever be referencing a given connection at the
// same time.
unsafe impl<C> Sync for OneshotConnection<C>
where
    C: Send,
{
}
