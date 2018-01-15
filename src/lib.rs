#![feature(nll)]

extern crate mio;
extern crate slab;

use std::io;
use std::thread;
use std::time::Duration;
use std::sync::{atomic, Arc, Mutex};
use mio::{PollOpt, Ready, Token};
use slab::Slab;

pub struct Pool<A>
where
    A: Listener,
{
    truth: Arc<Mutex<PoolContext<A>>>,
    epoch: Arc<atomic::AtomicUsize>,
    exit: Arc<atomic::AtomicBool>,
    poll: Arc<mio::Poll>,
}

struct PoolContext<A>
where
    A: Listener,
{
    acceptor: Arc<A>,
    token_to_conn: Slab<Arc<A::Connection>>,
}

impl<A> Clone for PoolContext<A>
where
    A: Listener,
{
    fn clone(&self) -> Self {
        PoolContext {
            acceptor: Arc::clone(&self.acceptor),
            token_to_conn: self.token_to_conn.clone(),
        }
    }
}

pub struct PoolHandle<R> {
    threads: Vec<thread::JoinHandle<R>>,
    exit: Arc<atomic::AtomicBool>,
}

pub trait Listener: mio::Evented + Sync + Send {
    type Connection: mio::Evented + Sync + Send;

    fn accept(&self) -> io::Result<Self::Connection>;
}

impl Listener for mio::net::TcpListener {
    type Connection = mio::net::TcpStream;
    fn accept(&self) -> io::Result<Self::Connection> {
        self.accept().map(|(c, _)| c)
    }
}

impl<A> Pool<A>
where
    A: 'static + Listener,
{
    pub fn new(acceptor: A) -> io::Result<Self> {
        let poll = mio::Poll::new()?;
        poll.register(
            &acceptor,
            Token(0),
            Ready::readable(),
            PollOpt::level() | PollOpt::oneshot(),
        )?;

        Ok(Pool {
            truth: Arc::new(Mutex::new(PoolContext {
                acceptor: Arc::new(acceptor),
                token_to_conn: Slab::new(),
            })),
            epoch: Arc::new(atomic::AtomicUsize::new(1)),
            poll: Arc::new(poll),
            exit: Arc::new(atomic::AtomicBool::new(false)),
        })
    }

    pub fn run<F, R>(self, workers: usize, on_ready: F) -> PoolHandle<R>
    where
        F: Fn(&A::Connection, &mut R) -> io::Result<bool> + 'static + Send + Sync,
        R: 'static + Default + Send,
    {
        let on_ready = Arc::new(on_ready);
        let wrkrs: Vec<_> = (0..workers)
            .map(|_| worker_main(&self, on_ready.clone()))
            .collect();
        PoolHandle {
            threads: wrkrs,
            exit: self.exit,
        }
    }
}

impl<R> PoolHandle<R> {
    pub fn wait(self) -> Vec<thread::Result<R>> {
        self.exit.store(true, atomic::Ordering::SeqCst);
        self.threads.into_iter().map(|jh| jh.join()).collect()
    }
}

fn worker_main<A, F, R>(pool: &Pool<A>, on_ready: Arc<F>) -> thread::JoinHandle<R>
where
    A: 'static + Listener,
    F: Fn(&A::Connection, &mut R) -> io::Result<bool> + 'static + Send + Sync,
    R: 'static + Default + Send,
{
    let mut cache_epoch;
    let truth = Arc::clone(&pool.truth);
    let mut cache = {
        let truth = pool.truth.lock().unwrap();
        cache_epoch = pool.epoch.load(atomic::Ordering::Acquire);
        truth.clone()
    };
    let poll = Arc::clone(&pool.poll);
    let epoch = Arc::clone(&pool.epoch);
    let exit = Arc::clone(&pool.exit);

    thread::spawn(move || {
        let mut worker_result = R::default();
        let mut events = mio::Events::with_capacity(1);
        while !exit.load(atomic::Ordering::SeqCst) {
            if let Err(e) = poll.poll(&mut events, Some(Duration::from_secs(1))) {
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
            let cur_epoch = epoch.load(atomic::Ordering::Acquire);
            if cur_epoch != cache_epoch {
                let truth = truth.lock().unwrap();
                cache_epoch = epoch.load(atomic::Ordering::Acquire);
                cache = truth.clone();
            }

            for e in &events {
                let Token(t) = e.token();

                if t == 0 {
                    let mut truth = truth.lock().unwrap();

                    // let's assume we accept at least one connection
                    cache_epoch = 1 + epoch.fetch_add(1, atomic::Ordering::AcqRel);

                    while let Ok(c) = cache.acceptor.accept() {
                        let c = Arc::new(c);

                        // pick a token for this new connection
                        let token = truth.token_to_conn.insert(Arc::clone(&c));

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
                } else {
                    match cache.token_to_conn.get(t - 1) {
                        Some(c) => {
                            let r = on_ready(&**c, &mut worker_result);
                            let mut closed = false;
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

                            if closed {
                                // connection was dropped; update truth
                                let mut truth = truth.lock().unwrap();
                                cache_epoch = 1 + epoch.fetch_add(1, atomic::Ordering::AcqRel);
                                truth.token_to_conn.remove(t);
                                // also update our cache while we're at it
                                cache = truth.clone(); // yay nll
                            } else {
                                // need to re-register so we get later events
                                poll.reregister(
                                    &**c,
                                    Token(t),
                                    Ready::readable(),
                                    PollOpt::level() | PollOpt::oneshot(),
                                ).unwrap()
                            }
                        }
                        None => {
                            unreachable!();
                        }
                    }
                }
            }
        }

        worker_result
    })
}
