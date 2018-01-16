use std::io;
use std::thread;
use std::time::Duration;
use std::sync::{atomic, Arc, Mutex};
use mio::*;
use slab::Slab;

use {EXIT_EVENTUALLY, EXIT_IMMEDIATE, NO_EXIT};
use {Listener, PoolBuilder};

pub(crate) fn worker_main<L, C, S, R, E>(
    _i: usize,
    pool: &PoolBuilder<L, C, S, R>,
    truth: Arc<Mutex<Slab<Arc<OneshotConnection<C>>>>>,
    on_ready: Arc<E>,
) -> thread::JoinHandle<R>
where
    L: 'static + Listener,
    C: Evented + Send + 'static,
    S: Clone + Send + 'static,
    R: 'static + Send,
    E: Fn(&mut C, &mut S) -> io::Result<bool> + 'static + Send + Sync,
{
    let mut cache_epoch;
    let listener = Arc::clone(&pool.listener);
    let mut cache = {
        let truth = truth.lock().unwrap();
        cache_epoch = pool.epoch.load(atomic::Ordering::SeqCst);
        truth.clone()
    };
    let mut state = pool.initial.clone();
    let poll = Arc::clone(&pool.poll);
    let epoch = Arc::clone(&pool.epoch);
    let exit = Arc::clone(&pool.exit);
    let adapter = Arc::clone(&pool.adapter);
    let finalizer = Arc::clone(&pool.finalizer);

    thread::spawn(move || {
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
                            on_ready(c, &mut state)
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

        finalizer(state)
    })
}

/// This is a bit of a hack, but allows mutable access to the underlying channels.
///
/// Specifically, since EPOLL_ONESHOT (should) guarantee that only one thread is woken up when
/// there's an event on a given socket, and we ensure that no thread touches a connection after it
/// re-registers it, we know that a thread that is woken up for a given connection has exclusive
/// access to that connection. This means that we are okay to hand out an `&mut C` to the
/// `on_ready` function, since it cannot leak that mutable reference anywhere.
pub(super) struct OneshotConnection<C>(Arc<C>, *mut C);

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
