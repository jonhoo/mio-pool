extern crate nix;

use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::time;
use std::io;
use self::nix::sys::epoll;

/// Polls for readiness events on all registered file descriptors.
///
/// `Poll` allows a program to monitor a large number of file descriptors, waiting until one or
/// more become "ready" for some class of operations; e.g. reading and writing. A file descriptor
/// is considered ready if it is possible to immediately perform a corresponding operation; e.g.
/// [`read`].
///
/// These `Poll` instances are optimized for a worker pool use-case, and so they are all
/// oneshot, edge-triggered, and only support "ready to read".
///
/// To use `Poll`, a file descriptor must first be registered with the `Poll` instance using the
/// [`register`] method. A `Token` is also passed to the [`register`] function, and that same
/// `Token` is returned when the given file descriptor is ready.
///
/// [`read`]: tcp/struct.TcpStream.html#method.read
/// [`register`]: #method.register
/// [`reregister`]: #method.reregister
///
/// # Examples
///
/// A basic example -- establishing a `TcpStream` connection.
///
/// ```no_run
/// # extern crate mio;
/// # extern crate mio_pool;
/// # use std::error::Error;
/// # fn try_main() -> Result<(), Box<Error>> {
/// use mio_pool::poll::{Events, Poll, Token};
/// use mio::net::TcpStream;
///
/// use std::net::{TcpListener, SocketAddr};
///
/// // Bind a server socket to connect to.
/// let addr: SocketAddr = "127.0.0.1:0".parse()?;
/// let server = TcpListener::bind(&addr)?;
///
/// // Construct a new `Poll` handle as well as the `Events` we'll store into
/// let poll = Poll::new()?;
/// let mut events = Events::with_capacity(1024);
///
/// // Connect the stream
/// let stream = TcpStream::connect(&server.local_addr()?)?;
///
/// // Register the stream with `Poll`
/// poll.register(&stream, Token(0))?;
///
/// // Wait for the socket to become ready. This has to happens in a loop to
/// // handle spurious wakeups.
/// loop {
///     poll.poll(&mut events, None)?;
///
///     for Token(t) in &events {
///         if t == 0 {
///             // The socket connected (probably; it could be a spurious wakeup)
///             return Ok(());
///         }
///     }
/// }
/// #     Ok(())
/// # }
/// #
/// # fn main() {
/// #     try_main().unwrap();
/// # }
/// ```
///
/// # Exclusive access
///
/// Since this `Poll` implementation is optimized for worker-pool style use-cases, all file
/// descriptors are registered using `EPOLL_ONESHOT`. This means that once an event has been issued
/// for a given descriptor, not more events will be issued for that descriptor until it has been
/// re-registered using [`reregister`].
pub struct Poll(RawFd);

/// Associates an event with a file descriptor.
///
/// `Token` is a wrapper around `usize`, and is used as an argument to
/// [`Poll::register`] and [`Poll::reregister`].
///
/// See [`Poll`] for more documentation on polling. You will likely want to use something like
/// [`slab`] for creating and managing these.
///
/// [`Poll`]: struct.Poll.html
/// [`Poll::register`]: struct.Poll.html#method.register
/// [`Poll::reregister`]: struct.Poll.html#method.reregister
/// [`slab`]: https://crates.io/crates/slab
pub struct Token(pub usize);

/// A collection of readiness events.
///
/// `Events` is passed as an argument to [`Poll::poll`], and provides any readiness events received
/// since the last poll. Usually, a single `Events` instance is created at the same time as a
/// [`Poll`] and reused on each call to [`Poll::poll`].
///
/// See [`Poll`] for more documentation on polling.
///
/// [`Poll::poll`]: struct.Poll.html#method.poll
/// [`Poll`]: struct.Poll.html
pub struct Events {
    all: Vec<epoll::EpollEvent>,

    /// How many of the events in `.all` are filled with responses to the last `poll()`?
    current: usize,
}

impl Events {
    /// Return a new `Events` capable of holding up to `capacity` events.
    pub fn with_capacity(capacity: usize) -> Events {
        let mut events = Vec::new();
        events.resize(capacity, epoll::EpollEvent::empty());
        Events {
            all: events,
            current: 0,
        }
    }
}

fn nix_to_io_err(e: nix::Error) -> io::Error {
    match e {
        nix::Error::Sys(errno) => io::Error::from_raw_os_error(errno as i32),
        nix::Error::InvalidPath => io::Error::new(io::ErrorKind::InvalidInput, e),
        nix::Error::InvalidUtf8 => io::Error::new(io::ErrorKind::InvalidInput, e),
        nix::Error::UnsupportedOperation => io::Error::new(io::ErrorKind::Other, e),
    }
}

impl Poll {
    /// Return a new `Poll` handle.
    ///
    /// This function will make a syscall to the operating system to create the system selector. If
    /// this syscall fails, `Poll::new` will return with the error.
    ///
    /// See [struct] level docs for more details.
    ///
    /// [struct]: struct.Poll.html
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::error::Error;
    /// # fn try_main() -> Result<(), Box<Error>> {
    /// use mio_pool::poll::{Poll, Events};
    /// use std::time::Duration;
    ///
    /// let poll = match Poll::new() {
    ///     Ok(poll) => poll,
    ///     Err(e) => panic!("failed to create Poll instance; err={:?}", e),
    /// };
    ///
    /// // Create a structure to receive polled events
    /// let mut events = Events::with_capacity(1024);
    ///
    /// // Wait for events, but none will be received because no `Evented`
    /// // handles have been registered with this `Poll` instance.
    /// let n = poll.poll(&mut events, Some(Duration::from_millis(500)))?;
    /// assert_eq!(n, 0);
    /// #     Ok(())
    /// # }
    /// #
    /// # fn main() {
    /// #     try_main().unwrap();
    /// # }
    /// ```
    pub fn new() -> io::Result<Self> {
        epoll::epoll_create1(epoll::EpollCreateFlags::empty())
            .map(Poll)
            .map_err(nix_to_io_err)
    }

    fn ctl(&self, file: &AsRawFd, t: Token, op: epoll::EpollOp) -> io::Result<()> {
        let mut event = epoll::EpollEvent::new(
            epoll::EpollFlags::EPOLLIN | epoll::EpollFlags::EPOLLONESHOT,
            t.0 as u64,
        );
        epoll::epoll_ctl(self.0, op, file.as_raw_fd(), &mut event).map_err(nix_to_io_err)
    }

    /// Register a file descriptor with this `Poll` instance.
    ///
    /// Once registered, the `Poll` instance monitors the given descriptor for readiness state
    /// changes. When it notices a state change, it will return a readiness event for the handle
    /// the next time [`poll`] is called.
    ///
    /// See the [`struct`] docs for a high level overview.
    ///
    /// `token` is user-defined value that is associated with the given `file`. When [`poll`]
    /// returns an event for `file`, this token is included. This allows the caller to map the
    /// event back to its descriptor. The token associated with a file descriptor can be changed at
    /// any time by calling [`reregister`].
    pub fn register(&self, file: &AsRawFd, t: Token) -> io::Result<()> {
        self.ctl(file, t, epoll::EpollOp::EpollCtlAdd)
    }

    /// Re-register a file descriptor with this `Poll` instance.
    ///
    /// When you re-register a file descriptor, you can change the details of the registration.
    /// Specifically, you can update the `token` specified in previous `register` and `reregister`
    /// calls.
    ///
    /// See the [`register`] documentation for details about the function
    /// arguments and see the [`struct`] docs for a high level overview of
    /// polling.
    ///
    /// [`struct`]: #
    /// [`register`]: #method.register
    pub fn reregister(&self, file: &AsRawFd, t: Token) -> io::Result<()> {
        self.ctl(file, t, epoll::EpollOp::EpollCtlMod)
    }

    /// Deregister a file descriptor from this `Poll` instance.
    ///
    /// When you deregister a file descriptor, it will no longer be modified for readiness events,
    /// and it will no longer produce events from `poll`.
    pub fn deregister(&self, file: &AsRawFd) -> io::Result<()> {
        epoll::epoll_ctl(self.0, epoll::EpollOp::EpollCtlDel, file.as_raw_fd(), None)
            .map_err(nix_to_io_err)
    }

    /// Wait for events on file descriptors associated with this `Poll` instance.
    ///
    /// Blocks the current thread and waits for events for any of the file descriptors that are
    /// registered with this `Poll` instance. The function blocks until either at least one
    /// readiness event has been received or `timeout` has elapsed. A `timeout` of `None` means
    /// that `poll` blocks until a readiness event has been received.
    ///
    /// The supplied `events` will be cleared and newly received readiness events will be pushed
    /// onto the end. At most `events.capacity()` events will be returned. If there are further
    /// pending readiness events, they are returned on the next call to `poll`.
    ///
    /// Note that once an event has been issued for a given `token` (or rather, for the token's
    /// file descriptor), no further events will be issued for that descriptor until it has been
    /// re-registered. Note also that the `timeout` is rounded up to the system clock granularity
    /// (usually 1ms), and kernel scheduling delays mean that the blocking interval may be overrun
    /// by a small amount.
    ///
    /// `poll` returns the number of events that have been pushed into `events`, or `Err` when an
    /// error has been encountered with the system selector.
    ///
    /// See the [struct] level documentation for a higher level discussion of polling.
    ///
    /// [struct]: #
    pub fn poll(&self, events: &mut Events, timeout: Option<time::Duration>) -> io::Result<usize> {
        let timeout = match timeout {
            None => -1,
            Some(d) => (d.as_secs() * 1000 + d.subsec_nanos() as u64 / 1_000_000) as isize,
        };

        events.current =
            epoll::epoll_wait(self.0, &mut events.all[..], timeout).map_err(nix_to_io_err)?;
        Ok(events.current)
    }
}

/// [`Events`] iterator.
///
/// This struct is created by the `into_iter` method on [`Events`].
///
/// [`Events`]: struct.Events.html
pub struct EventsIterator<'a> {
    events: &'a Events,
    at: usize,
}

impl<'a> IntoIterator for &'a Events {
    type IntoIter = EventsIterator<'a>;
    type Item = Token;

    fn into_iter(self) -> Self::IntoIter {
        EventsIterator {
            events: self,
            at: 0,
        }
    }
}

impl<'a> Iterator for EventsIterator<'a> {
    type Item = Token;
    fn next(&mut self) -> Option<Self::Item> {
        let at = &mut self.at;
        if *at >= self.events.current {
            // events beyond .1 are old
            return None;
        }

        self.events.all.get(*at).map(|e| {
            *at += 1;
            Token(e.data() as usize)
        })
    }
}
