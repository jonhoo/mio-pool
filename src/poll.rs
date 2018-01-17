extern crate nix;

use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::time;
use std::io;
use self::nix::sys::epoll;

pub(crate) struct Poll(RawFd);

pub(crate) struct Token(pub usize);

pub(crate) struct Events {
    all: Vec<epoll::EpollEvent>,

    /// How many of the events in `.all` are filled with responses to the last `poll()`?
    current: usize,
}

impl Events {
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
    pub fn new() -> io::Result<Self> {
        epoll::epoll_create1(epoll::EpollCreateFlags::empty())
            .map(Poll)
            .map_err(nix_to_io_err)
    }

    fn ctl(&self, file: &AsRawFd, t: Token, op: epoll::EpollOp) -> io::Result<()> {
        let mut event = epoll::EpollEvent::new(epoll::EPOLLIN | epoll::EPOLLONESHOT, t.0 as u64);
        epoll::epoll_ctl(self.0, op, file.as_raw_fd(), &mut event).map_err(nix_to_io_err)
    }

    pub fn register(&self, file: &AsRawFd, t: Token) -> io::Result<()> {
        self.ctl(file, t, epoll::EpollOp::EpollCtlAdd)
    }

    pub fn reregister(&self, file: &AsRawFd, t: Token) -> io::Result<()> {
        self.ctl(file, t, epoll::EpollOp::EpollCtlMod)
    }

    pub fn poll(&self, events: &mut Events, timeout: Option<time::Duration>) -> io::Result<usize> {
        let timeout = match timeout {
            None => 0,
            Some(d) => d.as_secs() * 1000 + d.subsec_nanos() as u64 / 1_000_000,
        } as isize;

        events.current =
            epoll::epoll_wait(self.0, &mut events.all[..], timeout).map_err(nix_to_io_err)?;
        Ok(events.current)
    }
}

pub(crate) struct EventsIterator<'a> {
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
