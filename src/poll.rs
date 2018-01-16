extern crate libc;

use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::time;
use std::io;

pub(crate) struct Poll {
    selector: RawFd,
}

pub(crate) struct Token(pub usize);

pub(crate) struct Events(Vec<libc::epoll_event>);

impl Events {
    pub fn with_capacity(capacity: usize) -> Events {
        Events(Vec::with_capacity(capacity))
    }
}

impl Poll {
    pub fn new() -> io::Result<Self> {
        let fd = unsafe { libc::epoll_create1(0) };
        if fd == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(Poll { selector: fd })
        }
    }

    fn ctl(&self, file: &AsRawFd, t: Token, op: libc::c_int) -> io::Result<()> {
        let fd = file.as_raw_fd();
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32 | libc::EPOLLONESHOT as u32,
            u64: t.0 as u64,
        };
        if unsafe { libc::epoll_ctl(self.selector, op, fd, &mut event as *mut _) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn register(&self, file: &AsRawFd, t: Token) -> io::Result<()> {
        self.ctl(file, t, libc::EPOLL_CTL_ADD)
    }

    pub fn reregister(&self, file: &AsRawFd, t: Token) -> io::Result<()> {
        self.ctl(file, t, libc::EPOLL_CTL_MOD)
    }

    pub fn poll(&self, events: &mut Events, timeout: Option<time::Duration>) -> io::Result<usize> {
        events.0.clear();
        let maxevents = events.0.capacity() as i32;
        let timeout = match timeout {
            None => 0,
            Some(d) => d.as_secs() * 1000 + d.subsec_nanos() as u64 / 1_000_000,
        } as i32;

        let n =
            unsafe { libc::epoll_wait(self.selector, events.0.as_mut_ptr(), maxevents, timeout) };
        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        unsafe { events.0.set_len(n as usize) };
        Ok(n as usize)
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
        self.events.0.get(*at).map(|e| {
            *at += 1;
            Token(e.u64 as usize)
        })
    }
}
