extern crate mio;
extern crate mio_pool;

use mio_pool::*;
use std::net::TcpStream;
use std::io::prelude::*;

#[test]
fn one_client() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = Pool::new(server).unwrap();
    let h = pool.run(1, |mut c: &mio::net::TcpStream, s: &mut Vec<u8>| {
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }

        s.extend(&buf[..n]);
        c.write_all(b"yes indeed")?;
        Ok(false)
    });
    let mut c = TcpStream::connect(&addr).unwrap();
    c.write_all(b"hello world").unwrap();
    let mut buf = [0u8; 1024];
    let n = c.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"yes indeed");
    let mut r = h.wait();
    assert_eq!(r.len(), 1);
    let r = r.swap_remove(0).unwrap();
    assert_eq!(&r, b"hello world");
}

#[test]
fn multi_rtt() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = Pool::new(server).unwrap();
    let h = pool.run(1, |mut c: &mio::net::TcpStream, s: &mut Vec<u8>| {
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }

        s.extend(&buf[..n]);
        c.write_all(b"yes indeed")?;
        Ok(false)
    });
    let mut c = TcpStream::connect(&addr).unwrap();
    for _ in 0..10 {
        c.write_all(b"hello world").unwrap();
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"yes indeed");
    }
    let mut r = h.wait();
    assert_eq!(r.len(), 1);
    let r = r.swap_remove(0).unwrap();
    let mut r = &r[..];
    while !r.is_empty() {
        assert!(r.starts_with(b"hello world"));
        r = &r[b"hello world".len()..];
    }
}

#[test]
fn client_churn() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = Pool::new(server).unwrap();
    let h = pool.run(1, |mut c: &mio::net::TcpStream, s: &mut Vec<u8>| {
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }

        s.extend(&buf[..n]);
        c.write_all(b"yes indeed")?;
        Ok(false)
    });

    for _ in 0..10 {
        let mut c = TcpStream::connect(&addr).unwrap();
        c.write_all(b"hello world").unwrap();
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"yes indeed");
    }

    let mut r = h.wait();
    assert_eq!(r.len(), 1);
    let r = r.swap_remove(0).unwrap();
    let mut r = &r[..];
    while !r.is_empty() {
        assert!(r.starts_with(b"hello world"));
        r = &r[b"hello world".len()..];
    }
}

#[test]
fn client_churn_two_workers() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = Pool::new(server).unwrap();
    let h = pool.run(2, |mut c: &mio::net::TcpStream, s: &mut Vec<u8>| {
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }

        s.extend(&buf[..n]);
        c.write_all(b"yes indeed")?;
        Ok(false)
    });

    for _ in 0..10 {
        let mut c = TcpStream::connect(&addr).unwrap();
        c.write_all(b"hello world").unwrap();
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"yes indeed");
    }

    let r = h.wait();
    assert_eq!(r.len(), 2);
    for r in r {
        let r = r.unwrap();
        let mut r = &r[..];
        while !r.is_empty() {
            assert!(r.starts_with(b"hello world"));
            r = &r[b"hello world".len()..];
        }
    }
}

#[test]
fn many_workers() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = Pool::new(server).unwrap();
    let h = pool.run(10, |mut c: &mio::net::TcpStream, s: &mut Vec<u8>| {
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }

        s.extend(&buf[..n]);
        c.write_all(b"yes indeed")?;
        Ok(false)
    });

    let cs: Vec<_> = (0..50)
        .map(|_| {
            use std::thread;
            thread::spawn(move || {
                let mut c = TcpStream::connect(&addr).unwrap();
                c.write_all(b"hello world").unwrap();
                let mut buf = [0u8; 1024];
                let n = c.read(&mut buf).unwrap();
                buf[..n].to_vec()
            })
        })
        .collect();

    for c in cs {
        assert_eq!(&c.join().unwrap()[..], b"yes indeed");
    }

    let r = h.wait();
    assert_eq!(r.len(), 10);
    for r in r {
        let r = r.unwrap();
        assert!(!r.is_empty());
        let mut r = &r[..];
        while !r.is_empty() {
            assert!(r.starts_with(b"hello world"));
            r = &r[b"hello world".len()..];
        }
    }
}
