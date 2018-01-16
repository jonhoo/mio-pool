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
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.and_return(|v| v)
        .run(1, |c: &mut mio::net::TcpStream, s: &mut Vec<u8>| {
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
    let mut r = h.terminate();
    assert_eq!(r.len(), 1);
    let r = r.swap_remove(0).unwrap();
    assert_eq!(&r, b"hello world");
}

#[test]
fn soft_exit_no_clients() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.run_stateless(2, |c: &mut mio::net::TcpStream| {
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }
        Ok(false)
    });

    let r = h.finish();
    assert_eq!(r.len(), 2);
    for r in r {
        assert!(r.is_ok());
    }
}

#[test]
fn soft_exit_one_client() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.run_stateless(2, |c: &mut mio::net::TcpStream| {
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }
        Ok(false)
    });

    drop(TcpStream::connect(&addr).unwrap());

    let r = h.finish();
    assert_eq!(r.len(), 2);
    for r in r {
        assert!(r.is_ok());
    }
}

#[test]
fn soft_exit_many_client() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.run_stateless(2, |c: &mut mio::net::TcpStream| {
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }
        Ok(false)
    });

    for _ in 0..20 {
        use std::thread;
        thread::spawn(move || {
            drop(TcpStream::connect(&addr).unwrap());
        });
    }

    let r = h.finish();
    assert_eq!(r.len(), 2);
    for r in r {
        assert!(r.is_ok());
    }
}

#[test]
fn soft_exit_no_new() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.run_stateless(2, |c: &mut mio::net::TcpStream| {
        let mut buf = [0u8; 1024];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }
        c.write_all(&buf[..n])?;
        Ok(false)
    });

    // long-running client
    let mut c = TcpStream::connect(&addr).unwrap();

    // start a thread that waits for workers to finish
    use std::thread;
    use std::sync::{atomic, Arc};
    let finished = Arc::new(atomic::AtomicBool::new(false));
    let d = Arc::clone(&finished);
    let jh1 = thread::spawn(move || {
        let r = h.finish();
        d.store(true, atomic::Ordering::SeqCst);
        assert_eq!(r.len(), 2);
        for r in r {
            assert!(r.is_ok());
        }
    });

    // give that thread some time to start and for threads to realize we're exiting
    use std::time;
    thread::sleep(time::Duration::from_millis(100));

    // start another thread tries to connect
    let connect_result = Arc::new(atomic::AtomicUsize::new(0));
    let d = Arc::clone(&connect_result);
    let jh2 = thread::spawn(move || {
        // note that even though the workers don't *accept* the connection, it'll still be
        // established (see https://stackoverflow.com/a/2411333/472927).
        let mut r = TcpStream::connect(&addr).unwrap();

        // we can also send data just fine
        r.write_all(&[0x00]).unwrap();;

        // the question is whether the workers respond
        let mut buf = [0; 1];
        let r = r.read(&mut buf[..]);
        if r.is_ok() {
            d.store(1, atomic::Ordering::SeqCst);
        } else {
            d.store(2, atomic::Ordering::SeqCst);
        }
        r
    });

    // give that thread some time to start too
    thread::sleep(time::Duration::from_millis(500));

    // at this point, `c` should still be active (and work correctly)
    assert!(c.write_all(&[0x00]).is_ok());
    let mut buf = [0];
    assert_eq!(c.read(&mut buf[..]).unwrap(), 1);
    // finish should therefore not yet have returned
    assert_eq!(finished.load(atomic::Ordering::SeqCst), false);
    // and the new connection should still hang
    assert_eq!(connect_result.load(atomic::Ordering::SeqCst), 0);

    // now we drop the last connection
    drop(c);
    // and give the threads some time to do their thing
    thread::sleep(time::Duration::from_millis(500));
    // now finish should have returned
    assert_eq!(finished.load(atomic::Ordering::SeqCst), true);
    jh1.join().unwrap();
    // and the connection we tried to establish should have failed
    assert_eq!(connect_result.load(atomic::Ordering::SeqCst), 2);
    jh2.join().unwrap().unwrap_err();
}

#[test]
fn multi_rtt() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.and_return(|v| v)
        .run(1, |c: &mut mio::net::TcpStream, s: &mut Vec<u8>| {
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
    let mut r = h.terminate();
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
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.and_return(|v| v)
        .run(1, |c: &mut mio::net::TcpStream, s: &mut Vec<u8>| {
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

    let mut r = h.terminate();
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
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.and_return(|v| v)
        .run(2, |c: &mut mio::net::TcpStream, s: &mut Vec<u8>| {
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

    let r = h.terminate();
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
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.and_return(|v| v)
        .run(10, |c: &mut mio::net::TcpStream, s: &mut Vec<u8>| {
            let mut buf = [0u8; 1024];
            let n = c.read(&mut buf)?;
            if n == 0 {
                return Ok(true);
            }

            s.extend(&buf[..n]);
            c.write_all(b"yes indeed")?;
            Ok(false)
        });

    let n = 50;
    let cs: Vec<_> = (0..n)
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

    let r = h.terminate();
    assert_eq!(r.len(), 10);
    let mut nr = 0;
    for r in r {
        let r = r.unwrap();
        let mut r = &r[..];
        while !r.is_empty() {
            assert!(r.starts_with(b"hello world"));
            r = &r[b"hello world".len()..];
            nr += 1;
        }
    }
    assert_eq!(nr, n);
}
