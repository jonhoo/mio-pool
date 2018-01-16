extern crate mio;
extern crate mio_pool;

use std::env;
use std::io::prelude::*;
use std::net::TcpStream;
use mio_pool::PoolBuilder;

fn main() {
    let mut args = env::args();
    args.next().unwrap();
    let mut iters = args.next().unwrap().parse().unwrap();
    let workers = args.next().unwrap().parse().unwrap();
    let clients = args.next().unwrap().parse().unwrap();
    iters /= clients;

    let addr = "127.0.0.1:0".parse().unwrap();
    let server = mio::net::TcpListener::bind(&addr).unwrap();
    let addr = server.local_addr().unwrap();
    let pool = PoolBuilder::from(server).unwrap();
    let h = pool.run_stateless(workers, |c: &mut mio::net::TcpStream| {
        let mut buf = [0];
        let n = c.read(&mut buf)?;
        if n == 0 {
            return Ok(true);
        }
        c.write_all(&buf[..n])?;
        Ok(false)
    });

    use std::time;
    let start = time::Instant::now();
    let clients: Vec<_> = (0..clients)
        .map(|_| {
            use std::thread;
            thread::spawn(move || {
                let mut c = TcpStream::connect(&addr).unwrap();

                let mut written = 0;
                let mut read = 0;
                let mut buf = [0x42; 1024];
                for _ in 0..iters {
                    c.write_all(&buf[..]).unwrap();
                    written += buf.len();
                    read += c.read(&mut buf).unwrap();
                }

                while read < written {
                    read += c.read(&mut buf).unwrap();
                }
            })
        })
        .collect();
    for c in clients {
        c.join().unwrap();
    }
    let took = start.elapsed();

    let r = h.terminate();
    assert_eq!(r.len(), workers);
    for r in r {
        r.unwrap();
    }

    println!(
        "{} ms",
        took.as_secs() * 1000 + took.subsec_nanos() as u64 / 1_000_000
    );
}
