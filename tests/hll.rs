use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

fn resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len());
    for arg in args {
        buf.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    buf.into_bytes()
}

fn read_all(stream: &mut TcpStream) -> String {
    let mut data = Vec::with_capacity(4096);
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(len) => data.extend_from_slice(&buf[..len]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&data).to_string()
}

fn send_and_read(stream: &mut TcpStream, args: &[&str]) -> String {
    stream.write_all(&resp_cmd(args)).unwrap();
    thread::sleep(Duration::from_millis(50));
    read_all(stream)
}

fn find_lux_binary() -> Option<std::path::PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let target_dir = exe.parent()?.parent()?.parent()?;
    let debug = target_dir.join("debug").join("lux");
    if debug.exists() {
        return Some(debug);
    }
    let release = target_dir.join("release").join("lux");
    if release.exists() {
        return Some(release);
    }
    None
}

struct LuxServer {
    child: std::process::Child,
    tmpdir: std::path::PathBuf,
}

impl Drop for LuxServer {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
        let _ = std::fs::remove_dir_all(&self.tmpdir);
    }
}

fn start_lux(port: u16) -> LuxServer {
    let bin = find_lux_binary().expect("no lux binary found - run `cargo build` first");
    let tmpdir = std::env::temp_dir().join(format!("lux_hll_test_{}_{}", std::process::id(), port));
    std::fs::create_dir_all(&tmpdir).unwrap();
    let child = std::process::Command::new(&bin)
        .env("LUX_PORT", port.to_string())
        .env("LUX_SHARDS", "4")
        .env("LUX_SAVE_INTERVAL", "0")
        .env("LUX_DATA_DIR", tmpdir.to_str().unwrap())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to start lux");

    let server = LuxServer { child, tmpdir };

    for _ in 0..40 {
        if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            return server;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("lux did not start within 2 seconds on port {port}");
}

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    stream
}

fn parse_integer(resp: &str) -> i64 {
    for line in resp.lines() {
        if let Some(rest) = line.strip_prefix(':') {
            return rest.trim().parse().unwrap_or(-999);
        }
    }
    panic!("no integer in response: {resp}");
}

#[test]
fn test_pfadd_basic() {
    let port: u16 = 17500;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let resp = send_and_read(&mut conn, &["PFADD", "hll", "a", "b", "c"]);
    assert!(resp.contains(":1"), "PFADD should return 1: {resp}");

    let resp = send_and_read(&mut conn, &["PFADD", "hll", "a", "b", "c"]);
    assert!(
        resp.contains(":0"),
        "PFADD duplicates should return 0: {resp}"
    );

    let resp = send_and_read(&mut conn, &["PFADD", "hll", "d"]);
    assert!(
        resp.contains(":1"),
        "PFADD new element should return 1: {resp}"
    );
}

#[test]
fn test_pfadd_creates_empty() {
    let port: u16 = 17501;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let resp = send_and_read(&mut conn, &["PFADD", "hll"]);
    assert!(
        resp.contains(":0") || resp.contains(":1"),
        "PFADD empty should work: {resp}"
    );

    let resp = send_and_read(&mut conn, &["PFCOUNT", "hll"]);
    assert!(resp.contains(":0"), "empty HLL should count 0: {resp}");
}

#[test]
fn test_pfcount_accuracy() {
    let port: u16 = 17502;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let n = 1000;
    let elements: Vec<String> = (0..n).map(|i| format!("element:{}", i)).collect();
    let mut args: Vec<&str> = vec!["PFADD", "hll"];
    args.extend(elements.iter().map(|s| s.as_str()));
    send_and_read(&mut conn, &args);

    let resp = send_and_read(&mut conn, &["PFCOUNT", "hll"]);
    let count = parse_integer(&resp);
    let error = (count as f64 - n as f64).abs() / n as f64;
    assert!(
        error < 0.05,
        "PFCOUNT {count} too far from {n}, error={error}"
    );
}

#[test]
fn test_pfcount_multiple_keys() {
    let port: u16 = 17503;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let elems1: Vec<String> = (0..500).map(|i| format!("a:{}", i)).collect();
    let mut args1: Vec<&str> = vec!["PFADD", "hll1"];
    args1.extend(elems1.iter().map(|s| s.as_str()));
    send_and_read(&mut conn, &args1);

    let elems2: Vec<String> = (0..500).map(|i| format!("b:{}", i)).collect();
    let mut args2: Vec<&str> = vec!["PFADD", "hll2"];
    args2.extend(elems2.iter().map(|s| s.as_str()));
    send_and_read(&mut conn, &args2);

    let resp = send_and_read(&mut conn, &["PFCOUNT", "hll1", "hll2"]);
    let count = parse_integer(&resp);
    let error = (count as f64 - 1000.0).abs() / 1000.0;
    assert!(
        error < 0.05,
        "PFCOUNT multi {count} too far from 1000, error={error}"
    );
}

#[test]
fn test_pfmerge_disjoint() {
    let port: u16 = 17504;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let elems1: Vec<String> = (0..500).map(|i| format!("a:{}", i)).collect();
    let mut args1: Vec<&str> = vec!["PFADD", "src1"];
    args1.extend(elems1.iter().map(|s| s.as_str()));
    send_and_read(&mut conn, &args1);

    let elems2: Vec<String> = (0..500).map(|i| format!("b:{}", i)).collect();
    let mut args2: Vec<&str> = vec!["PFADD", "src2"];
    args2.extend(elems2.iter().map(|s| s.as_str()));
    send_and_read(&mut conn, &args2);

    let resp = send_and_read(&mut conn, &["PFMERGE", "dest", "src1", "src2"]);
    assert!(resp.contains("+OK"), "PFMERGE should return OK: {resp}");

    let resp = send_and_read(&mut conn, &["PFCOUNT", "dest"]);
    let count = parse_integer(&resp);
    let error = (count as f64 - 1000.0).abs() / 1000.0;
    assert!(
        error < 0.05,
        "merged count {count} too far from 1000, error={error}"
    );
}

#[test]
fn test_pfmerge_overlapping() {
    let port: u16 = 17505;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let elems1: Vec<String> = (0..500).map(|i| format!("item:{}", i)).collect();
    let mut args1: Vec<&str> = vec!["PFADD", "src1"];
    args1.extend(elems1.iter().map(|s| s.as_str()));
    send_and_read(&mut conn, &args1);

    let elems2: Vec<String> = (250..750).map(|i| format!("item:{}", i)).collect();
    let mut args2: Vec<&str> = vec!["PFADD", "src2"];
    args2.extend(elems2.iter().map(|s| s.as_str()));
    send_and_read(&mut conn, &args2);

    send_and_read(&mut conn, &["PFMERGE", "dest", "src1", "src2"]);
    let resp = send_and_read(&mut conn, &["PFCOUNT", "dest"]);
    let count = parse_integer(&resp);
    let error = (count as f64 - 750.0).abs() / 750.0;
    assert!(
        error < 0.05,
        "overlapping merge count {count} too far from 750, error={error}"
    );
}

#[test]
fn test_pfcount_empty_key() {
    let port: u16 = 17506;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let resp = send_and_read(&mut conn, &["PFCOUNT", "nonexistent"]);
    assert!(
        resp.contains(":0"),
        "nonexistent key should return 0: {resp}"
    );
}

#[test]
fn test_wrongtype_on_non_hll() {
    let port: u16 = 17507;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["SET", "mystr", "hello"]);

    let resp = send_and_read(&mut conn, &["PFADD", "mystr", "element"]);
    assert!(
        resp.contains("WRONGTYPE"),
        "PFADD on string should return WRONGTYPE: {resp}"
    );

    let resp = send_and_read(&mut conn, &["PFCOUNT", "mystr"]);
    assert!(
        resp.contains("WRONGTYPE"),
        "PFCOUNT on string should return WRONGTYPE: {resp}"
    );
}

#[test]
fn test_type_returns_string() {
    let port: u16 = 17508;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["PFADD", "hll", "a"]);
    let resp = send_and_read(&mut conn, &["TYPE", "hll"]);
    assert!(
        resp.contains("string"),
        "TYPE should return string for HLL: {resp}"
    );
}
