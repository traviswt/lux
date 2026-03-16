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
    let release = target_dir.join("release").join("lux");
    if release.exists() {
        return Some(release);
    }
    let debug = target_dir.join("debug").join("lux");
    if debug.exists() {
        return Some(debug);
    }
    None
}

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    stream
}

fn wait_for_port(port: u16) {
    for _ in 0..40 {
        if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not start on port {port}");
}

#[test]
fn data_survives_restart_via_save() {
    let port: u16 = 16800;
    let bin = find_lux_binary().expect("no lux binary found");
    let tmpdir =
        std::env::temp_dir().join(format!("lux_persist_test_{}_{}", std::process::id(), port));
    std::fs::create_dir_all(&tmpdir).unwrap();

    let mut child = std::process::Command::new(&bin)
        .env("LUX_PORT", port.to_string())
        .env("LUX_SHARDS", "4")
        .env("LUX_SAVE_INTERVAL", "0")
        .env("LUX_DATA_DIR", tmpdir.to_str().unwrap())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to start lux");

    wait_for_port(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["SET", "persist_key", "persist_val"]);
    send_and_read(&mut conn, &["LPUSH", "persist_list", "a", "b", "c"]);
    send_and_read(&mut conn, &["SADD", "persist_set", "x", "y"]);
    send_and_read(&mut conn, &["HSET", "persist_hash", "f1", "v1"]);
    send_and_read(&mut conn, &["ZADD", "persist_zset", "1", "m1", "2", "m2"]);
    let resp = send_and_read(&mut conn, &["SAVE"]);
    assert!(resp.contains("OK"), "SAVE: {resp}");

    drop(conn);
    child.kill().ok();
    child.wait().ok();

    thread::sleep(Duration::from_millis(200));

    let mut child2 = std::process::Command::new(&bin)
        .env("LUX_PORT", port.to_string())
        .env("LUX_SHARDS", "4")
        .env("LUX_SAVE_INTERVAL", "0")
        .env("LUX_DATA_DIR", tmpdir.to_str().unwrap())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to restart lux");

    wait_for_port(port);
    let mut conn2 = connect(port);

    let resp = send_and_read(&mut conn2, &["GET", "persist_key"]);
    assert!(resp.contains("persist_val"), "string survived: {resp}");

    let resp = send_and_read(&mut conn2, &["LLEN", "persist_list"]);
    assert!(resp.contains(":3"), "list survived: {resp}");

    let resp = send_and_read(&mut conn2, &["SCARD", "persist_set"]);
    assert!(resp.contains(":2"), "set survived: {resp}");

    let resp = send_and_read(&mut conn2, &["HGET", "persist_hash", "f1"]);
    assert!(resp.contains("v1"), "hash survived: {resp}");

    let resp = send_and_read(&mut conn2, &["ZCARD", "persist_zset"]);
    assert!(resp.contains(":2"), "sorted set survived: {resp}");

    let resp = send_and_read(&mut conn2, &["DBSIZE"]);
    assert!(resp.contains(":5"), "all 5 keys present: {resp}");

    drop(conn2);
    child2.kill().ok();
    child2.wait().ok();
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[test]
fn empty_db_no_snapshot_starts_clean() {
    let port: u16 = 16801;
    let bin = find_lux_binary().expect("no lux binary found");
    let tmpdir =
        std::env::temp_dir().join(format!("lux_persist_test_{}_{}", std::process::id(), port));
    std::fs::create_dir_all(&tmpdir).unwrap();

    let mut child = std::process::Command::new(&bin)
        .env("LUX_PORT", port.to_string())
        .env("LUX_SHARDS", "4")
        .env("LUX_SAVE_INTERVAL", "0")
        .env("LUX_DATA_DIR", tmpdir.to_str().unwrap())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to start lux");

    wait_for_port(port);
    let mut conn = connect(port);

    let resp = send_and_read(&mut conn, &["DBSIZE"]);
    assert!(resp.contains(":0"), "fresh start has 0 keys: {resp}");

    drop(conn);
    child.kill().ok();
    child.wait().ok();
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[test]
fn flushdb_then_save_clears_snapshot() {
    let port: u16 = 16802;
    let bin = find_lux_binary().expect("no lux binary found");
    let tmpdir =
        std::env::temp_dir().join(format!("lux_persist_test_{}_{}", std::process::id(), port));
    std::fs::create_dir_all(&tmpdir).unwrap();

    let mut child = std::process::Command::new(&bin)
        .env("LUX_PORT", port.to_string())
        .env("LUX_SHARDS", "4")
        .env("LUX_SAVE_INTERVAL", "0")
        .env("LUX_DATA_DIR", tmpdir.to_str().unwrap())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to start lux");

    wait_for_port(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["SET", "k1", "v1"]);
    send_and_read(&mut conn, &["SET", "k2", "v2"]);
    send_and_read(&mut conn, &["SAVE"]);
    send_and_read(&mut conn, &["FLUSHDB"]);
    send_and_read(&mut conn, &["SAVE"]);

    drop(conn);
    child.kill().ok();
    child.wait().ok();
    thread::sleep(Duration::from_millis(200));

    let mut child2 = std::process::Command::new(&bin)
        .env("LUX_PORT", port.to_string())
        .env("LUX_SHARDS", "4")
        .env("LUX_SAVE_INTERVAL", "0")
        .env("LUX_DATA_DIR", tmpdir.to_str().unwrap())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to restart lux");

    wait_for_port(port);
    let mut conn2 = connect(port);

    let resp = send_and_read(&mut conn2, &["DBSIZE"]);
    assert!(
        resp.contains(":0"),
        "flushed db should be empty after restart: {resp}"
    );

    drop(conn2);
    child2.kill().ok();
    child2.wait().ok();
    let _ = std::fs::remove_dir_all(&tmpdir);
}
