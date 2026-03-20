use bytes::BytesMut;
use std::time::Instant;

use crate::resp;
use crate::store::Store;

use super::CmdResult;

pub fn cmd_pfadd(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 2 {
        resp::write_error(out, "ERR wrong number of arguments for 'pfadd' command");
        return CmdResult::Written;
    }
    let key = args[1];
    let elements: Vec<&[u8]> = args[2..].to_vec();
    match store.pfadd(key, &elements, now) {
        Ok(changed) => resp::write_integer(out, changed),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_pfcount(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 2 {
        resp::write_error(out, "ERR wrong number of arguments for 'pfcount' command");
        return CmdResult::Written;
    }
    let keys: Vec<&[u8]> = args[1..].to_vec();
    match store.pfcount(&keys, now) {
        Ok(count) => resp::write_integer(out, count),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_pfmerge(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 2 {
        resp::write_error(out, "ERR wrong number of arguments for 'pfmerge' command");
        return CmdResult::Written;
    }
    let dest = args[1];
    let sources: Vec<&[u8]> = args[2..].to_vec();
    match store.pfmerge(dest, &sources, now) {
        Ok(()) => resp::write_ok(out),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}
