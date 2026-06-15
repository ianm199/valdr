use super::*;
use redis_core::Client;

#[test]
fn lookup_is_case_insensitive() {
    assert!(lookup_command(b"PING").is_some());
    assert!(lookup_command(b"ping").is_some());
    assert!(lookup_command(b"Ping").is_some());
    assert!(lookup_command(b"PiNg").is_some());
    assert!(lookup_command(b"hgetdel").is_some());
}

#[test]
fn unknown_command_is_none() {
    assert!(lookup_command(b"NOTACOMMAND").is_none());
}

#[test]
fn runtime_dispatch_table_is_sorted_for_binary_search() {
    let table = runtime_dispatch_table();
    for pair in table.windows(2) {
        assert!(
            ascii_casecmp(pair[0].entry.name, pair[1].entry.name) == Ordering::Less,
            "{} should sort before {} with no duplicate handler names",
            std::str::from_utf8(pair[0].entry.name).unwrap_or("<bytes>"),
            std::str::from_utf8(pair[1].entry.name).unwrap_or("<bytes>")
        );
    }
}

#[test]
fn generated_metadata_table_is_sorted_for_binary_search() {
    let table = command_metadata_table();
    for pair in table.windows(2) {
        assert!(
            ascii_casecmp(pair[0].0, pair[1].0) != Ordering::Greater,
            "{} should sort before {}",
            std::str::from_utf8(pair[0].0).unwrap_or("<bytes>"),
            std::str::from_utf8(pair[1].0).unwrap_or("<bytes>")
        );
    }
}

#[test]
fn command_metadata_extracts_hot_path_flags() {
    let set = command_metadata(b"set");
    assert!(set.write);
    assert!(set.denyoom);
    assert!(set.acl_categories & acl_category::WRITE != 0);

    let get = command_metadata(b"GET");
    assert!(!get.write);
    assert!(get.acl_categories & acl_category::READ != 0);

    let auth = command_metadata(b"AUTH");
    assert!(auth.no_auth);
    assert!(auth.acl_categories & acl_category::CONNECTION != 0);
}

#[test]
fn dispatch_unknown_returns_err() {
    let mut c = Client::new(1);
    c.set_args(vec![RedisString::from_bytes(b"NOTACOMMAND")]);
    let mut ctx = CommandContext::new(&mut c);
    let err = dispatch(&mut ctx).unwrap_err();
    match err {
        RedisError::Runtime(s) => {
            assert!(s.as_bytes().starts_with(b"ERR unknown command"));
        }
        _ => panic!("expected Runtime error"),
    }
}

#[test]
fn dispatch_routes_known_command() {
    let mut c = Client::new(1);
    c.set_args(vec![RedisString::from_bytes(b"HELLO")]);
    let mut ctx = CommandContext::new(&mut c);
    dispatch(&mut ctx).unwrap();
    let reply = c.drain_reply();
    assert!(reply.starts_with(b"*"));
    assert!(reply.windows(b"server".len()).any(|w| w == b"server"));
}

#[test]
fn dispatch_routes_ping_to_real_handler() {
    let mut c = Client::new(1);
    c.set_args(vec![RedisString::from_bytes(b"PING")]);
    let mut ctx = CommandContext::new(&mut c);
    dispatch(&mut ctx).unwrap();
    assert_eq!(c.drain_reply(), b"+PONG\r\n");
}
