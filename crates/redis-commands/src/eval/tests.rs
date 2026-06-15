use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use mlua::{Lua, Table as LuaTable, Value as LuaValue};
use redis_core::{pubsub_registry::PubSubRegistry, RedisDb, RedisServer};
use redis_types::RedisString;

use super::bytes::hex_encode;
use super::lua_bit::install_bit;
use super::lua_cmsgpack::install_cmsgpack;
use super::lua_sandbox::install_sandbox;
use super::resp_bridge::reply_to_lua;
use super::script_cache::sha1_hex;
use super::script_checks::FunctionScriptChecks;
use super::*;

#[test]
fn sha1_hex_known_vectors() {
    let empty = sha1_hex(b"");
    assert_eq!(&empty, b"da39a3ee5e6b4b0d3255bfef95601890afd80709");
    let abc = sha1_hex(b"abc");
    assert_eq!(&abc, b"a9993e364706816aba3e25717850c26c9cd0d89d");
}

#[test]
fn normalise_sha_lowercases() {
    let upper = b"DA39A3EE5E6B4B0D3255BFEF95601890AFD80709";
    let n = normalise_sha(upper).unwrap();
    assert_eq!(&n, b"da39a3ee5e6b4b0d3255bfef95601890afd80709");
}

#[test]
fn normalise_sha_rejects_non_hex() {
    assert!(normalise_sha(b"short").is_none());
    assert!(normalise_sha(b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_none());
}

#[test]
fn eval_select_does_not_leak_db() {
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let mut client = redis_core::Client::new(7);
    client.db_index = 10;
    client.set_args(vec![
        RedisString::from_bytes(b"EVAL"),
        RedisString::from_bytes(b"return redis.call('select', '9')"),
        RedisString::from_bytes(b"0"),
    ]);
    let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut ctx =
        redis_core::CommandContext::with_server_and_db_list(&mut client, &mut dbs, server, pubsub);
    eval_command(&mut ctx).unwrap();
    assert_eq!(client.db_index, 10);
    assert_eq!(client.drain_reply(), b"+OK\r\n");
}

#[test]
fn eval_redis_call_error_is_single_resp_error_line() {
    let mut client = redis_core::Client::new(8);
    client.set_args(vec![
        RedisString::from_bytes(b"EVAL"),
        RedisString::from_bytes(b"redis.call('nosuchcommand')"),
        RedisString::from_bytes(b"0"),
    ]);
    let mut ctx = CommandContext::new(&mut client);
    let err = eval_command(&mut ctx).unwrap_err();
    let payload = err.to_resp_payload();
    let bytes = payload.as_bytes();
    assert!(bytes.starts_with(b"ERR "));
    assert!(bytes
        .windows(b"unknown command".len())
        .any(|w| w.eq_ignore_ascii_case(b"unknown command")));
    assert!(!bytes.contains(&b'\n'));
    assert!(!bytes.contains(&b'\r'));
    assert!(!bytes
        .windows(b"stack traceback".len())
        .any(|w| w == b"stack traceback"));
}

#[cfg(feature = "lua-rs-engine")]
#[test]
fn lua_rs_eval_smoke_covers_args_call_and_sha1hex() {
    let mut client = redis_core::Client::new(81);
    client.set_args(vec![
        RedisString::from_bytes(b"EVAL"),
        RedisString::from_bytes(
            b"return {KEYS[1], ARGV[1], redis.call('ping').ok, redis.sha1hex('abc')}",
        ),
        RedisString::from_bytes(b"1"),
        RedisString::from_bytes(b"k"),
        RedisString::from_bytes(b"v"),
    ]);
    let mut ctx = CommandContext::new(&mut client);

    eval_command(&mut ctx).unwrap();

    assert_eq!(
        client.drain_reply(),
        b"*4\r\n$1\r\nk\r\n$1\r\nv\r\n$4\r\nPONG\r\n$40\r\na9993e364706816aba3e25717850c26c9cd0d89d\r\n"
    );
}

#[cfg(feature = "lua-rs-engine")]
#[test]
fn lua_rs_eval_smoke_pcall_returns_error_table() {
    let mut client = redis_core::Client::new(82);
    client.set_args(vec![
        RedisString::from_bytes(b"EVAL"),
        RedisString::from_bytes(b"return redis.pcall('nosuchcommand').err"),
        RedisString::from_bytes(b"0"),
    ]);
    let mut ctx = CommandContext::new(&mut client);

    eval_command(&mut ctx).unwrap();

    let reply = client.drain_reply();
    assert!(reply.starts_with(b"$"));
    assert!(reply
        .windows(b"unknown command".len())
        .any(|w| w.eq_ignore_ascii_case(b"unknown command")));
}

#[cfg(feature = "lua-rs-engine")]
#[test]
fn lua_rs_evalsha_runs_stateful_token_bucket_fixture() {
    const TOKEN_BUCKET_SCRIPT: &[u8] = br#"
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local refill_tokens = tonumber(ARGV[3])
        local refill_ms = tonumber(ARGV[4])
        local cost = tonumber(ARGV[5])
        local ttl_ms = tonumber(ARGV[6])

        local function ceil_div(num, denom)
            return math.floor((num + denom - 1) / denom)
        end

        local tokens = capacity
        local updated_at = now
        local raw = redis.call('GET', key)
        if raw then
            local sep = string.find(raw, ':', 1, true)
            if sep then
                tokens = tonumber(string.sub(raw, 1, sep - 1))
                updated_at = tonumber(string.sub(raw, sep + 1))
            end
        end
        if tokens == nil then tokens = capacity end
        if updated_at == nil then updated_at = now end
        if now < updated_at then updated_at = now end

        local elapsed = now - updated_at
        local refill = math.floor(elapsed * refill_tokens / refill_ms)
        if refill > 0 then
            tokens = tokens + refill
            if tokens > capacity then tokens = capacity end
            updated_at = updated_at + math.floor(refill * refill_ms / refill_tokens)
        end

        local allowed = 0
        local retry_after = 0
        if tokens >= cost then
            tokens = tokens - cost
            allowed = 1
        else
            local missing = cost - tokens
            retry_after = updated_at + ceil_div(missing * refill_ms, refill_tokens) - now
            if retry_after < 0 then retry_after = 0 end
        end

        local reset_ms = updated_at + ceil_div((capacity - tokens) * refill_ms, refill_tokens)
        redis.call('SET', key, tostring(tokens) .. ':' .. tostring(updated_at), 'PX', ttl_ms)
        return {'allowed', allowed, 'remaining', tokens, 'reset_ms', reset_ms, 'retry_after_ms', retry_after}
    "#;

    fn parse_loaded_sha(reply: &[u8]) -> Vec<u8> {
        assert_eq!(reply.len(), 47, "unexpected SCRIPT LOAD reply: {reply:?}");
        assert_eq!(&reply[..5], b"$40\r\n");
        assert_eq!(&reply[45..], b"\r\n");
        reply[5..45].to_vec()
    }

    fn evalsha_token_bucket(ctx: &mut CommandContext<'_>, sha: &[u8], now_ms: &[u8]) -> Vec<u8> {
        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"EVALSHA"),
            RedisString::from_bytes(sha),
            RedisString::from_bytes(b"1"),
            RedisString::from_bytes(b"edge:tenant:42:tokens"),
            RedisString::from_bytes(now_ms),
            RedisString::from_bytes(b"10"),
            RedisString::from_bytes(b"5"),
            RedisString::from_bytes(b"1000"),
            RedisString::from_bytes(b"7"),
            RedisString::from_bytes(b"60000"),
        ]);
        evalsha_command(ctx).unwrap();
        ctx.client_mut().drain_reply()
    }

    let mut client = redis_core::Client::new(83);
    let mut db = RedisDb::new(0);
    let mut ctx = CommandContext::with_db(&mut client, &mut db);

    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"SCRIPT"),
        RedisString::from_bytes(b"LOAD"),
        RedisString::from_bytes(TOKEN_BUCKET_SCRIPT),
    ]);
    script_command(&mut ctx).unwrap();
    let sha = parse_loaded_sha(&ctx.client_mut().drain_reply());

    assert_eq!(
        evalsha_token_bucket(&mut ctx, &sha, b"1000"),
        b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:0\r\n"
    );
    assert_eq!(
        evalsha_token_bucket(&mut ctx, &sha, b"1100"),
        b"*8\r\n$7\r\nallowed\r\n:0\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:700\r\n"
    );
    assert_eq!(
        evalsha_token_bucket(&mut ctx, &sha, b"1800"),
        b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:0\r\n$8\r\nreset_ms\r\n:3800\r\n$14\r\nretry_after_ms\r\n:0\r\n"
    );
}

#[cfg(feature = "lua-rs-engine")]
#[test]
fn lua_rs_evalsha_reads_hash_policy_for_token_bucket_fixture() {
    const HASH_POLICY_TOKEN_BUCKET_SCRIPT: &[u8] = br#"
        local bucket_key = KEYS[1]
        local policy_key = KEYS[2]
        local now = tonumber(ARGV[1])
        local cost = tonumber(ARGV[2])

        local capacity = tonumber(redis.call('HGET', policy_key, 'capacity') or '10')
        local refill_tokens = tonumber(redis.call('HGET', policy_key, 'refill_tokens') or '5')
        local refill_ms = tonumber(redis.call('HGET', policy_key, 'refill_ms') or '1000')
        local ttl_ms = tonumber(redis.call('HGET', policy_key, 'ttl_ms') or '60000')

        local function ceil_div(num, denom)
            return math.floor((num + denom - 1) / denom)
        end

        local tokens = capacity
        local updated_at = now
        local raw = redis.call('GET', bucket_key)
        if raw then
            local sep = string.find(raw, ':', 1, true)
            if sep then
                tokens = tonumber(string.sub(raw, 1, sep - 1))
                updated_at = tonumber(string.sub(raw, sep + 1))
            end
        end
        if tokens == nil then tokens = capacity end
        if updated_at == nil then updated_at = now end
        if now < updated_at then updated_at = now end

        local elapsed = now - updated_at
        local refill = math.floor(elapsed * refill_tokens / refill_ms)
        if refill > 0 then
            tokens = tokens + refill
            if tokens > capacity then tokens = capacity end
            updated_at = updated_at + math.floor(refill * refill_ms / refill_tokens)
        end

        local allowed = 0
        local retry_after = 0
        if tokens >= cost then
            tokens = tokens - cost
            allowed = 1
        else
            local missing = cost - tokens
            retry_after = updated_at + ceil_div(missing * refill_ms, refill_tokens) - now
            if retry_after < 0 then retry_after = 0 end
        end

        local reset_ms = updated_at + ceil_div((capacity - tokens) * refill_ms, refill_tokens)
        redis.call('SET', bucket_key, tostring(tokens) .. ':' .. tostring(updated_at), 'PX', ttl_ms)
        return {'allowed', allowed, 'remaining', tokens, 'reset_ms', reset_ms, 'retry_after_ms', retry_after, 'capacity', capacity}
    "#;

    fn parse_loaded_sha(reply: &[u8]) -> Vec<u8> {
        assert_eq!(reply.len(), 47, "unexpected SCRIPT LOAD reply: {reply:?}");
        assert_eq!(&reply[..5], b"$40\r\n");
        assert_eq!(&reply[45..], b"\r\n");
        reply[5..45].to_vec()
    }

    fn evalsha_policy_bucket(ctx: &mut CommandContext<'_>, sha: &[u8], now_ms: &[u8]) -> Vec<u8> {
        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"EVALSHA"),
            RedisString::from_bytes(sha),
            RedisString::from_bytes(b"2"),
            RedisString::from_bytes(b"edge:tenant:42:tokens"),
            RedisString::from_bytes(b"edge:tenant:42:policy"),
            RedisString::from_bytes(now_ms),
            RedisString::from_bytes(b"7"),
        ]);
        evalsha_command(ctx).unwrap();
        ctx.client_mut().drain_reply()
    }

    let mut client = redis_core::Client::new(84);
    let mut db = RedisDb::new(0);
    let mut ctx = CommandContext::with_db(&mut client, &mut db);

    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"HSET"),
        RedisString::from_bytes(b"edge:tenant:42:policy"),
        RedisString::from_bytes(b"capacity"),
        RedisString::from_bytes(b"10"),
        RedisString::from_bytes(b"refill_tokens"),
        RedisString::from_bytes(b"5"),
        RedisString::from_bytes(b"refill_ms"),
        RedisString::from_bytes(b"1000"),
        RedisString::from_bytes(b"ttl_ms"),
        RedisString::from_bytes(b"60000"),
    ]);
    crate::hash::hset_command(&mut ctx).unwrap();
    assert_eq!(ctx.client_mut().drain_reply(), b":4\r\n");

    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"SCRIPT"),
        RedisString::from_bytes(b"LOAD"),
        RedisString::from_bytes(HASH_POLICY_TOKEN_BUCKET_SCRIPT),
    ]);
    script_command(&mut ctx).unwrap();
    let sha = parse_loaded_sha(&ctx.client_mut().drain_reply());

    assert_eq!(
        evalsha_policy_bucket(&mut ctx, &sha, b"1000"),
        b"*10\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:0\r\n$8\r\ncapacity\r\n:10\r\n"
    );
    assert_eq!(
        evalsha_policy_bucket(&mut ctx, &sha, b"1100"),
        b"*10\r\n$7\r\nallowed\r\n:0\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:700\r\n$8\r\ncapacity\r\n:10\r\n"
    );

    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"HSET"),
        RedisString::from_bytes(b"edge:tenant:42:policy"),
        RedisString::from_bytes(b"capacity"),
        RedisString::from_bytes(b"20"),
    ]);
    crate::hash::hset_command(&mut ctx).unwrap();
    assert_eq!(ctx.client_mut().drain_reply(), b":0\r\n");

    assert_eq!(
        evalsha_policy_bucket(&mut ctx, &sha, b"1800"),
        b"*10\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:0\r\n$8\r\nreset_ms\r\n:5800\r\n$14\r\nretry_after_ms\r\n:0\r\n$8\r\ncapacity\r\n:20\r\n"
    );
}

#[test]
fn fcall_cached_runtime_returns_key_argument_across_repeated_calls() {
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let mut client = redis_core::Client::new(9);
    let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut ctx =
        redis_core::CommandContext::with_server_and_db_list(&mut client, &mut dbs, server, pubsub);

    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"FUNCTION"),
        RedisString::from_bytes(b"LOAD"),
        RedisString::from_bytes(b"REPLACE"),
        RedisString::from_bytes(
            b"#!lua name=cachetest_keys\n\
              server.register_function('cachetest_key', function(keys, args) return keys[1] end)",
        ),
    ]);
    function_load_command(&mut ctx).unwrap();
    assert_eq!(ctx.client_mut().drain_reply(), b"$14\r\ncachetest_keys\r\n");

    for _ in 0..2 {
        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"FCALL"),
            RedisString::from_bytes(b"cachetest_key"),
            RedisString::from_bytes(b"1"),
            RedisString::from_bytes(b"key1"),
        ]);
        fcall_command(&mut ctx).unwrap();
        assert_eq!(ctx.client_mut().drain_reply(), b"$4\r\nkey1\r\n");
    }
}

#[test]
fn fcall_cached_runtime_keeps_redis_call_bridge() {
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let mut client = redis_core::Client::new(10);
    let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut ctx =
        redis_core::CommandContext::with_server_and_db_list(&mut client, &mut dbs, server, pubsub);

    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"FUNCTION"),
        RedisString::from_bytes(b"LOAD"),
        RedisString::from_bytes(b"REPLACE"),
        RedisString::from_bytes(
            b"#!lua name=cachetest_call\n\
              server.register_function('cachetest_ping', function(keys, args) return server.call('ping') end)",
        ),
    ]);
    function_load_command(&mut ctx).unwrap();
    assert_eq!(ctx.client_mut().drain_reply(), b"$14\r\ncachetest_call\r\n");

    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"FCALL"),
        RedisString::from_bytes(b"cachetest_ping"),
        RedisString::from_bytes(b"0"),
    ]);
    fcall_command(&mut ctx).unwrap();
    assert_eq!(ctx.client_mut().drain_reply(), b"+PONG\r\n");
}

#[test]
fn function_load_replace_identical_library_preserves_behavior() {
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let mut client = redis_core::Client::new(11);
    let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut ctx =
        redis_core::CommandContext::with_server_and_db_list(&mut client, &mut dbs, server, pubsub);
    let library_name = b"cachetest_noop_replace";
    let code = b"#!lua name=cachetest_noop_replace\n\
                 server.register_function('cachetest_noop_fn', function(keys, args) return 42 end)";

    {
        let mut guard = match function_libraries().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.retain(|_, library| !ascii_eq_ci(&library.name, library_name));
    }

    for _ in 0..2 {
        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"FUNCTION"),
            RedisString::from_bytes(b"LOAD"),
            RedisString::from_bytes(b"REPLACE"),
            RedisString::from_bytes(code),
        ]);
        function_load_command(&mut ctx).unwrap();
        assert_eq!(
            ctx.client_mut().drain_reply(),
            b"$22\r\ncachetest_noop_replace\r\n"
        );
    }

    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"FCALL"),
        RedisString::from_bytes(b"cachetest_noop_fn"),
        RedisString::from_bytes(b"0"),
    ]);
    fcall_command(&mut ctx).unwrap();
    assert_eq!(ctx.client_mut().drain_reply(), b":42\r\n");

    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"FUNCTION"),
        RedisString::from_bytes(b"LOAD"),
        RedisString::from_bytes(code),
    ]);
    let err = function_load_command(&mut ctx).unwrap_err();
    assert!(err
        .to_resp_payload()
        .as_bytes()
        .windows(b"already exists".len())
        .any(|w| w == b"already exists"));

    let mut guard = match function_libraries().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.retain(|_, library| !ascii_eq_ci(&library.name, library_name));
}

#[test]
fn loaded_library_code_identity_matches_name_case_insensitively() {
    let mut libraries = HashMap::new();
    libraries.insert(
        b"BenchLib".to_vec(),
        LoadedFunctionLibrary {
            name: b"BenchLib".to_vec(),
            code: b"body".to_vec(),
            functions: Vec::new(),
            script_checks: FunctionScriptChecks::default(),
        },
    );

    assert!(loaded_library_code_is_identical(
        &libraries,
        b"benchlib",
        b"body"
    ));
    assert!(!loaded_library_code_is_identical(
        &libraries,
        b"benchlib",
        b"different"
    ));
    assert!(!loaded_library_code_is_identical(
        &libraries, b"other", b"body"
    ));
}

#[test]
fn function_source_eval_flags_finds_existing_broad_markers() {
    let flags = function_source_eval_flags(
        b"-- FLAGS=NO-WRITES\n#!LUA name=lib\n-- flags=ALLOW-OOM\n-- flags=allow-stale",
    );

    assert!(flags.has_shebang);
    assert!(flags.no_writes);
    assert!(flags.allow_oom);
    assert!(flags.allow_stale);

    let flags = function_source_eval_flags(b"flags=no_writes flags=allow,oom");
    assert!(!flags.no_writes);
    assert!(!flags.allow_oom);
}

#[test]
fn function_source_allows_oom_matches_existing_marker_rule() {
    assert!(function_source_allows_oom(
        b"#!lua name=lib\n-- FLAGS=ALLOW-OOM"
    ));
    assert!(!function_source_allows_oom(
        b"#!lua name=lib flags=no-writes,allow-oom"
    ));
}

#[test]
fn strip_embedded_eval_shebang_lines_borrows_when_unmodified() {
    let code = b"#!lua name=lib\nserver.register_function('f', function() return 1 end)";
    let stripped = strip_embedded_eval_shebang_lines(code);
    assert_eq!(stripped.as_ref(), code);
    assert!(matches!(stripped, std::borrow::Cow::Borrowed(_)));

    let code =
        b"#!lua name=lib\n#!lua flags=no-writes\nserver.register_function('f', function() return 1 end)";
    let stripped = strip_embedded_eval_shebang_lines(code);
    assert_eq!(
        stripped.as_ref(),
        b"#!lua name=lib\nserver.register_function('f', function() return 1 end)"
    );
    assert!(matches!(stripped, std::borrow::Cow::Owned(_)));
}

#[test]
fn run_inner_wait_is_script_safe() {
    let mut client = redis_core::Client::new(1);
    let mut outer: redis_core::Client = redis_core::Client::new(1);
    client.set_args(vec![
        RedisString::from_bytes(b"SET"),
        RedisString::from_bytes(b"x"),
        RedisString::from_bytes(b"1"),
    ]);
    let original_args = client.argv.clone();
    let mut ctx = CommandContext::new(&mut client);
    let reply = run_inner_command(
        &mut ctx,
        &[b"WAIT".to_vec(), b"1".to_vec(), b"0".to_vec()],
        None,
    )
    .unwrap();

    match reply {
        ReplyValue::Integer(v) => assert_eq!(v, 0),
        _ => panic!("expected integer reply from WAIT inside script"),
    }
    assert_eq!(client.argv, original_args);

    let mut wait_ctx = CommandContext::new(&mut outer);
    let wait_reply = run_inner_command(
        &mut wait_ctx,
        &[
            b"WAITAOF".to_vec(),
            b"0".to_vec(),
            b"1".to_vec(),
            b"0".to_vec(),
        ],
        None,
    )
    .unwrap();
    match wait_reply {
        ReplyValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert!(matches!(items[0], ReplyValue::Integer(0)));
            assert!(matches!(items[1], ReplyValue::Integer(0)));
        }
        _ => panic!("expected two-item array reply from WAITAOF inside script"),
    }

    wait_ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"waitaof"),
        RedisString::from_bytes(b"0"),
        RedisString::from_bytes(b"1"),
        RedisString::from_bytes(b"0"),
    ]);
    let direct = crate::dispatch::dispatch_command_name(&mut wait_ctx, b"waitaof");
    if direct.is_ok() {
        assert_eq!(wait_ctx.client_mut().drain_reply(), b"*2\r\n:0\r\n:0\r\n");
    } else {
        panic!("WAITAOF handler should be registered");
    }
}

#[test]
fn resp3_double_and_null_reply_shapes_match_lua_bridge() {
    let lua = Lua::new();

    let double = reply_to_lua(&lua, &ReplyValue::Double(1.25), 3).unwrap();
    match double {
        LuaValue::Table(t) => assert_eq!(t.raw_get::<f64>("double").unwrap(), 1.25),
        other => panic!("expected table for RESP3 double, got {other:?}"),
    }

    assert!(matches!(
        reply_to_lua(&lua, &ReplyValue::Null, 3).unwrap(),
        LuaValue::Nil
    ));
    assert!(matches!(
        reply_to_lua(&lua, &ReplyValue::Nil, 3).unwrap(),
        LuaValue::Boolean(false)
    ));
}

#[test]
fn map_reply_view_depends_on_setresp() {
    let lua = Lua::new();
    let reply = ReplyValue::Map(vec![
        ReplyValue::Bulk(b"field".to_vec()),
        ReplyValue::Bulk(b"value".to_vec()),
    ]);

    let resp3 = reply_to_lua(&lua, &reply, 3).unwrap();
    match resp3 {
        LuaValue::Table(t) => {
            let map: LuaTable = t.raw_get("map").unwrap();
            let v: mlua::String = map.get("field").unwrap();
            assert_eq!(v.as_bytes().as_ref(), b"value");
        }
        other => panic!("expected {{map=...}} under setresp(3), got {other:?}"),
    }

    let resp2 = reply_to_lua(&lua, &reply, 2).unwrap();
    match resp2 {
        LuaValue::Table(t) => {
            let f: mlua::String = t.raw_get(1).unwrap();
            let v: mlua::String = t.raw_get(2).unwrap();
            assert_eq!(f.as_bytes().as_ref(), b"field");
            assert_eq!(v.as_bytes().as_ref(), b"value");
            assert!(t.raw_get::<Option<LuaTable>>("map").unwrap().is_none());
        }
        other => panic!("expected flat array under setresp(2), got {other:?}"),
    }
}

#[test]
fn map_table_encodes_per_client_resp_version() {
    let lua = Lua::new();
    let table = lua.create_table().unwrap();
    let map = lua.create_table().unwrap();
    map.raw_set("field", "value").unwrap();
    table.raw_set("map", map).unwrap();
    let value = LuaValue::Table(table);

    let mut resp3 = Vec::new();
    lua_to_resp(&value, &mut resp3, true);
    assert_eq!(resp3, b"%1\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");

    let mut resp2 = Vec::new();
    lua_to_resp(&value, &mut resp2, false);
    assert_eq!(resp2, b"*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");
}

#[test]
fn recursive_table_reply_hits_lua_stack_limit_instead_of_overflowing() {
    let lua = Lua::new();
    let a = lua.create_table().unwrap();
    let b = lua.create_table().unwrap();
    b.raw_set(1, a.clone()).unwrap();
    a.raw_set(1, b).unwrap();

    let mut out = Vec::new();
    lua_to_resp(&LuaValue::Table(a), &mut out, true);

    assert!(out.starts_with(b"*1\r\n"));
    assert!(out.ends_with(b"-ERR reached lua stack limit\r\n"));
}

#[test]
fn lua_double_table_serializes_as_resp3_double() {
    let lua = Lua::new();
    let table = lua.create_table().unwrap();
    table.raw_set("double", 1.25).unwrap();
    let mut out = Vec::new();

    lua_to_resp(&LuaValue::Table(table), &mut out, true);

    assert_eq!(out, b",1.25\r\n");
}

#[test]
fn cmsgpack_pack_matches_upstream_numeric_vectors() {
    let lua = Lua::new();
    install_cmsgpack(&lua).unwrap();

    let double: mlua::String = lua.load("return cmsgpack.pack(0.1)").eval().unwrap();
    assert_eq!(
        &hex_encode(double.as_bytes().as_ref()),
        b"cb3fb999999999999a"
    );

    let negative: mlua::String = lua
        .load("return cmsgpack.pack(-1099511627776)")
        .eval()
        .unwrap();
    assert_eq!(
        &hex_encode(negative.as_bytes().as_ref()),
        b"d3ffffff0000000000"
    );
}

#[test]
fn cmsgpack_unpack_limit_uses_redis_offsets() {
    let lua = Lua::new();
    install_cmsgpack(&lua).unwrap();

    let ok: bool = lua
        .load(
            "local encoded = cmsgpack.pack('a', 'bb')\n\
             local offset, first = cmsgpack.unpack_limit(encoded, 1, 0)\n\
             local final_offset, second = cmsgpack.unpack_limit(encoded, 1, offset)\n\
             return first == 'a' and second == 'bb' and final_offset == -1",
        )
        .eval()
        .unwrap();
    assert!(ok);
}

#[test]
fn cmsgpack_circular_cutoff_matches_upstream_depth_vector() {
    let lua = Lua::new();
    install_cmsgpack(&lua).unwrap();

    let packed: mlua::String = lua
        .load(
            "local a = {x=nil,y=5}\n\
             local b = {x=a}\n\
             a['x'] = b\n\
             return cmsgpack.pack(a)",
        )
        .eval()
        .unwrap();
    assert_eq!(
        &hex_encode(packed.as_bytes().as_ref()),
        b"82a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a178c0"
    );
}

#[test]
fn bit_minimal_bitop_matches_upstream() {
    let lua = Lua::new();
    install_bit(&lua).unwrap();

    let ok: bool = lua
        .load(
            "return bit.tobit(1) == 1\n\
             and bit.band(1) == 1\n\
             and bit.bxor(1, 2) == 3\n\
             and bit.bor(1, 2, 4, 8, 16, 32, 64, 128) == 255",
        )
        .eval()
        .unwrap();
    assert!(ok);
}

#[test]
fn bit_tohex_int32_min_width_matches_upstream() {
    let lua = Lua::new();
    install_bit(&lua).unwrap();

    let hex: mlua::String = lua
        .load("return bit.tohex(65535, -2147483648)")
        .eval()
        .unwrap();
    assert_eq!(hex.as_bytes().as_ref(), b"0000FFFF");
}

#[test]
fn bit_shifts_use_32bit_wrapping_semantics() {
    let lua = Lua::new();
    install_bit(&lua).unwrap();

    let ok: bool = lua
        .load(
            "return bit.bnot(0) == -1\n\
             and bit.lshift(1, 31) == -2147483648\n\
             and bit.rshift(-2147483648, 31) == 1\n\
             and bit.arshift(-2147483648, 31) == -1\n\
             and bit.rol(0x12345678, 12) == bit.tobit(0x45678123)\n\
             and bit.bswap(0x12345678) == bit.tobit(0x78563412)",
        )
        .eval()
        .unwrap();
    assert!(ok);
}

#[test]
fn bit_table_is_readonly() {
    let lua = Lua::new();
    install_bit(&lua).unwrap();

    let err = lua
        .load("bit.lshift = function() return 1 end")
        .exec()
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Attempt to modify a readonly table"));
}

#[test]
fn os_sandbox_exposes_only_clock() {
    let lua = Lua::new();
    install_sandbox(&lua).unwrap();

    let only_clock: bool = lua
        .load(
            "local keys = {}\n\
             for k, v in pairs(os) do keys[#keys + 1] = k .. ':' .. type(v) end\n\
             return #keys == 1 and keys[1] == 'clock:function'",
        )
        .eval()
        .unwrap();
    assert!(only_clock);
}

#[test]
fn os_clock_measures_elapsed_delta() {
    let lua = Lua::new();
    install_sandbox(&lua).unwrap();

    let nonnegative: bool = lua
        .load("local s = os.clock(); local e = os.clock(); return e - s >= 0")
        .eval()
        .unwrap();
    assert!(nonnegative);
}

#[test]
fn os_dangerous_methods_are_absent() {
    let lua = Lua::new();
    install_sandbox(&lua).unwrap();

    let err = lua.load("os.execute()").exec().unwrap_err();
    assert!(err.to_string().contains("attempt to call field 'execute'"));
}
