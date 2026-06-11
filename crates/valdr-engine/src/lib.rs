//! Wasm-safe embedded Valdr command engine.
//!
//! This crate is intentionally smaller than `redis-core` + `redis-commands`.
//! It is the first EdgeStash boundary: no networking, TLS, process APIs,
//! background workers, native filesystem access, or C Lua.

use std::cell::RefCell;
use std::collections::HashMap;

use lua_rs_runtime::{
    Lua, LuaError, LuaString, LuaVersion, Table as LuaTable, Value as LuaValue, Variadic,
};
use redis_protocol::{encode_resp2, RespFrame};
use redis_types::RedisString;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostError {
    Unavailable,
    Message(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    InvalidJson,
    InvalidVersion,
    MissingField(&'static str),
    InvalidField(&'static str),
    InvalidHex,
}

pub trait Host {
    fn now_millis(&self) -> u64;

    fn random_bytes(&mut self, _out: &mut [u8]) -> Result<(), HostError> {
        Err(HostError::Unavailable)
    }

    fn persist_append(&mut self, _record: &[u8]) -> Result<(), HostError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct NoopHost {
    now_millis: u64,
}

impl NoopHost {
    pub fn new(now_millis: u64) -> Self {
        Self { now_millis }
    }

    pub fn set_now_millis(&mut self, now_millis: u64) {
        self.now_millis = now_millis;
    }
}

impl Host for NoopHost {
    fn now_millis(&self) -> u64 {
        self.now_millis
    }
}

#[derive(Debug, Clone)]
struct Entry {
    value: StoredValue,
    expire_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
enum StoredValue {
    String(Vec<u8>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
}

#[derive(Debug, Clone)]
pub struct Engine<H> {
    host: H,
    db: HashMap<Vec<u8>, Entry>,
    scripts: HashMap<[u8; 40], Vec<u8>>,
}

impl Engine<NoopHost> {
    pub fn new_in_memory() -> Self {
        Self::new(NoopHost::default())
    }
}

impl<H: Host> Engine<H> {
    pub fn new(host: H) -> Self {
        Self {
            host,
            db: HashMap::new(),
            scripts: HashMap::new(),
        }
    }

    pub fn host(&self) -> &H {
        &self.host
    }

    pub fn host_mut(&mut self) -> &mut H {
        &mut self.host
    }

    pub fn execute(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        self.execute_inner(argv, false)
    }

    pub fn export_snapshot(&mut self) -> Vec<u8> {
        self.purge_expired_keys();

        let mut keys: Vec<_> = self.db.iter().collect();
        keys.sort_by(|(left, _), (right, _)| left.cmp(right));

        let mut encoded_keys = Vec::with_capacity(keys.len());
        for (key, entry) in keys {
            let mut object = JsonMap::new();
            object.insert("key".to_owned(), JsonValue::String(hex_encode(key)));
            if let Some(expire_at_ms) = entry.expire_at_ms {
                object.insert("expire_at_ms".to_owned(), json!(expire_at_ms));
            }
            match &entry.value {
                StoredValue::String(value) => {
                    object.insert("type".to_owned(), JsonValue::String("string".to_owned()));
                    object.insert("value".to_owned(), JsonValue::String(hex_encode(value)));
                }
                StoredValue::Hash(fields) => {
                    let mut field_items: Vec<_> = fields.iter().collect();
                    field_items.sort_by(|(left, _), (right, _)| left.cmp(right));
                    object.insert("type".to_owned(), JsonValue::String("hash".to_owned()));
                    object.insert(
                        "fields".to_owned(),
                        JsonValue::Array(
                            field_items
                                .into_iter()
                                .map(|(field, value)| {
                                    JsonValue::Array(vec![
                                        JsonValue::String(hex_encode(field)),
                                        JsonValue::String(hex_encode(value)),
                                    ])
                                })
                                .collect(),
                        ),
                    );
                }
            }
            encoded_keys.push(JsonValue::Object(object));
        }

        serde_json::to_vec(&json!({
            "format": "valdr-engine-snapshot",
            "version": 1,
            "keys": encoded_keys,
        }))
        .unwrap_or_else(|_| {
            b"{\"format\":\"valdr-engine-snapshot\",\"version\":1,\"keys\":[]}".to_vec()
        })
    }

    pub fn import_snapshot(&mut self, snapshot: &[u8]) -> Result<(), SnapshotError> {
        let json: JsonValue =
            serde_json::from_slice(snapshot).map_err(|_| SnapshotError::InvalidJson)?;
        if json.get("format").and_then(JsonValue::as_str) != Some("valdr-engine-snapshot") {
            return Err(SnapshotError::InvalidField("format"));
        }
        if json.get("version").and_then(JsonValue::as_u64) != Some(1) {
            return Err(SnapshotError::InvalidVersion);
        }
        let keys = json
            .get("keys")
            .and_then(JsonValue::as_array)
            .ok_or(SnapshotError::MissingField("keys"))?;

        let mut next_db = HashMap::new();
        for item in keys {
            let object = item
                .as_object()
                .ok_or(SnapshotError::InvalidField("keys"))?;
            let key = hex_decode(
                object
                    .get("key")
                    .and_then(JsonValue::as_str)
                    .ok_or(SnapshotError::MissingField("key"))?,
            )?;
            let expire_at_ms = match object.get("expire_at_ms") {
                Some(value) => Some(
                    value
                        .as_u64()
                        .ok_or(SnapshotError::InvalidField("expire_at_ms"))?,
                ),
                None => None,
            };
            let value = match object
                .get("type")
                .and_then(JsonValue::as_str)
                .ok_or(SnapshotError::MissingField("type"))?
            {
                "string" => StoredValue::String(hex_decode(
                    object
                        .get("value")
                        .and_then(JsonValue::as_str)
                        .ok_or(SnapshotError::MissingField("value"))?,
                )?),
                "hash" => {
                    let fields = object
                        .get("fields")
                        .and_then(JsonValue::as_array)
                        .ok_or(SnapshotError::MissingField("fields"))?;
                    let mut decoded_fields = HashMap::new();
                    for pair in fields {
                        let pair = pair
                            .as_array()
                            .ok_or(SnapshotError::InvalidField("fields"))?;
                        if pair.len() != 2 {
                            return Err(SnapshotError::InvalidField("fields"));
                        }
                        let field = hex_decode(
                            pair[0]
                                .as_str()
                                .ok_or(SnapshotError::InvalidField("fields"))?,
                        )?;
                        let value = hex_decode(
                            pair[1]
                                .as_str()
                                .ok_or(SnapshotError::InvalidField("fields"))?,
                        )?;
                        decoded_fields.insert(field, value);
                    }
                    StoredValue::Hash(decoded_fields)
                }
                _ => return Err(SnapshotError::InvalidField("type")),
            };
            next_db.insert(
                key,
                Entry {
                    value,
                    expire_at_ms,
                },
            );
        }

        self.db = next_db;
        self.scripts.clear();
        Ok(())
    }

    pub fn execute_rest(&mut self, request: RestRequest<'_>) -> RestResponse {
        if !matches!(
            request.method,
            RestMethod::Get | RestMethod::Post | RestMethod::Put | RestMethod::Head
        ) {
            return RestResponse::json_error(405, b"ERR unsupported HTTP method");
        }

        let command_result = match rest_command_from_request(request) {
            Ok(RestCommand::Single(argv)) => {
                let frame = self.execute(&argv);
                if request.method == RestMethod::Head {
                    return RestResponse {
                        status: rest_status_for_frame(&frame),
                        content_type: rest_content_type(request.response_format),
                        body: Vec::new(),
                    };
                }
                match request.response_format {
                    RestResponseFormat::Json => RestResponse::json_frame(frame),
                    RestResponseFormat::Resp2 => RestResponse::resp2_frame(frame),
                }
            }
            Ok(RestCommand::Pipeline(commands)) => {
                let mut items = Vec::with_capacity(commands.len());
                for argv in commands {
                    items.push(rest_frame_json(self.execute(&argv)));
                }
                match serde_json::to_vec(&JsonValue::Array(items)) {
                    Ok(body) => RestResponse {
                        status: 200,
                        content_type: APPLICATION_JSON,
                        body,
                    },
                    Err(_) => RestResponse::json_error(500, b"ERR JSON encode failed"),
                }
            }
            Err(error) => RestResponse::json_error(400, &error),
        };

        command_result
    }

    fn execute_inner(&mut self, argv: &[Vec<u8>], from_script: bool) -> RespFrame {
        let Some(command) = argv.first() else {
            return err(b"ERR unknown command ''");
        };
        if from_script && script_blocked_command(command) {
            return err(b"ERR This Redis command is not allowed from script");
        }

        if ascii_eq(command, b"GET") {
            self.get_command(argv)
        } else if ascii_eq(command, b"SET") {
            self.set_command(argv)
        } else if ascii_eq(command, b"SETEX") {
            self.setex_command(argv)
        } else if ascii_eq(command, b"DEL") {
            self.del_command(argv)
        } else if ascii_eq(command, b"EXISTS") {
            self.exists_command(argv)
        } else if ascii_eq(command, b"INCR") {
            self.incrby_command(argv, 1)
        } else if ascii_eq(command, b"INCRBY") {
            self.incrby_command_from_argv(argv)
        } else if ascii_eq(command, b"EXPIRE") {
            self.expire_command(argv, 1000)
        } else if ascii_eq(command, b"PEXPIRE") {
            self.expire_command(argv, 1)
        } else if ascii_eq(command, b"TTL") {
            self.ttl_command(argv, false)
        } else if ascii_eq(command, b"PTTL") {
            self.ttl_command(argv, true)
        } else if ascii_eq(command, b"HSET") {
            self.hset_command(argv)
        } else if ascii_eq(command, b"HGET") {
            self.hget_command(argv)
        } else if ascii_eq(command, b"HGETALL") {
            self.hgetall_command(argv)
        } else if ascii_eq(command, b"HDEL") {
            self.hdel_command(argv)
        } else if ascii_eq(command, b"SCRIPT") {
            self.script_command(argv)
        } else if ascii_eq(command, b"EVAL") {
            self.eval_command(argv)
        } else if ascii_eq(command, b"EVALSHA") {
            self.evalsha_command(argv)
        } else {
            let mut msg = b"ERR unknown command '".to_vec();
            msg.extend_from_slice(command);
            msg.push(b'\'');
            err(&msg)
        }
    }

    fn get_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"get");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => bulk(value),
            Some(StoredValue::Hash(_)) => wrong_type(),
            None => RespFrame::null_bulk(),
        }
    }

    fn set_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"set");
        }
        let mut expire_at_ms = None;
        let mut index = 3;
        while index < argv.len() {
            if ascii_eq(&argv[index], b"PX") || ascii_eq(&argv[index], b"EX") {
                if index + 1 >= argv.len() {
                    return err(b"ERR syntax error");
                }
                let Some(raw) = parse_i64(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if raw <= 0 {
                    return err(b"ERR invalid expire time in 'set' command");
                }
                let unit = if ascii_eq(&argv[index], b"PX") {
                    1
                } else {
                    1000
                };
                let Some(ttl) = checked_ttl_ms(raw, unit) else {
                    return err(b"ERR invalid expire time in 'set' command");
                };
                expire_at_ms = self.host.now_millis().checked_add(ttl);
                if expire_at_ms.is_none() {
                    return err(b"ERR invalid expire time in 'set' command");
                }
                index += 2;
            } else {
                return err(b"ERR syntax error");
            }
        }
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[2].clone()),
                expire_at_ms,
            },
        );
        simple(b"OK")
    }

    fn setex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"setex");
        }
        let Some(seconds) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if seconds <= 0 {
            return err(b"ERR invalid expire time in 'setex' command");
        }
        let Some(ttl) = checked_ttl_ms(seconds, 1000) else {
            return err(b"ERR invalid expire time in 'setex' command");
        };
        let Some(expire_at_ms) = self.host.now_millis().checked_add(ttl) else {
            return err(b"ERR invalid expire time in 'setex' command");
        };
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[3].clone()),
                expire_at_ms: Some(expire_at_ms),
            },
        );
        simple(b"OK")
    }

    fn del_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"del");
        }
        let mut deleted = 0;
        for key in &argv[1..] {
            self.purge_if_expired(key);
            if self.db.remove(key).is_some() {
                deleted += 1;
            }
        }
        RespFrame::integer(deleted)
    }

    fn exists_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"exists");
        }
        let mut count = 0;
        for key in &argv[1..] {
            if self.get_value(key).is_some() {
                count += 1;
            }
        }
        RespFrame::integer(count)
    }

    fn incrby_command_from_argv(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"incrby");
        }
        let Some(delta) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        self.incrby_command(argv, delta)
    }

    fn incrby_command(&mut self, argv: &[Vec<u8>], delta: i64) -> RespFrame {
        if argv.len() != 2 && argv.len() != 3 {
            return wrong_arity(if argv.first().is_some_and(|c| ascii_eq(c, b"INCR")) {
                b"incr"
            } else {
                b"incrby"
            });
        }
        let current = match self.get_value(&argv[1]) {
            Some(Entry {
                value: StoredValue::String(value),
                ..
            }) => match parse_i64(value) {
                Some(n) => n,
                None => return err(b"ERR value is not an integer or out of range"),
            },
            Some(Entry {
                value: StoredValue::Hash(_),
                ..
            }) => return wrong_type(),
            None => 0,
        };
        let Some(next) = current.checked_add(delta) else {
            return err(b"ERR increment or decrement would overflow");
        };
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(next.to_string().into_bytes()),
                expire_at_ms,
            },
        );
        RespFrame::integer(next)
    }

    fn hset_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 || argv.len() % 2 != 0 {
            return wrong_arity(b"hset");
        }
        self.purge_if_expired(&argv[1]);

        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                let mut added = 0;
                for pair in argv[2..].chunks_exact(2) {
                    if fields.insert(pair[0].clone(), pair[1].clone()).is_none() {
                        added += 1;
                    }
                }
                RespFrame::integer(added)
            }
            Some(Entry {
                value: StoredValue::String(_),
                ..
            }) => wrong_type(),
            None => {
                let mut fields = HashMap::new();
                let mut added = 0;
                for pair in argv[2..].chunks_exact(2) {
                    if fields.insert(pair[0].clone(), pair[1].clone()).is_none() {
                        added += 1;
                    }
                }
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(fields),
                        expire_at_ms,
                    },
                );
                RespFrame::integer(added)
            }
        }
    }

    fn hget_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"hget");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => match fields.get(&argv[2]) {
                Some(value) => bulk(value),
                None => RespFrame::null_bulk(),
            },
            Some(StoredValue::String(_)) => wrong_type(),
            None => RespFrame::null_bulk(),
        }
    }

    fn hgetall_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"hgetall");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                let mut pairs: Vec<_> = fields.iter().collect();
                pairs.sort_by(|(left, _), (right, _)| left.cmp(right));
                let mut items = Vec::with_capacity(fields.len() * 2);
                for (field, value) in pairs {
                    items.push(bulk(field));
                    items.push(bulk(value));
                }
                RespFrame::array(items)
            }
            Some(StoredValue::String(_)) => wrong_type(),
            None => RespFrame::array(Vec::new()),
        }
    }

    fn hdel_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"hdel");
        }
        self.purge_if_expired(&argv[1]);
        let mut remove_empty_hash = false;
        let response = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                let mut deleted = 0;
                for field in &argv[2..] {
                    if fields.remove(field).is_some() {
                        deleted += 1;
                    }
                }
                remove_empty_hash = fields.is_empty();
                RespFrame::integer(deleted)
            }
            Some(Entry {
                value: StoredValue::String(_),
                ..
            }) => wrong_type(),
            None => RespFrame::integer(0),
        };
        if remove_empty_hash {
            self.db.remove(&argv[1]);
        }
        response
    }

    fn expire_command(&mut self, argv: &[Vec<u8>], unit_ms: u64) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(if unit_ms == 1 { b"pexpire" } else { b"expire" });
        }
        let Some(raw) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if raw <= 0 {
            self.purge_if_expired(&argv[1]);
            let existed = self.db.remove(&argv[1]).is_some();
            return RespFrame::integer(if existed { 1 } else { 0 });
        }
        self.purge_if_expired(&argv[1]);
        let Some(ttl) = checked_ttl_ms(raw, unit_ms) else {
            return err(b"ERR invalid expire time");
        };
        let Some(expire_at_ms) = self.host.now_millis().checked_add(ttl) else {
            return err(b"ERR invalid expire time");
        };
        match self.db.get_mut(&argv[1]) {
            Some(entry) => {
                entry.expire_at_ms = Some(expire_at_ms);
                RespFrame::integer(1)
            }
            None => RespFrame::integer(0),
        }
    }

    fn ttl_command(&mut self, argv: &[Vec<u8>], milliseconds: bool) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(if milliseconds { b"pttl" } else { b"ttl" });
        }
        self.purge_if_expired(&argv[1]);
        let Some(entry) = self.db.get(&argv[1]) else {
            return RespFrame::integer(-2);
        };
        let Some(expire_at_ms) = entry.expire_at_ms else {
            return RespFrame::integer(-1);
        };
        let remaining = expire_at_ms.saturating_sub(self.host.now_millis());
        if milliseconds {
            RespFrame::integer(remaining as i64)
        } else {
            RespFrame::integer(remaining.div_ceil(1000) as i64)
        }
    }

    fn script_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"script");
        }
        if ascii_eq(&argv[1], b"LOAD") {
            if argv.len() != 3 {
                return wrong_arity(b"script|load");
            }
            let sha = sha1_hex(&argv[2]);
            self.scripts.insert(sha, argv[2].clone());
            bulk(sha.to_vec())
        } else if ascii_eq(&argv[1], b"EXISTS") {
            if argv.len() < 3 {
                return wrong_arity(b"script|exists");
            }
            RespFrame::array(
                argv[2..]
                    .iter()
                    .map(|raw| {
                        let exists = normalise_sha(raw)
                            .map(|sha| self.scripts.contains_key(&sha))
                            .unwrap_or(false);
                        RespFrame::integer(if exists { 1 } else { 0 })
                    })
                    .collect(),
            )
        } else if ascii_eq(&argv[1], b"FLUSH") {
            if argv.len() > 3 {
                return wrong_arity(b"script|flush");
            }
            self.scripts.clear();
            simple(b"OK")
        } else {
            err(b"ERR Unknown SCRIPT subcommand")
        }
    }

    fn eval_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"eval");
        }
        self.eval_script(&argv[1], &argv[2..])
    }

    fn evalsha_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"evalsha");
        }
        let Some(sha) = normalise_sha(&argv[1]) else {
            return err(b"NOSCRIPT No matching script. Please use EVAL.");
        };
        let Some(script) = self.scripts.get(&sha).cloned() else {
            return err(b"NOSCRIPT No matching script. Please use EVAL.");
        };
        self.eval_script(&script, &argv[2..])
    }

    fn eval_script(&mut self, script: &[u8], rest: &[Vec<u8>]) -> RespFrame {
        let Some(numkeys) = rest.first().and_then(|arg| parse_usize(arg)) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if rest.len() < 1 + numkeys {
            return err(b"ERR Number of keys can't be greater than number of args");
        }
        let keys = &rest[1..1 + numkeys];
        let args = &rest[1 + numkeys..];
        match self.run_lua_script(script, keys, args) {
            Ok(frame) => frame,
            Err(error) => {
                let mut msg = b"ERR ".to_vec();
                msg.extend_from_slice(error.message_lossy().as_bytes());
                err(&msg)
            }
        }
    }

    fn run_lua_script(
        &mut self,
        script: &[u8],
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
    ) -> lua_rs_runtime::Result<RespFrame> {
        let lua = Lua::new_versioned(LuaVersion::V51);
        install_sandbox(&lua)?;
        install_keys_argv(&lua, keys, args)?;

        let engine_cell: RefCell<&mut Engine<H>> = RefCell::new(self);
        let value = lua.scope(|scope| {
            let redis_table = lua.create_table()?;

            let call_fn = {
                let cell = &engine_cell;
                scope.create_function_mut(
                    &lua,
                    move |lua_inner, call_args: Variadic<LuaValue>| {
                        let argv = collect_lua_args(call_args)?;
                        let mut engine = cell.borrow_mut();
                        match engine.execute_inner(&argv, true) {
                            RespFrame::Error(message) => {
                                Err(lua_runtime_error_bytes(message.as_bytes()))
                            }
                            frame => resp_to_lua(lua_inner, &frame),
                        }
                    },
                )?
            };

            let pcall_fn = {
                let cell = &engine_cell;
                scope.create_function_mut(
                    &lua,
                    move |lua_inner, call_args: Variadic<LuaValue>| {
                        let argv = collect_lua_args(call_args)?;
                        let mut engine = cell.borrow_mut();
                        match engine.execute_inner(&argv, true) {
                            RespFrame::Error(message) => error_table(lua_inner, message.as_bytes()),
                            frame => resp_to_lua(lua_inner, &frame),
                        }
                    },
                )?
            };

            let status_reply_fn = lua.create_function(
                |lua_inner, msg: LuaString| -> lua_rs_runtime::Result<LuaTable> {
                    let table = lua_inner.create_table()?;
                    table.set("ok", msg)?;
                    Ok(table)
                },
            )?;
            let error_reply_fn = lua.create_function(
                |lua_inner, msg: LuaString| -> lua_rs_runtime::Result<LuaTable> {
                    let table = lua_inner.create_table()?;
                    table.set("err", msg)?;
                    Ok(table)
                },
            )?;
            let sha1hex_fn = lua.create_function(
                |_lua_inner, msg: LuaString| -> lua_rs_runtime::Result<String> {
                    let hex = sha1_hex(&msg.as_bytes()?);
                    Ok(String::from_utf8(hex.to_vec()).unwrap_or_default())
                },
            )?;

            redis_table.set("call", call_fn)?;
            redis_table.set("pcall", pcall_fn)?;
            redis_table.set("status_reply", status_reply_fn)?;
            redis_table.set("error_reply", error_reply_fn)?;
            redis_table.set("sha1hex", sha1hex_fn)?;
            lua.globals().set("redis", redis_table.clone())?;
            lua.globals().set("server", redis_table)?;

            lua.load(script).set_name("edge_script").eval::<LuaValue>()
        })?;
        lua_to_resp(&value)
    }

    fn get_value(&mut self, key: &[u8]) -> Option<&Entry> {
        self.purge_if_expired(key);
        self.db.get(key)
    }

    fn purge_if_expired(&mut self, key: &[u8]) {
        let expired = self
            .db
            .get(key)
            .and_then(|entry| entry.expire_at_ms)
            .is_some_and(|deadline| deadline <= self.host.now_millis());
        if expired {
            self.db.remove(key);
        }
    }

    fn purge_expired_keys(&mut self) {
        let now = self.host.now_millis();
        self.db
            .retain(|_, entry| !entry.expire_at_ms.is_some_and(|deadline| deadline <= now));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestMethod {
    Get,
    Post,
    Put,
    Head,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestResponseFormat {
    Json,
    Resp2,
}

#[derive(Debug, Clone, Copy)]
pub struct RestRequest<'a> {
    pub method: RestMethod,
    pub path: &'a str,
    pub body: &'a [u8],
    pub response_format: RestResponseFormat,
}

impl<'a> RestRequest<'a> {
    pub fn get(path: &'a str) -> Self {
        Self {
            method: RestMethod::Get,
            path,
            body: &[],
            response_format: RestResponseFormat::Json,
        }
    }

    pub fn post(path: &'a str, body: &'a [u8]) -> Self {
        Self {
            method: RestMethod::Post,
            path,
            body,
            response_format: RestResponseFormat::Json,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

impl RestResponse {
    fn json_frame(frame: RespFrame) -> Self {
        let status = rest_status_for_frame(&frame);
        match serde_json::to_vec(&rest_frame_json(frame)) {
            Ok(body) => Self {
                status,
                content_type: APPLICATION_JSON,
                body,
            },
            Err(_) => Self::json_error(500, b"ERR JSON encode failed"),
        }
    }

    fn resp2_frame(frame: RespFrame) -> Self {
        let status = rest_status_for_frame(&frame);
        let mut body = Vec::new();
        encode_resp2(&frame, &mut body);
        Self {
            status,
            content_type: APPLICATION_OCTET_STREAM,
            body,
        }
    }

    fn json_error(status: u16, message: &[u8]) -> Self {
        let body = serde_json::to_vec(&json!({
            "error": String::from_utf8_lossy(message)
        }))
        .unwrap_or_else(|_| b"{\"error\":\"ERR JSON encode failed\"}".to_vec());
        Self {
            status,
            content_type: APPLICATION_JSON,
            body,
        }
    }
}

const APPLICATION_JSON: &str = "application/json";
const APPLICATION_OCTET_STREAM: &str = "application/octet-stream";

enum RestCommand {
    Single(Vec<Vec<u8>>),
    Pipeline(Vec<Vec<Vec<u8>>>),
}

fn rest_command_from_request(request: RestRequest<'_>) -> Result<RestCommand, Vec<u8>> {
    let (path, query) = split_path_query(request.path);
    let segments = parse_path_segments(path)?;
    let is_pipeline = segments
        .first()
        .is_some_and(|segment| ascii_eq(segment, b"pipeline"));

    if is_pipeline {
        if !matches!(request.method, RestMethod::Post | RestMethod::Put) {
            return Err(b"ERR pipeline requires POST or PUT".to_vec());
        }
        return parse_pipeline_body(request.body);
    }

    if segments.is_empty() {
        if !matches!(request.method, RestMethod::Post | RestMethod::Put) {
            return Err(b"ERR missing command".to_vec());
        }
        return parse_single_command_body(request.body).map(RestCommand::Single);
    }

    if matches!(request.method, RestMethod::Post | RestMethod::Put)
        && body_looks_like_json_array(request.body)
    {
        let argv = parse_single_command_body(request.body)?;
        if argv
            .first()
            .is_some_and(|command| ascii_eq(command, &segments[0]))
        {
            return Ok(RestCommand::Single(argv));
        }
    }

    let mut argv = segments;
    if matches!(request.method, RestMethod::Post | RestMethod::Put) && !request.body.is_empty() {
        argv.push(request.body.to_vec());
    }
    append_query_args(query, &mut argv)?;
    Ok(RestCommand::Single(argv))
}

fn split_path_query(path: &str) -> (&str, &str) {
    match path.split_once('?') {
        Some((path, query)) => (path, query),
        None => (path, ""),
    }
}

fn parse_path_segments(path: &str) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| percent_decode(segment.as_bytes(), false))
        .collect()
}

fn append_query_args(query: &str, argv: &mut Vec<Vec<u8>>) -> Result<(), Vec<u8>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == "_token" {
            continue;
        }
        argv.push(percent_decode(key.as_bytes(), true)?);
        if !value.is_empty() {
            argv.push(percent_decode(value.as_bytes(), true)?);
        }
    }
    Ok(())
}

fn percent_decode(input: &[u8], plus_as_space: bool) -> Result<Vec<u8>, Vec<u8>> {
    let mut out = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        match input[index] {
            b'%' => {
                if index + 2 >= input.len() {
                    return Err(b"ERR invalid URL escape".to_vec());
                }
                let Some(high) = hex_nibble(input[index + 1]) else {
                    return Err(b"ERR invalid URL escape".to_vec());
                };
                let Some(low) = hex_nibble(input[index + 2]) else {
                    return Err(b"ERR invalid URL escape".to_vec());
                };
                out.push((high << 4) | low);
                index += 3;
            }
            b'+' if plus_as_space => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn body_looks_like_json_array(body: &[u8]) -> bool {
    body.iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'[')
}

fn parse_single_command_body(body: &[u8]) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let json: JsonValue =
        serde_json::from_slice(body).map_err(|_| b"ERR invalid JSON command body".to_vec())?;
    json_array_to_argv(&json)
}

fn parse_pipeline_body(body: &[u8]) -> Result<RestCommand, Vec<u8>> {
    let json: JsonValue =
        serde_json::from_slice(body).map_err(|_| b"ERR invalid JSON pipeline body".to_vec())?;
    let Some(rows) = json.as_array() else {
        return Err(b"ERR pipeline body must be an array".to_vec());
    };
    let mut commands = Vec::with_capacity(rows.len());
    for row in rows {
        commands.push(json_array_to_argv(row)?);
    }
    Ok(RestCommand::Pipeline(commands))
}

fn json_array_to_argv(value: &JsonValue) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let Some(items) = value.as_array() else {
        return Err(b"ERR command body must be an array".to_vec());
    };
    if items.is_empty() {
        return Err(b"ERR command body must not be empty".to_vec());
    }
    let mut argv = Vec::with_capacity(items.len());
    for item in items {
        argv.push(json_arg_to_bytes(item)?);
    }
    Ok(argv)
}

fn json_arg_to_bytes(value: &JsonValue) -> Result<Vec<u8>, Vec<u8>> {
    match value {
        JsonValue::String(value) => Ok(value.as_bytes().to_vec()),
        JsonValue::Number(value) => Ok(value.to_string().into_bytes()),
        JsonValue::Bool(true) => Ok(b"true".to_vec()),
        JsonValue::Bool(false) => Ok(b"false".to_vec()),
        JsonValue::Null => Ok(b"null".to_vec()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            Err(b"ERR command arguments must be scalar JSON values".to_vec())
        }
    }
}

fn rest_status_for_frame(frame: &RespFrame) -> u16 {
    match frame {
        RespFrame::Error(_) | RespFrame::BulkError(_) => 400,
        _ => 200,
    }
}

fn rest_content_type(format: RestResponseFormat) -> &'static str {
    match format {
        RestResponseFormat::Json => APPLICATION_JSON,
        RestResponseFormat::Resp2 => APPLICATION_OCTET_STREAM,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, SnapshotError> {
    let bytes = text.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(SnapshotError::InvalidHex);
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = hex_nibble(pair[0]).ok_or(SnapshotError::InvalidHex)?;
        let low = hex_nibble(pair[1]).ok_or(SnapshotError::InvalidHex)?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn rest_frame_json(frame: RespFrame) -> JsonValue {
    match frame {
        RespFrame::Error(message) | RespFrame::BulkError(message) => json!({
            "error": String::from_utf8_lossy(message.as_bytes())
        }),
        other => json!({
            "result": resp_frame_result_json(other)
        }),
    }
}

fn resp_frame_result_json(frame: RespFrame) -> JsonValue {
    match frame {
        RespFrame::Simple(value)
        | RespFrame::Bulk(Some(value))
        | RespFrame::BigNumber(value)
        | RespFrame::VerbatimString { data: value, .. } => {
            JsonValue::String(String::from_utf8_lossy(value.as_bytes()).into_owned())
        }
        RespFrame::Integer(value) => json!(value),
        RespFrame::Bulk(None) | RespFrame::Array(None) | RespFrame::Null => JsonValue::Null,
        RespFrame::Array(Some(items)) | RespFrame::Push(items) | RespFrame::Set(items) => {
            JsonValue::Array(items.into_iter().map(resp_frame_result_json).collect())
        }
        RespFrame::Boolean(value) => JsonValue::Bool(value),
        RespFrame::Double(value) => json!(value),
        RespFrame::Map(pairs) | RespFrame::Attribute(pairs) => JsonValue::Array(
            pairs
                .into_iter()
                .flat_map(|(key, value)| {
                    [resp_frame_result_json(key), resp_frame_result_json(value)]
                })
                .collect(),
        ),
        RespFrame::Error(message) | RespFrame::BulkError(message) => {
            JsonValue::String(String::from_utf8_lossy(message.as_bytes()).into_owned())
        }
    }
}

fn install_sandbox(lua: &Lua) -> lua_rs_runtime::Result<()> {
    let globals = lua.globals();
    for name in [
        "io",
        "debug",
        "package",
        "require",
        "load",
        "loadfile",
        "dofile",
        "loadstring",
        "print",
        "getfenv",
        "setfenv",
    ] {
        globals.set(name, LuaValue::Nil)?;
    }
    Ok(())
}

fn install_keys_argv(lua: &Lua, keys: &[Vec<u8>], args: &[Vec<u8>]) -> lua_rs_runtime::Result<()> {
    let keys_table = lua.create_table()?;
    for (index, key) in keys.iter().enumerate() {
        keys_table.set(index as i64 + 1, lua.create_string(key)?)?;
    }
    lua.globals().set("KEYS", keys_table)?;

    let argv_table = lua.create_table()?;
    for (index, arg) in args.iter().enumerate() {
        argv_table.set(index as i64 + 1, lua.create_string(arg)?)?;
    }
    lua.globals().set("ARGV", argv_table)?;
    Ok(())
}

fn resp_to_lua(lua: &Lua, frame: &RespFrame) -> lua_rs_runtime::Result<LuaValue> {
    match frame {
        RespFrame::Simple(value) => {
            let table = lua.create_table()?;
            table.set("ok", lua.create_string(value.as_bytes())?)?;
            Ok(LuaValue::Table(table))
        }
        RespFrame::Error(value) | RespFrame::BulkError(value) => {
            let table = lua.create_table()?;
            table.set("err", lua.create_string(value.as_bytes())?)?;
            Ok(LuaValue::Table(table))
        }
        RespFrame::Integer(value) => Ok(LuaValue::Integer(*value)),
        RespFrame::Bulk(Some(value)) => Ok(LuaValue::String(lua.create_string(value.as_bytes())?)),
        RespFrame::Bulk(None) | RespFrame::Null => Ok(LuaValue::Nil),
        RespFrame::Array(Some(items)) | RespFrame::Push(items) | RespFrame::Set(items) => {
            let table = lua.create_table()?;
            for (index, item) in items.iter().enumerate() {
                table.set(index as i64 + 1, resp_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        RespFrame::Array(None) => Ok(LuaValue::Nil),
        RespFrame::Boolean(value) => Ok(LuaValue::Boolean(*value)),
        RespFrame::Double(value) => Ok(LuaValue::Number(*value)),
        RespFrame::BigNumber(value) => Ok(LuaValue::String(lua.create_string(value.as_bytes())?)),
        RespFrame::VerbatimString { data, .. } => {
            Ok(LuaValue::String(lua.create_string(data.as_bytes())?))
        }
        RespFrame::Map(pairs) | RespFrame::Attribute(pairs) => {
            let table = lua.create_table()?;
            let mut index = 1_i64;
            for (key, value) in pairs {
                table.set(index, resp_to_lua(lua, key)?)?;
                table.set(index + 1, resp_to_lua(lua, value)?)?;
                index += 2;
            }
            Ok(LuaValue::Table(table))
        }
    }
}

fn lua_to_resp(value: &LuaValue) -> lua_rs_runtime::Result<RespFrame> {
    match value {
        LuaValue::Nil => Ok(RespFrame::null_bulk()),
        LuaValue::Boolean(true) => Ok(RespFrame::integer(1)),
        LuaValue::Boolean(false) => Ok(RespFrame::null_bulk()),
        LuaValue::Integer(value) => Ok(RespFrame::integer(*value)),
        LuaValue::Number(value) => Ok(RespFrame::integer(*value as i64)),
        LuaValue::String(value) => Ok(bulk(value.as_bytes()?)),
        LuaValue::Table(table) => {
            if let Some(message) = table_string_bytes(table, "err")? {
                return Ok(err(&message));
            }
            if let Some(message) = table_string_bytes(table, "ok")? {
                return Ok(simple(&message));
            }

            let mut items = Vec::new();
            let mut index = 1_i64;
            loop {
                let item = table.get::<_, LuaValue>(index)?;
                if matches!(item, LuaValue::Nil) {
                    break;
                }
                items.push(lua_to_resp(&item)?);
                index += 1;
            }
            Ok(RespFrame::array(items))
        }
        _ => Ok(RespFrame::null_bulk()),
    }
}

fn table_string_bytes(table: &LuaTable, key: &str) -> lua_rs_runtime::Result<Option<Vec<u8>>> {
    match table.get::<_, Option<LuaString>>(key)? {
        Some(value) => Ok(Some(value.as_bytes()?)),
        None => Ok(None),
    }
}

fn collect_lua_args(args: Variadic<LuaValue>) -> lua_rs_runtime::Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(args.len());
    for value in args {
        out.push(lua_arg_to_bytes(&value)?);
    }
    Ok(out)
}

fn lua_arg_to_bytes(value: &LuaValue) -> lua_rs_runtime::Result<Vec<u8>> {
    match value {
        LuaValue::String(value) => value.as_bytes(),
        LuaValue::Integer(value) => Ok(value.to_string().into_bytes()),
        LuaValue::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok((*value as i64).to_string().into_bytes())
        }
        LuaValue::Number(value) if value.is_finite() => Ok(value.to_string().into_bytes()),
        LuaValue::Boolean(true) => Ok(b"1".to_vec()),
        LuaValue::Boolean(false) => Ok(b"0".to_vec()),
        _ => Err(lua_runtime_error(
            "command arguments must be strings or integers",
        )),
    }
}

fn error_table(lua: &Lua, message: &[u8]) -> lua_rs_runtime::Result<LuaValue> {
    let table = lua.create_table()?;
    table.set("err", lua.create_string(message)?)?;
    Ok(LuaValue::Table(table))
}

fn lua_runtime_error(message: &str) -> LuaError {
    LuaError::runtime(format_args!("{message}"))
}

fn lua_runtime_error_bytes(message: &[u8]) -> LuaError {
    LuaError::runtime(format_args!("{}", String::from_utf8_lossy(message)))
}

fn script_blocked_command(command: &[u8]) -> bool {
    ascii_eq(command, b"EVAL") || ascii_eq(command, b"EVALSHA") || ascii_eq(command, b"SCRIPT")
}

fn simple(value: &[u8]) -> RespFrame {
    RespFrame::simple(RedisString::from_bytes(value))
}

fn bulk(value: impl AsRef<[u8]>) -> RespFrame {
    RespFrame::bulk(RedisString::from_bytes(value))
}

fn err(value: &[u8]) -> RespFrame {
    RespFrame::error(RedisString::from_bytes(value))
}

fn wrong_type() -> RespFrame {
    err(b"WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn wrong_arity(command: &[u8]) -> RespFrame {
    let mut msg = b"ERR wrong number of arguments for '".to_vec();
    msg.extend_from_slice(command);
    msg.extend_from_slice(b"' command");
    err(&msg)
}

fn checked_ttl_ms(raw: i64, unit_ms: u64) -> Option<u64> {
    u64::try_from(raw).ok()?.checked_mul(unit_ms)
}

fn parse_i64(bytes: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(bytes).ok()?;
    if text.is_empty()
        || text.as_bytes().iter().any(|b| !b.is_ascii_digit())
            && !matches!(text.as_bytes(), [b'-', rest @ ..] if !rest.is_empty() && rest.iter().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    text.parse::<i64>().ok()
}

fn parse_usize(bytes: &[u8]) -> Option<usize> {
    let value = parse_i64(bytes)?;
    usize::try_from(value).ok()
}

fn ascii_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(l, r)| l.eq_ignore_ascii_case(r))
}

fn normalise_sha(bytes: &[u8]) -> Option<[u8; 40]> {
    if bytes.len() != 40 {
        return None;
    }
    let mut out = [0u8; 40];
    for (index, byte) in bytes.iter().enumerate() {
        out[index] = match *byte {
            b'0'..=b'9' | b'a'..=b'f' => *byte,
            b'A'..=b'F' => *byte + 32,
            _ => return None,
        };
    }
    Some(out)
}

fn sha1_hex(data: &[u8]) -> [u8; 40] {
    let digest = sha1_digest(data);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 40];
    for (index, byte) in digest.iter().enumerate() {
        out[index * 2] = HEX[(byte >> 4) as usize];
        out[index * 2 + 1] = HEX[(byte & 0x0f) as usize];
    }
    out
}

fn sha1_digest(data: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;
    let bit_len = (data.len() as u64) * 8;

    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks(64) {
        let mut w = [0u32; 80];
        for index in 0..16 {
            w[index] = u32::from_be_bytes([
                chunk[index * 4],
                chunk[index * 4 + 1],
                chunk[index * 4 + 2],
                chunk[index * 4 + 3],
            ]);
        }
        for index in 16..80 {
            w[index] = (w[index - 3] ^ w[index - 8] ^ w[index - 14] ^ w[index - 16]).rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;

        for index in 0..80 {
            let (f, k) = if index < 20 {
                ((b & c) | ((!b) & d), 0x5A827999u32)
            } else if index < 40 {
                (b ^ c ^ d, 0x6ED9EBA1u32)
            } else if index < 60 {
                ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32)
            } else {
                (b ^ c ^ d, 0xCA62C1D6)
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[index]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use redis_protocol::encode_resp2;

    use super::*;

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

    fn argv(items: &[&[u8]]) -> Vec<Vec<u8>> {
        items.iter().map(|item| item.to_vec()).collect()
    }

    fn resp2(frame: &RespFrame) -> Vec<u8> {
        let mut out = Vec::new();
        encode_resp2(frame, &mut out);
        out
    }

    fn response_json(response: RestResponse) -> JsonValue {
        assert_eq!(response.content_type, APPLICATION_JSON);
        serde_json::from_slice(&response.body).unwrap()
    }

    fn load_token_bucket(engine: &mut Engine<NoopHost>) -> Vec<u8> {
        let reply = engine.execute(&argv(&[b"SCRIPT", b"LOAD", TOKEN_BUCKET_SCRIPT]));
        match reply {
            RespFrame::Bulk(Some(sha)) => sha.into_bytes(),
            other => panic!("unexpected script load reply: {other:?}"),
        }
    }

    fn evalsha_token_bucket(engine: &mut Engine<NoopHost>, sha: &[u8], now: &[u8]) -> Vec<u8> {
        let request = vec![
            b"EVALSHA".to_vec(),
            sha.to_vec(),
            b"1".to_vec(),
            b"edge:tenant:42:tokens".to_vec(),
            now.to_vec(),
            b"10".to_vec(),
            b"5".to_vec(),
            b"1000".to_vec(),
            b"7".to_vec(),
            b"60000".to_vec(),
        ];
        resp2(&engine.execute(&request))
    }

    fn rest_load_token_bucket(engine: &mut Engine<NoopHost>) -> Vec<u8> {
        let script = String::from_utf8_lossy(TOKEN_BUCKET_SCRIPT);
        let body = serde_json::to_vec(&json!(["SCRIPT", "LOAD", script])).unwrap();
        let response = engine.execute_rest(RestRequest::post("/", &body));
        assert_eq!(response.status, 200);
        let value = response_json(response);
        value["result"].as_str().unwrap().as_bytes().to_vec()
    }

    fn rest_evalsha_token_bucket(
        engine: &mut Engine<NoopHost>,
        sha: &[u8],
        now: &str,
    ) -> JsonValue {
        let sha = String::from_utf8_lossy(sha);
        let body = serde_json::to_vec(&json!([
            "EVALSHA",
            sha,
            1,
            "edge:tenant:42:tokens",
            now,
            10,
            5,
            1000,
            7,
            60000
        ]))
        .unwrap();
        let response = engine.execute_rest(RestRequest::post("/EVALSHA", &body));
        assert_eq!(response.status, 200);
        response_json(response)
    }

    fn rest_evalsha_hash_policy_bucket(
        engine: &mut Engine<NoopHost>,
        sha: &[u8],
        now: &str,
    ) -> JsonValue {
        let sha = String::from_utf8_lossy(sha);
        let body = serde_json::to_vec(&json!([
            "EVALSHA",
            sha,
            2,
            "edge:tenant:42:tokens",
            "edge:tenant:42:policy",
            now,
            7
        ]))
        .unwrap();
        let response = engine.execute_rest(RestRequest::post("/EVALSHA", &body));
        assert_eq!(response.status, 200);
        response_json(response)
    }

    #[test]
    fn basic_mvp_commands_work_without_redis_core() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"41"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[b"INCR", b"k"])),
            RespFrame::integer(42)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"k"]))),
            b"$2\r\n42\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"EXISTS", b"k"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"PEXPIRE", b"k", b"2500"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"TTL", b"k"])),
            RespFrame::integer(3)
        );

        engine.host_mut().set_now_millis(3_501);
        assert_eq!(
            engine.execute(&argv(&[b"GET", b"k"])),
            RespFrame::null_bulk()
        );
    }

    #[test]
    fn hash_commands_cover_policy_storage_shape() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[
                b"HSET",
                b"policy",
                b"capacity",
                b"10",
                b"refill_tokens",
                b"5",
                b"refill_ms",
                b"1000",
                b"ttl_ms",
                b"60000"
            ])),
            RespFrame::integer(4)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGET", b"policy", b"capacity"]))),
            b"$2\r\n10\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"HGET", b"policy", b"missing"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGETALL", b"policy"]))),
            b"*8\r\n$8\r\ncapacity\r\n$2\r\n10\r\n$9\r\nrefill_ms\r\n$4\r\n1000\r\n$13\r\nrefill_tokens\r\n$1\r\n5\r\n$6\r\nttl_ms\r\n$5\r\n60000\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"HDEL", b"policy", b"ttl_ms", b"missing"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"PEXPIRE", b"policy", b"500"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            response_json(engine.execute_rest(RestRequest::get("/HGET/policy/refill_ms"))),
            json!({"result": "1000"})
        );

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"plain", b"value"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGET", b"plain", b"field"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"policy"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn snapshot_round_trip_preserves_strings_hashes_and_absolute_expiry() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"plain", b"value"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"HSET",
                b"policy",
                b"capacity",
                b"10",
                b"refill_ms",
                b"1000"
            ])),
            RespFrame::integer(2)
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"volatile", b"soon", b"PX", b"500"])),
            RespFrame::simple("OK")
        );

        let snapshot = engine.export_snapshot();
        let mut restored = Engine::new(NoopHost::new(1_200));
        restored.import_snapshot(&snapshot).unwrap();

        assert_eq!(
            resp2(&restored.execute(&argv(&[b"GET", b"plain"]))),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"HGET", b"policy", b"capacity"]))),
            b"$2\r\n10\r\n"
        );
        assert_eq!(
            restored.execute(&argv(&[b"PTTL", b"volatile"])),
            RespFrame::integer(300)
        );

        restored.host_mut().set_now_millis(1_500);
        assert_eq!(
            restored.execute(&argv(&[b"GET", b"volatile"])),
            RespFrame::null_bulk()
        );
    }

    #[test]
    fn snapshot_import_rejects_malformed_hex() {
        let mut engine = Engine::new_in_memory();
        let malformed = br#"{
            "format": "valdr-engine-snapshot",
            "version": 1,
            "keys": [{"key": "0", "type": "string", "value": "00"}]
        }"#;

        assert_eq!(
            engine.import_snapshot(malformed),
            Err(SnapshotError::InvalidHex)
        );
    }

    #[test]
    fn script_load_exists_flush_round_trip() {
        let mut engine = Engine::new_in_memory();
        let sha = load_token_bucket(&mut engine);

        let exists = engine.execute(&vec![
            b"SCRIPT".to_vec(),
            b"EXISTS".to_vec(),
            sha.clone(),
            b"ffffffffffffffffffffffffffffffffffffffff".to_vec(),
        ]);
        assert_eq!(resp2(&exists), b"*2\r\n:1\r\n:0\r\n");

        assert_eq!(
            engine.execute(&argv(&[b"SCRIPT", b"FLUSH"])),
            RespFrame::simple("OK")
        );
        let missing = engine.execute(&vec![b"EVALSHA".to_vec(), sha, b"0".to_vec()]);
        assert_eq!(
            missing,
            RespFrame::error("NOSCRIPT No matching script. Please use EVAL.")
        );
    }

    #[test]
    fn evalsha_runs_stateful_token_bucket_fixture() {
        let mut engine = Engine::new_in_memory();
        let sha = load_token_bucket(&mut engine);

        assert_eq!(
            evalsha_token_bucket(&mut engine, &sha, b"1000"),
            b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:0\r\n"
        );
        assert_eq!(
            evalsha_token_bucket(&mut engine, &sha, b"1100"),
            b"*8\r\n$7\r\nallowed\r\n:0\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:700\r\n"
        );
        assert_eq!(
            evalsha_token_bucket(&mut engine, &sha, b"1800"),
            b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:0\r\n$8\r\nreset_ms\r\n:3800\r\n$14\r\nretry_after_ms\r\n:0\r\n"
        );
    }

    #[test]
    fn rest_path_post_body_and_resp2_modes_match_edge_adapter_shape() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        let set = engine.execute_rest(RestRequest::get("/SET/foo/bar"));
        assert_eq!(set.status, 200);
        assert_eq!(response_json(set), json!({"result": "OK"}));

        let get = engine.execute_rest(RestRequest::get("/GET/foo"));
        assert_eq!(get.status, 200);
        assert_eq!(response_json(get), json!({"result": "bar"}));

        let post = engine.execute_rest(RestRequest::post("/SET/post-key?PX=2500", b"post-value"));
        assert_eq!(post.status, 200);
        assert_eq!(response_json(post), json!({"result": "OK"}));
        assert_eq!(
            response_json(engine.execute_rest(RestRequest::get("/PTTL/post-key"))),
            json!({"result": 2500})
        );

        let resp2_get = engine.execute_rest(RestRequest {
            method: RestMethod::Get,
            path: "/GET/post-key",
            body: &[],
            response_format: RestResponseFormat::Resp2,
        });
        assert_eq!(resp2_get.status, 200);
        assert_eq!(resp2_get.content_type, APPLICATION_OCTET_STREAM);
        assert_eq!(resp2_get.body, b"$10\r\npost-value\r\n");
    }

    #[test]
    fn rest_pipeline_is_ordered_and_non_atomic() {
        let mut engine = Engine::new_in_memory();
        let body = br#"[
            ["SET", "key1", "1"],
            ["INCR", "key1"],
            ["GET", "key1"],
            ["NOPE"]
        ]"#;

        let response = engine.execute_rest(RestRequest::post("/pipeline", body));

        assert_eq!(response.status, 200);
        assert_eq!(
            response_json(response),
            json!([
                {"result": "OK"},
                {"result": 2},
                {"result": "2"},
                {"error": "ERR unknown command 'NOPE'"}
            ])
        );
    }

    #[test]
    fn rest_adapter_runs_token_bucket_fixture() {
        let mut engine = Engine::new_in_memory();
        let sha = rest_load_token_bucket(&mut engine);

        assert_eq!(
            rest_evalsha_token_bucket(&mut engine, &sha, "1000"),
            json!({"result": ["allowed", 1, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 0]})
        );
        assert_eq!(
            rest_evalsha_token_bucket(&mut engine, &sha, "1100"),
            json!({"result": ["allowed", 0, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 700]})
        );
        assert_eq!(
            rest_evalsha_token_bucket(&mut engine, &sha, "1800"),
            json!({"result": ["allowed", 1, "remaining", 0, "reset_ms", 3800, "retry_after_ms", 0]})
        );
    }

    #[test]
    fn rest_adapter_runs_hash_policy_token_bucket_fixture() {
        let mut engine = Engine::new_in_memory();
        let policy = engine.execute_rest(RestRequest::post(
            "/HSET/edge%3Atenant%3A42%3Apolicy",
            b"[\"HSET\",\"edge:tenant:42:policy\",\"capacity\",\"10\",\"refill_tokens\",\"5\",\"refill_ms\",\"1000\",\"ttl_ms\",\"60000\"]",
        ));
        assert_eq!(response_json(policy), json!({"result": 4}));

        let script = String::from_utf8_lossy(HASH_POLICY_TOKEN_BUCKET_SCRIPT);
        let body = serde_json::to_vec(&json!(["SCRIPT", "LOAD", script])).unwrap();
        let response = engine.execute_rest(RestRequest::post("/", &body));
        assert_eq!(response.status, 200);
        let value = response_json(response);
        let sha = value["result"].as_str().unwrap().as_bytes().to_vec();

        assert_eq!(
            rest_evalsha_hash_policy_bucket(&mut engine, &sha, "1000"),
            json!({"result": ["allowed", 1, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 0, "capacity", 10]})
        );
        assert_eq!(
            rest_evalsha_hash_policy_bucket(&mut engine, &sha, "1100"),
            json!({"result": ["allowed", 0, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 700, "capacity", 10]})
        );

        let upgrade = engine.execute_rest(RestRequest::get(
            "/HSET/edge%3Atenant%3A42%3Apolicy/capacity/20",
        ));
        assert_eq!(response_json(upgrade), json!({"result": 0}));
        assert_eq!(
            rest_evalsha_hash_policy_bucket(&mut engine, &sha, "1800"),
            json!({"result": ["allowed", 1, "remaining", 0, "reset_ms", 5800, "retry_after_ms", 0, "capacity", 20]})
        );
    }

    #[test]
    fn sha1_hex_known_vectors() {
        assert_eq!(&sha1_hex(b""), b"da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            &sha1_hex(b"abc"),
            b"a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }
}
