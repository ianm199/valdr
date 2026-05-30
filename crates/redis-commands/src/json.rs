//! RedisJSON command implementations.
//! Provides native JSON.SET / JSON.GET / JSON.DEL / JSON.TYPE /
//! JSON.NUMINCRBY / JSON.NUMMULTBY / JSON.STRAPPEND / JSON.STRLEN /
//! JSON.OBJKEYS / JSON.OBJLEN / JSON.ARRAPPEND / JSON.ARRLEN /
//! JSON.ARRINSERT / JSON.ARRPOP / JSON.CLEAR / JSON.MGET /
//! JSON.FORGET (alias of JSON.DEL).
//! Storage: `ObjectKind::Json(serde_json::Value)`.
//! JSONPath subset implemented:
//! `$` — root
//! `$.foo` — object key
//! `$["foo"]` — bracket-notation key
//! `$.a.b` — nested
//! `$[0]`, `$[-1]` — array index (negative = from end)
//! `$.arr[*]` — wildcard (all array/object elements)
//! `$..foo` — recursive descent
//! Deferred JSONPath features: filter expressions `[?(...)]`,
//! union `[a,b]`, slices `[1:3]`.
//! TODO: JSON.MERGE, JSON.RESP (RESP3 native types), JSON.MSET.

use redis_core::command_context::CommandContext;
use redis_core::object::{ObjectKind, RedisObject};
use redis_types::{RedisError, RedisResult, RedisString};
use serde_json::Value;

// ─────────────────────────────────────────────────────────────────────────────
// JSONPath lexer + evaluator
// ─────────────────────────────────────────────────────────────────────────────

/// Tokens produced by the JSONPath lexer.
#[derive(Debug, Clone, PartialEq)]
enum PathToken {
    Root,
    Key(String),
    Index(i64),
    Wildcard,
    RecursiveKey(String),
}

/// Lex a JSONPath string into a token stream.
fn lex_path(path: &str) -> Result<Vec<PathToken>, RedisError> {
    let bytes = path.as_bytes();
    let len = bytes.len();
    let mut pos = 0;
    let mut tokens = Vec::new();

    if pos >= len || bytes[pos] != b'$' {
        return Err(RedisError::runtime(b"ERR Path must begin with '$'"));
    }
    tokens.push(PathToken::Root);
    pos += 1;

    while pos < len {
        if bytes[pos] == b'.' && pos + 1 < len && bytes[pos + 1] == b'.' {
            pos += 2;
            let start = pos;
            while pos < len && bytes[pos] != b'.' && bytes[pos] != b'[' {
                pos += 1;
            }
            let key = std::str::from_utf8(&bytes[start..pos])
                .map_err(|_| RedisError::runtime(b"ERR invalid path encoding"))?
                .to_string();
            if key.is_empty() {
                return Err(RedisError::runtime(b"ERR empty key in recursive descent"));
            }
            tokens.push(PathToken::RecursiveKey(key));
        } else if bytes[pos] == b'.' {
            pos += 1;
            let start = pos;
            while pos < len && bytes[pos] != b'.' && bytes[pos] != b'[' {
                pos += 1;
            }
            let key = std::str::from_utf8(&bytes[start..pos])
                .map_err(|_| RedisError::runtime(b"ERR invalid path encoding"))?
                .to_string();
            if key.is_empty() {
                return Err(RedisError::runtime(b"ERR empty key after '.'"));
            }
            tokens.push(PathToken::Key(key));
        } else if bytes[pos] == b'[' {
            pos += 1;
            if pos < len && (bytes[pos] == b'\'' || bytes[pos] == b'"') {
                let quote = bytes[pos];
                pos += 1;
                let start = pos;
                while pos < len && bytes[pos] != quote {
                    pos += 1;
                }
                let key = std::str::from_utf8(&bytes[start..pos])
                    .map_err(|_| RedisError::runtime(b"ERR invalid path encoding"))?
                    .to_string();
                pos += 1;
                if pos >= len || bytes[pos] != b']' {
                    return Err(RedisError::runtime(b"ERR expected ']'"));
                }
                pos += 1;
                tokens.push(PathToken::Key(key));
            } else if pos < len && bytes[pos] == b'*' {
                pos += 1;
                if pos >= len || bytes[pos] != b']' {
                    return Err(RedisError::runtime(b"ERR expected ']' after '*'"));
                }
                pos += 1;
                tokens.push(PathToken::Wildcard);
            } else {
                let start = pos;
                if pos < len && (bytes[pos] == b'-' || bytes[pos].is_ascii_digit()) {
                    if bytes[pos] == b'-' {
                        pos += 1;
                    }
                    while pos < len && bytes[pos].is_ascii_digit() {
                        pos += 1;
                    }
                }
                let s = std::str::from_utf8(&bytes[start..pos])
                    .map_err(|_| RedisError::runtime(b"ERR invalid path encoding"))?;
                let idx: i64 = s
                    .parse()
                    .map_err(|_| RedisError::runtime(b"ERR invalid array index"))?;
                if pos >= len || bytes[pos] != b']' {
                    return Err(RedisError::runtime(b"ERR expected ']'"));
                }
                pos += 1;
                tokens.push(PathToken::Index(idx));
            }
        } else {
            return Err(RedisError::runtime(b"ERR unexpected character in path"));
        }
    }
    Ok(tokens)
}

/// Resolve a possibly-negative index to an absolute array index.
fn resolve_index(idx: i64, len: usize) -> Option<usize> {
    if idx >= 0 {
        let u = idx as usize;
        if u < len {
            Some(u)
        } else {
            None
        }
    } else {
        let abs = (-idx) as usize;
        if abs <= len {
            Some(len - abs)
        } else {
            None
        }
    }
}

/// Read-only evaluation: collect all Values matching the token stream.
fn eval_tokens<'a>(value: &'a Value, tokens: &[PathToken], pos: usize) -> Vec<&'a Value> {
    if pos >= tokens.len() {
        return vec![value];
    }
    match &tokens[pos] {
        PathToken::Root => eval_tokens(value, tokens, pos + 1),
        PathToken::Key(k) => match value {
            Value::Object(map) => match map.get(k.as_str()) {
                Some(child) => eval_tokens(child, tokens, pos + 1),
                None => vec![],
            },
            _ => vec![],
        },
        PathToken::Index(idx) => match value {
            Value::Array(arr) => match resolve_index(*idx, arr.len()) {
                Some(i) => eval_tokens(&arr[i], tokens, pos + 1),
                None => vec![],
            },
            _ => vec![],
        },
        PathToken::Wildcard => match value {
            Value::Array(arr) => arr
                .iter()
                .flat_map(|child| eval_tokens(child, tokens, pos + 1))
                .collect(),
            Value::Object(map) => map
                .values()
                .flat_map(|child| eval_tokens(child, tokens, pos + 1))
                .collect(),
            _ => vec![],
        },
        PathToken::RecursiveKey(k) => {
            let mut results = vec![];
            collect_recursive(value, k.as_str(), tokens, pos + 1, &mut results);
            results
        }
    }
}

fn collect_recursive<'a>(
    v: &'a Value,
    key: &str,
    tokens: &[PathToken],
    next_pos: usize,
    out: &mut Vec<&'a Value>,
) {
    match v {
        Value::Object(map) => {
            if let Some(child) = map.get(key) {
                out.extend(eval_tokens(child, tokens, next_pos));
            }
            for child in map.values() {
                collect_recursive(child, key, tokens, next_pos, out);
            }
        }
        Value::Array(arr) => {
            for child in arr {
                collect_recursive(child, key, tokens, next_pos, out);
            }
        }
        _ => {}
    }
}

/// Owned variant: clone all matched values.
fn query_path_owned(root: &Value, path: &str) -> Result<Vec<Value>, RedisError> {
    let tokens = lex_path(path)?;
    Ok(eval_tokens(root, &tokens, 0).into_iter().cloned().collect())
}

// ─────────────────────────────────────────────────────────────────────────────
// Mutable path operations — set / delete / numeric / array ops
// ─────────────────────────────────────────────────────────────────────────────

/// Set the value at `tokens[1..]` within `root`. Returns number of fields set.
fn set_path_value(
    root: &mut Value,
    tokens: &[PathToken],
    new_val: Value,
    flag_nx: bool,
    flag_xx: bool,
) -> Result<bool, RedisError> {
    if tokens.is_empty() || tokens[0] != PathToken::Root {
        return Err(RedisError::runtime(b"ERR path must start with $"));
    }
    set_recursive(root, tokens, 1, new_val, flag_nx, flag_xx)
}

fn set_recursive(
    v: &mut Value,
    tokens: &[PathToken],
    pos: usize,
    new_val: Value,
    flag_nx: bool,
    flag_xx: bool,
) -> Result<bool, RedisError> {
    if pos >= tokens.len() {
        return Err(RedisError::runtime(b"ERR path resolution error"));
    }
    let is_last = pos == tokens.len() - 1;
    match &tokens[pos] {
        PathToken::Root => set_recursive(v, tokens, pos + 1, new_val, flag_nx, flag_xx),
        PathToken::Key(k) => {
            let k_owned = k.clone();
            match v {
                Value::Object(map) => {
                    if is_last {
                        let exists = map.contains_key(k_owned.as_str());
                        if flag_nx && exists {
                            return Ok(false);
                        }
                        if flag_xx && !exists {
                            return Ok(false);
                        }
                        map.insert(k_owned, new_val);
                        Ok(true)
                    } else {
                        match map.get_mut(k_owned.as_str()) {
                            Some(child) => {
                                set_recursive(child, tokens, pos + 1, new_val, flag_nx, flag_xx)
                            }
                            None => Err(RedisError::runtime(b"ERR path does not exist")),
                        }
                    }
                }
                _ => Err(RedisError::runtime(b"ERR path traversal into non-object")),
            }
        }
        PathToken::Index(idx) => match v {
            Value::Array(arr) => {
                let len = arr.len();
                let i = resolve_index(*idx, len)
                    .ok_or_else(|| RedisError::runtime(b"ERR array index out of bounds"))?;
                if is_last {
                    arr[i] = new_val;
                    Ok(true)
                } else {
                    set_recursive(&mut arr[i], tokens, pos + 1, new_val, flag_nx, flag_xx)
                }
            }
            _ => Err(RedisError::runtime(b"ERR path traversal into non-array")),
        },
        _ => Err(RedisError::runtime(
            b"ERR wildcard/recursive not supported in SET path",
        )),
    }
}

/// Delete matching paths. Returns count deleted.
fn delete_paths(root: &mut Value, tokens: &[PathToken]) -> i64 {
    if tokens.len() < 2 {
        return 0;
    }
    delete_recursive(root, tokens, 1)
}

fn delete_recursive(v: &mut Value, tokens: &[PathToken], pos: usize) -> i64 {
    if pos >= tokens.len() {
        return 0;
    }
    let is_last = pos == tokens.len() - 1;
    match &tokens[pos] {
        PathToken::Root => delete_recursive(v, tokens, pos + 1),
        PathToken::Key(k) => {
            let k_owned = k.clone();
            match v {
                Value::Object(map) => {
                    if is_last {
                        if map.remove(k_owned.as_str()).is_some() {
                            1
                        } else {
                            0
                        }
                    } else {
                        match map.get_mut(k_owned.as_str()) {
                            Some(child) => delete_recursive(child, tokens, pos + 1),
                            None => 0,
                        }
                    }
                }
                _ => 0,
            }
        }
        PathToken::Index(idx) => match v {
            Value::Array(arr) => {
                let len = arr.len();
                match resolve_index(*idx, len) {
                    None => 0,
                    Some(i) => {
                        if is_last {
                            arr.remove(i);
                            1
                        } else {
                            delete_recursive(&mut arr[i], tokens, pos + 1)
                        }
                    }
                }
            }
            _ => 0,
        },
        PathToken::RecursiveKey(k) => {
            let k_owned = k.clone();
            delete_recursive_descent(v, &k_owned, tokens, pos + 1, is_last)
        }
        PathToken::Wildcard => match v {
            Value::Array(arr) => {
                if is_last {
                    let count = arr.len() as i64;
                    arr.clear();
                    count
                } else {
                    arr.iter_mut()
                        .map(|child| delete_recursive(child, tokens, pos + 1))
                        .sum()
                }
            }
            Value::Object(map) => {
                if is_last {
                    let count = map.len() as i64;
                    map.clear();
                    count
                } else {
                    map.values_mut()
                        .map(|child| delete_recursive(child, tokens, pos + 1))
                        .sum()
                }
            }
            _ => 0,
        },
    }
}

fn delete_recursive_descent(
    v: &mut Value,
    key: &str,
    tokens: &[PathToken],
    next_pos: usize,
    is_last: bool,
) -> i64 {
    let mut count = 0i64;
    match v {
        Value::Object(map) => {
            if is_last {
                if map.remove(key).is_some() {
                    count += 1;
                }
            } else if let Some(child) = map.get_mut(key) {
                count += delete_recursive(child, tokens, next_pos);
            }
            let other_keys: Vec<String> =
                map.keys().filter(|k| k.as_str() != key).cloned().collect();
            for k in other_keys {
                if let Some(child) = map.get_mut(k.as_str()) {
                    count += delete_recursive_descent(child, key, tokens, next_pos, is_last);
                }
            }
        }
        Value::Array(arr) => {
            for child in arr.iter_mut() {
                count += delete_recursive_descent(child, key, tokens, next_pos, is_last);
            }
        }
        _ => {}
    }
    count
}

/// Apply numeric increment or multiply to all matching paths. Returns new values.
fn num_op(
    root: &mut Value,
    tokens: &[PathToken],
    operand: f64,
    is_multiply: bool,
) -> Result<Vec<Value>, RedisError> {
    if tokens.len() == 1 && tokens[0] == PathToken::Root {
        apply_num_op(root, operand, is_multiply)?;
        return Ok(vec![root.clone()]);
    }
    num_op_recursive(root, tokens, 1, operand, is_multiply)
}

fn num_op_recursive(
    v: &mut Value,
    tokens: &[PathToken],
    pos: usize,
    operand: f64,
    is_multiply: bool,
) -> Result<Vec<Value>, RedisError> {
    if pos >= tokens.len() {
        return Err(RedisError::runtime(b"ERR path resolution error"));
    }
    let is_last = pos == tokens.len() - 1;
    match &tokens[pos] {
        PathToken::Root => num_op_recursive(v, tokens, pos + 1, operand, is_multiply),
        PathToken::Key(k) => {
            let k_owned = k.clone();
            match v {
                Value::Object(map) => {
                    if is_last {
                        let target = map
                            .get_mut(k_owned.as_str())
                            .ok_or_else(|| RedisError::runtime(b"ERR path does not exist"))?;
                        apply_num_op(target, operand, is_multiply)?;
                        Ok(vec![target.clone()])
                    } else {
                        match map.get_mut(k_owned.as_str()) {
                            Some(child) => {
                                num_op_recursive(child, tokens, pos + 1, operand, is_multiply)
                            }
                            None => Err(RedisError::runtime(b"ERR path does not exist")),
                        }
                    }
                }
                _ => Err(RedisError::runtime(b"ERR not an object at path")),
            }
        }
        PathToken::Index(idx) => match v {
            Value::Array(arr) => {
                let len = arr.len();
                let i = resolve_index(*idx, len)
                    .ok_or_else(|| RedisError::runtime(b"ERR index out of range"))?;
                if is_last {
                    apply_num_op(&mut arr[i], operand, is_multiply)?;
                    Ok(vec![arr[i].clone()])
                } else {
                    num_op_recursive(&mut arr[i], tokens, pos + 1, operand, is_multiply)
                }
            }
            _ => Err(RedisError::runtime(b"ERR not an array at path")),
        },
        PathToken::Wildcard => {
            let mut results = vec![];
            if let Value::Array(arr) = v {
                for child in arr.iter_mut() {
                    if is_last {
                        if apply_num_op(child, operand, is_multiply).is_ok() {
                            results.push(child.clone());
                        }
                    } else if let Ok(sub) =
                        num_op_recursive(child, tokens, pos + 1, operand, is_multiply)
                    {
                        results.extend(sub);
                    }
                }
            }
            Ok(results)
        }
        PathToken::RecursiveKey(k) => {
            let k_owned = k.clone();
            let mut results = vec![];
            collect_num_op_recursive(
                v,
                &k_owned,
                tokens,
                pos + 1,
                is_last,
                operand,
                is_multiply,
                &mut results,
            );
            Ok(results)
        }
    }
}

fn apply_num_op(v: &mut Value, operand: f64, is_multiply: bool) -> Result<(), RedisError> {
    match v {
        Value::Number(n) => {
            let current = if let Some(i) = n.as_i64() {
                i as f64
            } else if let Some(u) = n.as_u64() {
                u as f64
            } else {
                n.as_f64()
                    .ok_or_else(|| RedisError::runtime(b"ERR number conversion failed"))?
            };
            let result = if is_multiply {
                current * operand
            } else {
                current + operand
            };
            if result.fract() == 0.0 && result >= i64::MIN as f64 && result <= i64::MAX as f64 {
                *n = serde_json::Number::from(result as i64);
            } else {
                *n = serde_json::Number::from_f64(result)
                    .ok_or_else(|| RedisError::runtime(b"ERR result is NaN or infinite"))?;
            }
            Ok(())
        }
        _ => Err(RedisError::runtime(b"ERR value is not a number")),
    }
}

fn collect_num_op_recursive(
    v: &mut Value,
    key: &str,
    tokens: &[PathToken],
    next_pos: usize,
    is_last: bool,
    operand: f64,
    is_multiply: bool,
    out: &mut Vec<Value>,
) {
    match v {
        Value::Object(map) => {
            let has_key = map.contains_key(key);
            if has_key {
                if is_last {
                    if let Some(target) = map.get_mut(key) {
                        if apply_num_op(target, operand, is_multiply).is_ok() {
                            out.push(target.clone());
                        }
                    }
                } else if let Some(child) = map.get_mut(key) {
                    if let Ok(sub) = num_op_recursive(child, tokens, next_pos, operand, is_multiply)
                    {
                        out.extend(sub);
                    }
                }
            }
            let child_keys: Vec<String> =
                map.keys().filter(|k| k.as_str() != key).cloned().collect();
            for ck in child_keys {
                if let Some(child) = map.get_mut(ck.as_str()) {
                    collect_num_op_recursive(
                        child,
                        key,
                        tokens,
                        next_pos,
                        is_last,
                        operand,
                        is_multiply,
                        out,
                    );
                }
            }
        }
        Value::Array(arr) => {
            for child in arr.iter_mut() {
                collect_num_op_recursive(
                    child,
                    key,
                    tokens,
                    next_pos,
                    is_last,
                    operand,
                    is_multiply,
                    out,
                );
            }
        }
        _ => {}
    }
}

/// Append to string fields matching the path. Returns lengths.
fn strappend_op(root: &mut Value, tokens: &[PathToken], append: &str) -> Vec<Option<usize>> {
    if tokens.len() == 1 && tokens[0] == PathToken::Root {
        match root {
            Value::String(s) => {
                s.push_str(append);
                return vec![Some(s.len())];
            }
            _ => return vec![None],
        }
    }
    strappend_recursive(root, tokens, 1, append)
}

fn strappend_recursive(
    v: &mut Value,
    tokens: &[PathToken],
    pos: usize,
    append: &str,
) -> Vec<Option<usize>> {
    if pos >= tokens.len() {
        return vec![];
    }
    let is_last = pos == tokens.len() - 1;
    match &tokens[pos] {
        PathToken::Root => strappend_recursive(v, tokens, pos + 1, append),
        PathToken::Key(k) => {
            let k_owned = k.clone();
            match v {
                Value::Object(map) => {
                    if is_last {
                        match map.get_mut(k_owned.as_str()) {
                            Some(Value::String(s)) => {
                                s.push_str(append);
                                vec![Some(s.len())]
                            }
                            Some(_) => vec![None],
                            None => vec![],
                        }
                    } else {
                        match map.get_mut(k_owned.as_str()) {
                            Some(child) => strappend_recursive(child, tokens, pos + 1, append),
                            None => vec![],
                        }
                    }
                }
                _ => vec![],
            }
        }
        PathToken::Index(idx) => match v {
            Value::Array(arr) => {
                let len = arr.len();
                let i = match resolve_index(*idx, len) {
                    Some(i) => i,
                    None => return vec![],
                };
                if is_last {
                    match &mut arr[i] {
                        Value::String(s) => {
                            s.push_str(append);
                            vec![Some(s.len())]
                        }
                        _ => vec![None],
                    }
                } else {
                    strappend_recursive(&mut arr[i], tokens, pos + 1, append)
                }
            }
            _ => vec![],
        },
        PathToken::Wildcard => {
            let mut results = vec![];
            if let Value::Array(arr) = v {
                for child in arr.iter_mut() {
                    if is_last {
                        match child {
                            Value::String(s) => {
                                s.push_str(append);
                                results.push(Some(s.len()));
                            }
                            _ => results.push(None),
                        }
                    } else {
                        results.extend(strappend_recursive(child, tokens, pos + 1, append));
                    }
                }
            }
            results
        }
        PathToken::RecursiveKey(_) => vec![],
    }
}

/// Append values to array fields matching the path. Returns new lengths.
fn arrappend_op(root: &mut Value, tokens: &[PathToken], new_vals: &[Value]) -> Vec<Option<usize>> {
    if tokens.len() == 1 && tokens[0] == PathToken::Root {
        match root {
            Value::Array(arr) => {
                arr.extend(new_vals.iter().cloned());
                return vec![Some(arr.len())];
            }
            _ => return vec![None],
        }
    }
    arrappend_recursive(root, tokens, 1, new_vals)
}

fn arrappend_recursive(
    v: &mut Value,
    tokens: &[PathToken],
    pos: usize,
    new_vals: &[Value],
) -> Vec<Option<usize>> {
    if pos >= tokens.len() {
        return vec![];
    }
    let is_last = pos == tokens.len() - 1;
    match &tokens[pos] {
        PathToken::Root => arrappend_recursive(v, tokens, pos + 1, new_vals),
        PathToken::Key(k) => {
            let k_owned = k.clone();
            match v {
                Value::Object(map) => {
                    if is_last {
                        match map.get_mut(k_owned.as_str()) {
                            Some(Value::Array(arr)) => {
                                arr.extend(new_vals.iter().cloned());
                                vec![Some(arr.len())]
                            }
                            Some(_) => vec![None],
                            None => vec![],
                        }
                    } else {
                        match map.get_mut(k_owned.as_str()) {
                            Some(child) => arrappend_recursive(child, tokens, pos + 1, new_vals),
                            None => vec![],
                        }
                    }
                }
                _ => vec![],
            }
        }
        PathToken::Index(idx) => match v {
            Value::Array(arr) => {
                let len = arr.len();
                let i = match resolve_index(*idx, len) {
                    Some(i) => i,
                    None => return vec![],
                };
                if is_last {
                    match &mut arr[i] {
                        Value::Array(inner) => {
                            inner.extend(new_vals.iter().cloned());
                            vec![Some(inner.len())]
                        }
                        _ => vec![None],
                    }
                } else {
                    arrappend_recursive(&mut arr[i], tokens, pos + 1, new_vals)
                }
            }
            _ => vec![],
        },
        PathToken::Wildcard => {
            let mut results = vec![];
            if let Value::Array(arr) = v {
                for child in arr.iter_mut() {
                    if is_last {
                        match child {
                            Value::Array(inner) => {
                                inner.extend(new_vals.iter().cloned());
                                results.push(Some(inner.len()));
                            }
                            _ => results.push(None),
                        }
                    } else {
                        results.extend(arrappend_recursive(child, tokens, pos + 1, new_vals));
                    }
                }
            }
            results
        }
        PathToken::RecursiveKey(_) => vec![],
    }
}

/// Insert values into array fields at index. Returns new lengths.
fn arrinsert_op(
    root: &mut Value,
    tokens: &[PathToken],
    raw_idx: i64,
    new_vals: &[Value],
) -> Vec<Option<usize>> {
    if tokens.len() == 1 && tokens[0] == PathToken::Root {
        match root {
            Value::Array(arr) => {
                let len = arr.len();
                let insert_pos = if raw_idx >= 0 {
                    (raw_idx as usize).min(len)
                } else {
                    let abs = (-raw_idx) as usize;
                    len.saturating_sub(abs)
                };
                for (offset, val) in new_vals.iter().enumerate() {
                    arr.insert(insert_pos + offset, val.clone());
                }
                return vec![Some(arr.len())];
            }
            _ => return vec![None],
        }
    }
    arrinsert_recursive(root, tokens, 1, raw_idx, new_vals)
}

fn arrinsert_recursive(
    v: &mut Value,
    tokens: &[PathToken],
    pos: usize,
    raw_idx: i64,
    new_vals: &[Value],
) -> Vec<Option<usize>> {
    if pos >= tokens.len() {
        return vec![];
    }
    let is_last = pos == tokens.len() - 1;

    fn do_insert(arr: &mut Vec<Value>, raw_idx: i64, new_vals: &[Value]) -> usize {
        let len = arr.len();
        let insert_pos = if raw_idx >= 0 {
            (raw_idx as usize).min(len)
        } else {
            let abs = (-raw_idx) as usize;
            len.saturating_sub(abs)
        };
        for (offset, val) in new_vals.iter().enumerate() {
            arr.insert(insert_pos + offset, val.clone());
        }
        arr.len()
    }

    match &tokens[pos] {
        PathToken::Root => arrinsert_recursive(v, tokens, pos + 1, raw_idx, new_vals),
        PathToken::Key(k) => {
            let k_owned = k.clone();
            match v {
                Value::Object(map) => {
                    if is_last {
                        match map.get_mut(k_owned.as_str()) {
                            Some(Value::Array(arr)) => {
                                vec![Some(do_insert(arr, raw_idx, new_vals))]
                            }
                            Some(_) => vec![None],
                            None => vec![],
                        }
                    } else {
                        match map.get_mut(k_owned.as_str()) {
                            Some(child) => {
                                arrinsert_recursive(child, tokens, pos + 1, raw_idx, new_vals)
                            }
                            None => vec![],
                        }
                    }
                }
                _ => vec![],
            }
        }
        PathToken::Index(idx) => match v {
            Value::Array(arr) => {
                let len = arr.len();
                let i = match resolve_index(*idx, len) {
                    Some(i) => i,
                    None => return vec![],
                };
                if is_last {
                    match &mut arr[i] {
                        Value::Array(inner) => vec![Some(do_insert(inner, raw_idx, new_vals))],
                        _ => vec![None],
                    }
                } else {
                    arrinsert_recursive(&mut arr[i], tokens, pos + 1, raw_idx, new_vals)
                }
            }
            _ => vec![],
        },
        PathToken::Wildcard | PathToken::RecursiveKey(_) => vec![],
    }
}

/// Pop from array fields. Returns the popped values.
fn arrpop_op(root: &mut Value, tokens: &[PathToken], pop_idx: i64) -> Vec<Option<Value>> {
    if tokens.len() == 1 && tokens[0] == PathToken::Root {
        match root {
            Value::Array(arr) => return vec![do_arrpop(arr, pop_idx)],
            _ => return vec![None],
        }
    }
    arrpop_recursive(root, tokens, 1, pop_idx)
}

fn do_arrpop(arr: &mut Vec<Value>, pop_idx: i64) -> Option<Value> {
    if arr.is_empty() {
        None
    } else {
        let i = resolve_index(pop_idx, arr.len()).unwrap_or(arr.len() - 1);
        Some(arr.remove(i))
    }
}

fn arrpop_recursive(
    v: &mut Value,
    tokens: &[PathToken],
    pos: usize,
    pop_idx: i64,
) -> Vec<Option<Value>> {
    if pos >= tokens.len() {
        return vec![];
    }
    let is_last = pos == tokens.len() - 1;
    match &tokens[pos] {
        PathToken::Root => arrpop_recursive(v, tokens, pos + 1, pop_idx),
        PathToken::Key(k) => {
            let k_owned = k.clone();
            match v {
                Value::Object(map) => {
                    if is_last {
                        match map.get_mut(k_owned.as_str()) {
                            Some(Value::Array(arr)) => vec![do_arrpop(arr, pop_idx)],
                            Some(_) => vec![None],
                            None => vec![],
                        }
                    } else {
                        match map.get_mut(k_owned.as_str()) {
                            Some(child) => arrpop_recursive(child, tokens, pos + 1, pop_idx),
                            None => vec![],
                        }
                    }
                }
                _ => vec![],
            }
        }
        PathToken::Index(idx) => match v {
            Value::Array(arr) => {
                let len = arr.len();
                let i = match resolve_index(*idx, len) {
                    Some(i) => i,
                    None => return vec![],
                };
                if is_last {
                    match &mut arr[i] {
                        Value::Array(inner) => vec![do_arrpop(inner, pop_idx)],
                        _ => vec![None],
                    }
                } else {
                    arrpop_recursive(&mut arr[i], tokens, pos + 1, pop_idx)
                }
            }
            _ => vec![],
        },
        PathToken::Wildcard | PathToken::RecursiveKey(_) => vec![],
    }
}

/// Clear (zero/empty) all matching paths.
fn clear_op(root: &mut Value, tokens: &[PathToken]) -> i64 {
    if tokens.len() == 1 && tokens[0] == PathToken::Root {
        return if clear_value(root) { 1 } else { 0 };
    }
    clear_recursive(root, tokens, 1)
}

fn clear_value(v: &mut Value) -> bool {
    match v {
        Value::Array(arr) => {
            arr.clear();
            true
        }
        Value::Object(map) => {
            map.clear();
            true
        }
        Value::Number(n) => {
            *n = serde_json::Number::from(0i64);
            true
        }
        Value::String(s) => {
            s.clear();
            true
        }
        Value::Bool(_) | Value::Null => false,
    }
}

fn clear_recursive(v: &mut Value, tokens: &[PathToken], pos: usize) -> i64 {
    if pos >= tokens.len() {
        return 0;
    }
    let is_last = pos == tokens.len() - 1;
    match &tokens[pos] {
        PathToken::Root => {
            if is_last {
                if clear_value(v) {
                    1
                } else {
                    0
                }
            } else {
                clear_recursive(v, tokens, pos + 1)
            }
        }
        PathToken::Key(k) => {
            let k_owned = k.clone();
            match v {
                Value::Object(map) => {
                    if is_last {
                        match map.get_mut(k_owned.as_str()) {
                            Some(target) => {
                                if clear_value(target) {
                                    1
                                } else {
                                    0
                                }
                            }
                            None => 0,
                        }
                    } else {
                        match map.get_mut(k_owned.as_str()) {
                            Some(child) => clear_recursive(child, tokens, pos + 1),
                            None => 0,
                        }
                    }
                }
                _ => 0,
            }
        }
        PathToken::Index(idx) => match v {
            Value::Array(arr) => {
                let len = arr.len();
                match resolve_index(*idx, len) {
                    None => 0,
                    Some(i) => {
                        if is_last {
                            if clear_value(&mut arr[i]) {
                                1
                            } else {
                                0
                            }
                        } else {
                            clear_recursive(&mut arr[i], tokens, pos + 1)
                        }
                    }
                }
            }
            _ => 0,
        },
        PathToken::Wildcard => {
            let mut count = 0i64;
            if let Value::Array(arr) = v {
                for child in arr.iter_mut() {
                    if is_last {
                        if clear_value(child) {
                            count += 1;
                        }
                    } else {
                        count += clear_recursive(child, tokens, pos + 1);
                    }
                }
            }
            count
        }
        PathToken::RecursiveKey(_) => 0,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: JSON type name for wire replies
// ─────────────────────────────────────────────────────────────────────────────

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Serialize a list of matched values as a JSON array string.
fn serialize_matches(matches: &[Value]) -> String {
    if matches.is_empty() {
        "[]".to_string()
    } else {
        let parts: Vec<String> = matches.iter().map(|v| v.to_string()).collect();
        format!("[{}]", parts.join(","))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: extract Json value or WRONGTYPE
// ─────────────────────────────────────────────────────────────────────────────

fn get_json_clone(obj: Option<&RedisObject>) -> Result<Option<Value>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => match &o.kind {
            ObjectKind::Json(v) => Ok(Some(v.clone())),
            _ => Err(RedisError::wrong_type()),
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.SET key path value [NX|XX]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_set_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"JSON.SET"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_arg = ctx.arg_owned(2usize)?;
    let raw_value = ctx.arg_owned(3usize)?;

    let mut flag_nx = false;
    let mut flag_xx = false;
    if argc > 4 {
        let flag = ctx.arg_owned(4usize)?;
        let flag_bytes = flag.as_bytes().to_ascii_uppercase();
        if flag_bytes == b"NX" {
            flag_nx = true;
        } else if flag_bytes == b"XX" {
            flag_xx = true;
        } else {
            return Err(RedisError::runtime(b"ERR syntax error: expected NX or XX"));
        }
    }

    let new_val: Value = serde_json::from_slice(raw_value.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR invalid JSON"))?;

    let path_str = std::str::from_utf8(path_arg.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
        .to_string();

    if path_str == "$" {
        let key_exists = ctx.db().lookup_key_read(&key).is_some();
        if flag_nx && key_exists {
            return ctx.reply_null();
        }
        if flag_xx && !key_exists {
            return ctx.reply_null();
        }
        let obj = RedisObject::new_json(new_val);
        ctx.db_mut().insert(key, obj);
        return ctx.reply_simple_string(b"OK");
    }

    let tokens = lex_path(&path_str)?;

    let existing = ctx.db_mut().lookup_key_write(&key);
    match existing {
        None => {
            return Err(RedisError::runtime(
                b"ERR key does not exist for non-root path",
            ));
        }
        Some(obj) => {
            if !obj.is_json() {
                return Err(RedisError::wrong_type());
            }
            let root = match &mut obj.kind {
                ObjectKind::Json(v) => v,
                _ => unreachable!(),
            };
            match set_path_value(root, &tokens, new_val, flag_nx, flag_xx)? {
                true => {}
                false => return ctx.reply_null(),
            }
        }
    }
    ctx.reply_simple_string(b"OK")
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.GET key [INDENT s] [NEWLINE s] [SPACE s] [path...]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_get_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"JSON.GET"));
    }
    let key = ctx.arg_owned(1usize)?;

    let mut paths: Vec<String> = Vec::new();
    let mut i = 2;
    while i < argc {
        let arg = ctx.arg_owned(i)?;
        let upper = arg.as_bytes().to_ascii_uppercase();
        if upper == b"INDENT" || upper == b"NEWLINE" || upper == b"SPACE" {
            i += 2;
            continue;
        }
        let path_str = std::str::from_utf8(arg.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string();
        paths.push(path_str);
        i += 1;
    }
    if paths.is_empty() {
        paths.push("$".to_string());
    }

    let root_opt = get_json_clone(ctx.db().lookup_key_read(&key))?;
    let root = match root_opt {
        None => return ctx.reply_null(),
        Some(v) => v,
    };

    if paths.len() == 1 && paths[0] == "$" {
        let json = root.to_string();
        return ctx.reply_bulk(json.as_bytes());
    }

    if paths.len() == 1 {
        let matches = query_path_owned(&root, &paths[0])?;
        let out = serialize_matches(&matches);
        return ctx.reply_bulk(out.as_bytes());
    }

    let mut combined = serde_json::Map::new();
    for path in &paths {
        let matches = query_path_owned(&root, path)?;
        let arr: Value = Value::Array(matches);
        combined.insert(path.clone(), arr);
    }
    let out = Value::Object(combined).to_string();
    ctx.reply_bulk(out.as_bytes())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.DEL key [path] / JSON.FORGET key [path]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_del_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"JSON.DEL"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_str = if argc >= 3 {
        let p = ctx.arg_owned(2usize)?;
        std::str::from_utf8(p.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string()
    } else {
        "$".to_string()
    };

    if path_str == "$" {
        let existed = ctx.db().lookup_key_read(&key).is_some();
        if existed {
            ctx.db_mut().delete(&key);
            return ctx.reply_integer(1);
        }
        return ctx.reply_integer(0);
    }

    let tokens = lex_path(&path_str)?;
    let existing = ctx.db_mut().lookup_key_write(&key);
    let count = match existing {
        None => 0i64,
        Some(obj) => {
            if !obj.is_json() {
                return Err(RedisError::wrong_type());
            }
            let root = match &mut obj.kind {
                ObjectKind::Json(v) => v,
                _ => unreachable!(),
            };
            delete_paths(root, &tokens)
        }
    };
    ctx.reply_integer(count)
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.TYPE key [path]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_type_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"JSON.TYPE"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_str = if argc >= 3 {
        let p = ctx.arg_owned(2usize)?;
        std::str::from_utf8(p.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string()
    } else {
        "$".to_string()
    };

    let root_opt = get_json_clone(ctx.db().lookup_key_read(&key))?;
    let root = match root_opt {
        None => return ctx.reply_null(),
        Some(v) => v,
    };

    let matches = query_path_owned(&root, &path_str)?;
    let type_names: Vec<&'static str> = matches.iter().map(|m| json_type_name(m)).collect();
    ctx.reply_array_header(type_names.len())?;
    for tn in type_names {
        ctx.reply_bulk(tn.as_bytes())?;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.NUMINCRBY key path value
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_numincrby_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(b"JSON.NUMINCRBY"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_arg = ctx.arg_owned(2usize)?;
    let inc_arg = ctx.arg_owned(3usize)?;

    let path_str = std::str::from_utf8(path_arg.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
        .to_string();
    let inc: f64 = std::str::from_utf8(inc_arg.as_bytes())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RedisError::runtime(b"ERR value is not a number"))?;

    let existing = ctx.db_mut().lookup_key_write(&key);
    let root = match existing {
        None => return ctx.reply_null(),
        Some(obj) => {
            if !obj.is_json() {
                return Err(RedisError::wrong_type());
            }
            match &mut obj.kind {
                ObjectKind::Json(v) => v,
                _ => unreachable!(),
            }
        }
    };

    let tokens = lex_path(&path_str)?;
    let results = num_op(root, &tokens, inc, false)?;
    let out = serialize_matches(&results);
    ctx.reply_bulk(out.as_bytes())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.NUMMULTBY key path value
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_nummultby_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(b"JSON.NUMMULTBY"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_arg = ctx.arg_owned(2usize)?;
    let mul_arg = ctx.arg_owned(3usize)?;

    let path_str = std::str::from_utf8(path_arg.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
        .to_string();
    let mul: f64 = std::str::from_utf8(mul_arg.as_bytes())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RedisError::runtime(b"ERR value is not a number"))?;

    let existing = ctx.db_mut().lookup_key_write(&key);
    let root = match existing {
        None => return ctx.reply_null(),
        Some(obj) => {
            if !obj.is_json() {
                return Err(RedisError::wrong_type());
            }
            match &mut obj.kind {
                ObjectKind::Json(v) => v,
                _ => unreachable!(),
            }
        }
    };

    let tokens = lex_path(&path_str)?;
    let results = num_op(root, &tokens, mul, true)?;
    let out = serialize_matches(&results);
    ctx.reply_bulk(out.as_bytes())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.STRAPPEND key [path] value
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_strappend_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"JSON.STRAPPEND"));
    }
    let key = ctx.arg_owned(1usize)?;
    let (path_str, append_arg) = if argc == 3 {
        ("$".to_string(), ctx.arg_owned(2usize)?)
    } else {
        let p = ctx.arg_owned(2usize)?;
        let ps = std::str::from_utf8(p.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string();
        (ps, ctx.arg_owned(3usize)?)
    };

    let append_val: Value = serde_json::from_slice(append_arg.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR invalid JSON for appended value"))?;
    let append_str = match &append_val {
        Value::String(s) => s.clone(),
        _ => {
            return Err(RedisError::runtime(
                b"ERR appended value is not a JSON string",
            ))
        }
    };

    let tokens = lex_path(&path_str)?;

    let existing = ctx.db_mut().lookup_key_write(&key);
    let root = match existing {
        None => return ctx.reply_null(),
        Some(obj) => {
            if !obj.is_json() {
                return Err(RedisError::wrong_type());
            }
            match &mut obj.kind {
                ObjectKind::Json(v) => v,
                _ => unreachable!(),
            }
        }
    };

    let results = strappend_op(root, &tokens, &append_str);
    ctx.reply_array_header(results.len())?;
    for r in results {
        match r {
            Some(len) => ctx.reply_integer(len as i64)?,
            None => ctx.reply_null()?,
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.STRLEN key [path]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_strlen_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"JSON.STRLEN"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_str = if argc >= 3 {
        let p = ctx.arg_owned(2usize)?;
        std::str::from_utf8(p.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string()
    } else {
        "$".to_string()
    };

    let root_opt = get_json_clone(ctx.db().lookup_key_read(&key))?;
    let root = match root_opt {
        None => return ctx.reply_null(),
        Some(v) => v,
    };

    let matches = query_path_owned(&root, &path_str)?;
    let lengths: Vec<Option<i64>> = matches
        .iter()
        .map(|m| match m {
            Value::String(s) => Some(s.len() as i64),
            _ => None,
        })
        .collect();
    ctx.reply_array_header(lengths.len())?;
    for r in lengths {
        match r {
            Some(n) => ctx.reply_integer(n)?,
            None => ctx.reply_null()?,
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.OBJKEYS key [path]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_objkeys_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"JSON.OBJKEYS"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_str = if argc >= 3 {
        let p = ctx.arg_owned(2usize)?;
        std::str::from_utf8(p.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string()
    } else {
        "$".to_string()
    };

    let root_opt = get_json_clone(ctx.db().lookup_key_read(&key))?;
    let root = match root_opt {
        None => return ctx.reply_null(),
        Some(v) => v,
    };

    let matches = query_path_owned(&root, &path_str)?;
    ctx.reply_array_header(matches.len())?;
    for m in matches {
        match m {
            Value::Object(map) => {
                let keys: Vec<String> = map.keys().cloned().collect();
                ctx.reply_array_header(keys.len())?;
                for k in keys {
                    ctx.reply_bulk(k.as_bytes())?;
                }
            }
            _ => ctx.reply_null()?,
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.OBJLEN key [path]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_objlen_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"JSON.OBJLEN"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_str = if argc >= 3 {
        let p = ctx.arg_owned(2usize)?;
        std::str::from_utf8(p.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string()
    } else {
        "$".to_string()
    };

    let root_opt = get_json_clone(ctx.db().lookup_key_read(&key))?;
    let root = match root_opt {
        None => return ctx.reply_null(),
        Some(v) => v,
    };

    let matches = query_path_owned(&root, &path_str)?;
    let lens: Vec<Option<i64>> = matches
        .iter()
        .map(|m| match m {
            Value::Object(map) => Some(map.len() as i64),
            _ => None,
        })
        .collect();
    ctx.reply_array_header(lens.len())?;
    for r in lens {
        match r {
            Some(n) => ctx.reply_integer(n)?,
            None => ctx.reply_null()?,
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.ARRAPPEND key path value [value...]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_arrappend_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"JSON.ARRAPPEND"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_arg = ctx.arg_owned(2usize)?;
    let path_str = std::str::from_utf8(path_arg.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
        .to_string();

    let mut new_vals: Vec<Value> = Vec::new();
    for i in 3..argc {
        let arg = ctx.arg_owned(i)?;
        let val: Value = serde_json::from_slice(arg.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR invalid JSON value"))?;
        new_vals.push(val);
    }

    let tokens = lex_path(&path_str)?;

    let existing = ctx.db_mut().lookup_key_write(&key);
    let root = match existing {
        None => return ctx.reply_null(),
        Some(obj) => {
            if !obj.is_json() {
                return Err(RedisError::wrong_type());
            }
            match &mut obj.kind {
                ObjectKind::Json(v) => v,
                _ => unreachable!(),
            }
        }
    };

    let results = arrappend_op(root, &tokens, &new_vals);
    ctx.reply_array_header(results.len())?;
    for r in results {
        match r {
            Some(len) => ctx.reply_integer(len as i64)?,
            None => ctx.reply_null()?,
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.ARRLEN key [path]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_arrlen_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"JSON.ARRLEN"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_str = if argc >= 3 {
        let p = ctx.arg_owned(2usize)?;
        std::str::from_utf8(p.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string()
    } else {
        "$".to_string()
    };

    let root_opt = get_json_clone(ctx.db().lookup_key_read(&key))?;
    let root = match root_opt {
        None => return ctx.reply_null(),
        Some(v) => v,
    };

    let matches = query_path_owned(&root, &path_str)?;
    let lens: Vec<Option<i64>> = matches
        .iter()
        .map(|m| match m {
            Value::Array(arr) => Some(arr.len() as i64),
            _ => None,
        })
        .collect();
    ctx.reply_array_header(lens.len())?;
    for r in lens {
        match r {
            Some(n) => ctx.reply_integer(n)?,
            None => ctx.reply_null()?,
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.ARRINSERT key path index value [value...]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_arrinsert_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 5 {
        return Err(RedisError::wrong_number_of_args(b"JSON.ARRINSERT"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_arg = ctx.arg_owned(2usize)?;
    let path_str = std::str::from_utf8(path_arg.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
        .to_string();
    let idx_arg = ctx.arg_owned(3usize)?;
    let raw_idx: i64 = std::str::from_utf8(idx_arg.as_bytes())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RedisError::runtime(b"ERR index is not an integer"))?;

    let mut new_vals: Vec<Value> = Vec::new();
    for i in 4..argc {
        let arg = ctx.arg_owned(i)?;
        let val: Value = serde_json::from_slice(arg.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR invalid JSON value"))?;
        new_vals.push(val);
    }

    let tokens = lex_path(&path_str)?;

    let existing = ctx.db_mut().lookup_key_write(&key);
    let root = match existing {
        None => return ctx.reply_null(),
        Some(obj) => {
            if !obj.is_json() {
                return Err(RedisError::wrong_type());
            }
            match &mut obj.kind {
                ObjectKind::Json(v) => v,
                _ => unreachable!(),
            }
        }
    };

    let results = arrinsert_op(root, &tokens, raw_idx, &new_vals);
    ctx.reply_array_header(results.len())?;
    for r in results {
        match r {
            Some(len) => ctx.reply_integer(len as i64)?,
            None => ctx.reply_null()?,
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.ARRPOP key [path [index]]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_arrpop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"JSON.ARRPOP"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_str = if argc >= 3 {
        let p = ctx.arg_owned(2usize)?;
        std::str::from_utf8(p.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string()
    } else {
        "$".to_string()
    };
    let pop_idx: i64 = if argc >= 4 {
        let idx_arg = ctx.arg_owned(3usize)?;
        std::str::from_utf8(idx_arg.as_bytes())
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RedisError::runtime(b"ERR index is not an integer"))?
    } else {
        -1
    };

    let tokens = lex_path(&path_str)?;

    let existing = ctx.db_mut().lookup_key_write(&key);
    let root = match existing {
        None => return ctx.reply_null(),
        Some(obj) => {
            if !obj.is_json() {
                return Err(RedisError::wrong_type());
            }
            match &mut obj.kind {
                ObjectKind::Json(v) => v,
                _ => unreachable!(),
            }
        }
    };

    let results = arrpop_op(root, &tokens, pop_idx);
    ctx.reply_array_header(results.len())?;
    for r in results {
        match r {
            Some(v) => ctx.reply_bulk(v.to_string().as_bytes())?,
            None => ctx.reply_null()?,
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.CLEAR key [path]
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_clear_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"JSON.CLEAR"));
    }
    let key = ctx.arg_owned(1usize)?;
    let path_str = if argc >= 3 {
        let p = ctx.arg_owned(2usize)?;
        std::str::from_utf8(p.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
            .to_string()
    } else {
        "$".to_string()
    };

    let tokens = lex_path(&path_str)?;

    let existing = ctx.db_mut().lookup_key_write(&key);
    let count = match existing {
        None => 0i64,
        Some(obj) => {
            if !obj.is_json() {
                return Err(RedisError::wrong_type());
            }
            let root = match &mut obj.kind {
                ObjectKind::Json(v) => v,
                _ => unreachable!(),
            };
            clear_op(root, &tokens)
        }
    };
    ctx.reply_integer(count)
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON.MGET key [key...] path
// ─────────────────────────────────────────────────────────────────────────────

pub fn json_mget_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"JSON.MGET"));
    }
    let path_arg = ctx.arg_owned(argc - 1)?;
    let path_str = std::str::from_utf8(path_arg.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR path is not valid UTF-8"))?
        .to_string();

    let key_count = argc - 2;
    let mut keys: Vec<RedisString> = Vec::with_capacity(key_count);
    for i in 1..argc - 1 {
        keys.push(ctx.arg_owned(i)?);
    }

    let mut results: Vec<Option<String>> = Vec::with_capacity(key_count);
    for key in &keys {
        let root_opt = get_json_clone(ctx.db().lookup_key_read(key));
        match root_opt {
            Ok(None) => results.push(None),
            Ok(Some(root)) => match query_path_owned(&root, &path_str) {
                Ok(matches) => results.push(Some(serialize_matches(&matches))),
                Err(_) => results.push(None),
            },
            Err(_) => results.push(None),
        }
    }

    ctx.reply_array_header(results.len())?;
    for r in results {
        match r {
            Some(s) => ctx.reply_bulk(s.as_bytes())?,
            None => ctx.reply_null()?,
        }
    }
    Ok(())
}
