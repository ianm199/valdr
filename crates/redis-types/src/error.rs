//! `RedisError` — the canonical Redis error type.
//! Per PORTING.md §6: a byte-string-payloaded enum (Redis errors round-
//! trip through RESP and may not be UTF-8 for user-supplied keys
//! appearing in messages). Constructors per §6.1 match the verbatim
//! C-Redis error strings so wire-diff and Tcl tests pass.

use crate::string::RedisString;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisError {
 /// Generic runtime error with arbitrary message (the most common variant).
    Runtime(RedisString),
 /// WRONGTYPE — operation against a key holding the wrong kind of value.
    WrongType,
 /// `wrong number of arguments for '<cmd>' command`.
    WrongNumberOfArgs(RedisString),
 /// Syntax error in command arguments.
    Syntax(RedisString),
 /// `LOADING Redis is loading the dataset in memory`.
    Loading,
 /// `NOAUTH Authentication required.`.
    NoAuth,
 /// `NOPERM...` — ACL deny, with scope detail.
    NoPerm(RedisString),
 /// `value is out of range`.
    OutOfRange,
 /// `value is not an integer or out of range`.
    NotInteger,
 /// `value is not a valid float`.
    NotFloat,
 /// I/O underneath (connection closed, write failure).
    Io(std::io::ErrorKind),
}

impl RedisError {
    pub fn runtime(msg: impl AsRef<[u8]>) -> Self {
        RedisError::Runtime(RedisString::from_bytes(msg))
    }

    pub fn wrong_type() -> Self {
        RedisError::WrongType
    }

    pub fn wrong_number_of_args(cmd: impl AsRef<[u8]>) -> Self {
        RedisError::WrongNumberOfArgs(RedisString::from_bytes(cmd))
    }

    pub fn syntax(msg: impl AsRef<[u8]>) -> Self {
        RedisError::Syntax(RedisString::from_bytes(msg))
    }

    pub fn loading() -> Self {
        RedisError::Loading
    }

    pub fn no_auth() -> Self {
        RedisError::NoAuth
    }

    pub fn no_perm(scope: impl AsRef<[u8]>) -> Self {
        RedisError::NoPerm(RedisString::from_bytes(scope))
    }

    pub fn out_of_range() -> Self {
        RedisError::OutOfRange
    }

    pub fn not_integer() -> Self {
        RedisError::NotInteger
    }

    pub fn not_float() -> Self {
        RedisError::NotFloat
    }

    pub fn io(kind: std::io::ErrorKind) -> Self {
        RedisError::Io(kind)
    }

 /// The RESP error-line bytes (without the leading `-` and trailing CRLF).
 /// Used by the reply writer when serializing this error onto the wire.
    pub fn to_resp_payload(&self) -> RedisString {
        use RedisError::*;
        let mut buf = Vec::new();
        match self {
            Runtime(s) => buf.extend_from_slice(s.as_bytes()),
            WrongType => buf.extend_from_slice(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"),
            WrongNumberOfArgs(cmd) => {
                buf.extend_from_slice(b"ERR wrong number of arguments for '");
                buf.extend_from_slice(cmd.as_bytes());
                buf.extend_from_slice(b"' command");
            }
            Syntax(msg) => {
                if msg.is_empty() {
                    buf.extend_from_slice(b"ERR syntax error");
                } else {
                    buf.extend_from_slice(b"ERR ");
                    buf.extend_from_slice(msg.as_bytes());
                }
            }
            Loading => buf.extend_from_slice(
                b"LOADING Redis is loading the dataset in memory"),
            NoAuth => buf.extend_from_slice(b"NOAUTH Authentication required."),
            NoPerm(scope) => {
                buf.extend_from_slice(b"NOPERM ");
                buf.extend_from_slice(scope.as_bytes());
            }
            OutOfRange => buf.extend_from_slice(
                b"ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
            ),
            NotInteger => buf.extend_from_slice(b"ERR value is not an integer or out of range"),
            NotFloat => buf.extend_from_slice(b"ERR value is not a valid float"),
            Io(kind) => {
                buf.extend_from_slice(b"ERR I/O error: ");
                buf.extend_from_slice(format!("{:?}", kind).as_bytes());
            }
        }
        RedisString::from_vec(buf)
    }
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload = self.to_resp_payload();
        match std::str::from_utf8(payload.as_bytes()) {
            Ok(s) => f.write_str(s),
            Err(_) => write!(f, "{:?}", payload),
        }
    }
}

impl std::error::Error for RedisError {}

impl From<std::io::Error> for RedisError {
    fn from(e: std::io::Error) -> Self {
        RedisError::Io(e.kind())
    }
}

pub type RedisResult<T> = Result<T, RedisError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrong_type_message_verbatim() {
        let e = RedisError::wrong_type();
        assert_eq!(
            e.to_resp_payload().as_bytes(),
            b"WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[test]
    fn wrong_number_of_args_interpolates_cmd() {
        let e = RedisError::wrong_number_of_args(b"SET");
        assert_eq!(
            e.to_resp_payload().as_bytes(),
            b"ERR wrong number of arguments for 'SET' command"
        );
    }

    #[test]
    fn not_integer_verbatim() {
        let e = RedisError::not_integer();
        assert_eq!(
            e.to_resp_payload().as_bytes(),
            b"ERR value is not an integer or out of range"
        );
    }

    #[test]
    fn runtime_passes_payload_through() {
        let e = RedisError::runtime(b"custom message");
        assert_eq!(e.to_resp_payload().as_bytes(), b"custom message");
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (PORTING.md §6 + §6.1); error-sites.tsv informs constructor set
//   target_crate:  redis-types
//   confidence:    high
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Byte-string payloads (NOT String). to_resp_payload() builds wire-compat error lines.
// ──────────────────────────────────────────────────────────────────────────
