//! Minimal Cluster command foundations.
//!
//! This module intentionally starts with key-slot calculation only. It does not
//! enable cluster mode, node metadata, redirection, gossip, or failover.

use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult};

pub const CLUSTER_SLOTS: u16 = 16_384;

pub fn cluster_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"cluster"));
    }
    let sub = ctx.arg(1)?.as_bytes();
    if ascii_eq_ignore_case(sub, b"KEYSLOT") {
        return cluster_keyslot_command(ctx);
    }

    let mut msg = Vec::with_capacity(b"ERR unknown CLUSTER subcommand ".len() + sub.len());
    msg.extend_from_slice(b"ERR unknown CLUSTER subcommand ");
    msg.extend_from_slice(sub);
    Err(RedisError::runtime(msg))
}

fn cluster_keyslot_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"cluster|keyslot"));
    }
    let slot = key_slot(ctx.arg(2)?.as_bytes());
    ctx.reply_integer(slot as i64)
}

pub fn key_slot(key: &[u8]) -> u16 {
    crc16_xmodem(hashtag(key)) % CLUSTER_SLOTS
}

pub fn hashtag(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|b| *b == b'{') else {
        return key;
    };
    let rest = &key[open + 1..];
    let Some(close_rel) = rest.iter().position(|b| *b == b'}') else {
        return key;
    };
    if close_rel == 0 {
        return key;
    }
    &rest[..close_rel]
}

pub fn crc16_xmodem(bytes: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &byte in bytes {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::{Client, CommandContext};
    use redis_types::RedisString;

    #[test]
    fn cluster_crc16_matches_standard_vector() {
        assert_eq!(crc16_xmodem(b"123456789"), 0x31c3);
    }

    #[test]
    fn cluster_keyslot_matches_known_vectors() {
        assert_eq!(key_slot(b""), 0);
        assert_eq!(key_slot(b"123456789"), 12_739);
        assert_eq!(key_slot(b"foo"), 12_182);
        assert_eq!(key_slot(b"bar"), 5_061);
        assert_eq!(key_slot(b"somekey"), 11_058);
        assert_eq!(key_slot(b"{user1000}.following"), 3_443);
        assert_eq!(key_slot(b"{user1000}.followers"), 3_443);
    }

    #[test]
    fn cluster_hashtag_uses_first_non_empty_braced_region() {
        assert_eq!(hashtag(b"foo{bar}zap"), b"bar");
        assert_eq!(hashtag(b"{bar}foo"), b"bar");
        assert_eq!(hashtag(b"foo{}{bar}"), b"foo{}{bar}");
        assert_eq!(hashtag(b"foo{{bar}}zap"), b"{bar");
        assert_eq!(hashtag(b"foo{bar}{zap}"), b"bar");
        assert_eq!(hashtag(b"foo{bar"), b"foo{bar");
    }

    #[test]
    fn cluster_keyslot_command_replies_with_slot() {
        let mut c = Client::new(1);
        c.set_args(vec![
            RedisString::from_bytes(b"CLUSTER"),
            RedisString::from_bytes(b"KEYSLOT"),
            RedisString::from_bytes(b"foo"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        cluster_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b":12182\r\n");
    }

    #[test]
    fn cluster_keyslot_dispatches_from_parent_command() {
        let mut c = Client::new(2);
        c.set_args(vec![
            RedisString::from_bytes(b"CLUSTER"),
            RedisString::from_bytes(b"KEYSLOT"),
            RedisString::from_bytes(b"{user1000}.followers"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        crate::dispatch::dispatch(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b":3443\r\n");
    }
}
