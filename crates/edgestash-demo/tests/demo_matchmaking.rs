//! Skill-based matchmaking: players queue by rating and are atomically paired
//! with the closest opponent inside a skill band, and can never be
//! double-matched.
//!
//! The model is one Durable Object per matchmaking pool (a region+mode tuple).
//! That object owns a single sorted set, `queue`, scored by player rating.
//! Because Cloudflare serializes requests to a single Durable Object and each
//! Lua `EVALSHA` runs atomically inside the Valdr engine, the band query and the
//! dual `ZREM` that claims a pair execute as one indivisible step: no two
//! `FIND_MATCH` calls can observe the same opponent as available and both
//! remove it. That is what makes "never double-matched" a hard invariant rather
//! than a race we hope to lose.
//!
//! Two Lua scripts, installed over the wire via `SCRIPT LOAD` + `EVALSHA`:
//!   * `ENQUEUE`    — idempotently places a player in the queue at their rating.
//!     A re-enqueue of a player already present returns `queued` without adding
//!     a duplicate (a sorted set is a set; `ZADD` would merely overwrite the
//!     score, but the explicit `ZSCORE` guard keeps the reply uniform and proves
//!     the idempotency in one place).
//!   * `FIND_MATCH` — reads the caller's rating, queries the skill band with
//!     `ZRANGEBYSCORE queue rating-band rating+band`, picks the candidate whose
//!     rating is closest to the caller's (ties broken by the engine's
//!     score-then-member-lex order), and atomically `ZREM`s both the caller and
//!     that opponent. If the only in-band candidate is the caller itself it
//!     returns `waiting`; if the caller is not enqueued at all it errors
//!     `NOTQUEUED`.
//!
//! ## Band bounds: inclusive
//!
//! The band query uses inclusive bounds — `ZRANGEBYSCORE queue (rating-band)
//! (rating+band)` with no `(` exclusivity prefix. A candidate exactly `band`
//! rating points away therefore *is* a match. `band` is the widest acceptable
//! skill gap, and "exactly at the edge" is acceptable by definition; an
//! exclusive bound would make `band` mean "strictly closer than band", which is
//! the less intuitive contract for a tuning knob a matchmaker operator sets.
//! `band_boundary_is_inclusive` pins this choice.
//!
//! ## Why FIND_MATCH returns an array, not `{ok=...}`
//!
//! The Valdr Lua bridge follows Redis status-reply semantics: a returned table
//! with an `ok` field collapses to a bare simple-string reply and every other
//! field is discarded. To surface the opponent and its rating alongside the
//! `matched` verdict, `FIND_MATCH` returns the Lua array `{'matched', opponent,
//! opponent_rating}`, which the REST adapter renders as
//! `{"result": ["matched", "<opponent>", <rating>]}`. The payload-free `waiting`
//! verdict is `{'waiting'}` (a one-element array) and the not-enqueued failure
//! is `{err='NOTQUEUED'}` (a non-200 HTTP error).

use edgestash_demo::{EdgeHttpRequest, EdgeObject, MemoryObjectStorage};
use serde_json::{json, Value as JsonValue};

/// Idempotently enqueue a player. `KEYS = [queue]`, `ARGV = [player, rating]`.
/// A player already present is left at their existing rating and reported
/// `queued` without a duplicate insert.
const ENQUEUE_SCRIPT: &str = r#"
    local queue = KEYS[1]
    local player = ARGV[1]
    local rating = tonumber(ARGV[2])
    local existing = redis.call('ZSCORE', queue, player)
    if existing then
        return {'queued', tonumber(existing)}
    end
    redis.call('ZADD', queue, rating, player)
    return {'queued', rating}
"#;

/// Atomically pair the caller with the closest in-band opponent.
/// `KEYS = [queue]`, `ARGV = [player, band]`. Returns one of:
///   * `{'matched', opponent, opponent_rating}` — both removed from the queue,
///   * `{'waiting'}` — the caller is the only in-band candidate, stays queued,
///   * `{err='NOTQUEUED'}` — the caller is not in the queue.
///
/// The band is inclusive: `ZRANGEBYSCORE` is called with bare bounds so a
/// candidate exactly `band` away qualifies. Candidates arrive ascending by
/// score (then member-lex); the loop tracks the minimum absolute rating
/// difference, skipping the caller's own entry, so the chosen opponent is the
/// closest skill match, not merely the first in range.
const FIND_MATCH_SCRIPT: &str = r#"
    local queue = KEYS[1]
    local player = ARGV[1]
    local band = tonumber(ARGV[2])
    local rating = redis.call('ZSCORE', queue, player)
    if not rating then
        return {err='NOTQUEUED player is not in the queue'}
    end
    rating = tonumber(rating)
    local low = tostring(rating - band)
    local high = tostring(rating + band)
    local candidates = redis.call('ZRANGEBYSCORE', queue, low, high, 'WITHSCORES')
    local best_player = nil
    local best_rating = nil
    local best_diff = nil
    local index = 1
    while index < #candidates do
        local candidate = candidates[index]
        local candidate_rating = tonumber(candidates[index + 1])
        if candidate ~= player then
            local diff = candidate_rating - rating
            if diff < 0 then diff = -diff end
            if best_diff == nil or diff < best_diff then
                best_player = candidate
                best_rating = candidate_rating
                best_diff = diff
            end
        end
        index = index + 2
    end
    if best_player == nil then
        return {'waiting'}
    end
    redis.call('ZREM', queue, player, best_player)
    return {'matched', best_player, best_rating}
"#;

fn open(storage: MemoryObjectStorage) -> EdgeObject<MemoryObjectStorage> {
    EdgeObject::open(storage).unwrap()
}

fn body_json(body: &[u8]) -> JsonValue {
    serde_json::from_slice(body).unwrap()
}

fn get(
    object: &mut EdgeObject<MemoryObjectStorage>,
    path: &str,
    now_millis: u64,
) -> (u16, JsonValue) {
    let response = object.handle_http(EdgeHttpRequest::get(path, now_millis));
    (response.status, body_json(&response.body))
}

fn post(
    object: &mut EdgeObject<MemoryObjectStorage>,
    path: &str,
    body: &[u8],
    now_millis: u64,
) -> (u16, JsonValue) {
    let response = object.handle_http(EdgeHttpRequest::post(path, body, now_millis));
    (response.status, body_json(&response.body))
}

fn load_script(
    object: &mut EdgeObject<MemoryObjectStorage>,
    tenant: &str,
    script: &str,
    now_millis: u64,
) -> String {
    let (status, value) = post(
        object,
        &format!("/v1/valdr/{tenant}/SCRIPT/LOAD"),
        script.as_bytes(),
        now_millis,
    );
    assert_eq!(status, 200, "SCRIPT LOAD failed: {value}");
    value["result"].as_str().unwrap().to_owned()
}

/// The pool tenant: one Durable Object per region+mode matchmaking pool.
const POOL: &str = "na-ranked";
/// The single sorted set the pool owns, scored by player rating.
const QUEUE: &str = "queue";
/// The skill band: the widest acceptable rating gap between paired players.
const BAND: i64 = 100;

fn enqueue(
    object: &mut EdgeObject<MemoryObjectStorage>,
    enqueue_sha: &str,
    player: &str,
    rating: i64,
    now_millis: u64,
) {
    let path = format!("/v1/valdr/{POOL}/EVALSHA/{enqueue_sha}/1/{QUEUE}/{player}/{rating}");
    let (status, value) = get(object, &path, now_millis);
    assert_eq!(status, 200, "ENQUEUE failed for {player}: {value}");
    assert_eq!(
        value,
        json!({"result": ["queued", rating]}),
        "ENQUEUE reply for {player}"
    );
}

/// The outcome of one `FIND_MATCH` call, modelled so a test can assert on the
/// verdict without re-parsing the raw array each time.
enum MatchResult {
    Matched { opponent: String, rating: i64 },
    Waiting,
    NotQueued,
}

fn find_match(
    object: &mut EdgeObject<MemoryObjectStorage>,
    find_sha: &str,
    player: &str,
    now_millis: u64,
) -> MatchResult {
    let path = format!("/v1/valdr/{POOL}/EVALSHA/{find_sha}/1/{QUEUE}/{player}/{BAND}");
    let (status, value) = get(object, &path, now_millis);
    if status != 200 {
        assert!(
            value["error"].as_str().unwrap().starts_with("NOTQUEUED"),
            "non-200 FIND_MATCH must be NOTQUEUED: {value}"
        );
        return MatchResult::NotQueued;
    }
    let array = value["result"].as_array().unwrap();
    match array[0].as_str().unwrap() {
        "matched" => MatchResult::Matched {
            opponent: array[1].as_str().unwrap().to_owned(),
            rating: array[2].as_i64().unwrap(),
        },
        "waiting" => MatchResult::Waiting,
        other => panic!("unexpected FIND_MATCH verdict {other}: {value}"),
    }
}

fn zcard(object: &mut EdgeObject<MemoryObjectStorage>, now_millis: u64) -> i64 {
    let (status, value) = get(
        object,
        &format!("/v1/valdr/{POOL}/ZCARD/{QUEUE}"),
        now_millis,
    );
    assert_eq!(status, 200, "ZCARD failed: {value}");
    value["result"].as_i64().unwrap()
}

/// The six seed players: two close pairs and two ratings isolated in their own
/// band. 1000/1040 are 40 apart (in band), 1500/1520 are 20 apart (in band),
/// 2000 and 2400 are each 400+ from every other player (alone in band).
const SEEDS: [(&str, i64); 6] = [
    ("p-1000", 1000),
    ("p-1040", 1040),
    ("p-1500", 1500),
    ("p-1520", 1520),
    ("p-2000", 2000),
    ("p-2400", 2400),
];

fn seed_pool(object: &mut EdgeObject<MemoryObjectStorage>, enqueue_sha: &str, now_millis: u64) {
    for (offset, (player, rating)) in SEEDS.iter().enumerate() {
        enqueue(
            object,
            enqueue_sha,
            player,
            *rating,
            now_millis + offset as u64,
        );
    }
}

#[test]
fn close_players_pair_and_isolated_ratings_wait() {
    let mut object = open(MemoryObjectStorage::default());
    let enqueue_sha = load_script(&mut object, POOL, ENQUEUE_SCRIPT, 1_000);
    let find_sha = load_script(&mut object, POOL, FIND_MATCH_SCRIPT, 1_000);
    seed_pool(&mut object, &enqueue_sha, 1_001);
    assert_eq!(zcard(&mut object, 1_100), 6, "all six seeds are queued");

    let before = zcard(&mut object, 2_000);
    match find_match(&mut object, &find_sha, "p-1000", 2_001) {
        MatchResult::Matched { opponent, rating } => {
            assert_eq!(opponent, "p-1040", "1000 pairs with its in-band neighbour");
            assert_eq!(rating, 1040);
            assert!(
                (rating - 1000).abs() <= BAND,
                "a matched pair is always within band"
            );
        }
        _ => panic!("p-1000 must match p-1040"),
    }
    assert_eq!(
        zcard(&mut object, 2_002),
        before - 2,
        "a match removes exactly two players, never one or three"
    );

    let before = zcard(&mut object, 2_003);
    match find_match(&mut object, &find_sha, "p-1500", 2_004) {
        MatchResult::Matched { opponent, rating } => {
            assert_eq!(opponent, "p-1520", "1500 pairs with 1520");
            assert_eq!(rating, 1520);
            assert!((rating - 1500).abs() <= BAND);
        }
        _ => panic!("p-1500 must match p-1520"),
    }
    assert_eq!(
        zcard(&mut object, 2_005),
        before - 2,
        "second match also drops the card by exactly two"
    );

    for isolated in ["p-2000", "p-2400"] {
        let before = zcard(&mut object, 2_006);
        assert!(
            matches!(
                find_match(&mut object, &find_sha, isolated, 2_007),
                MatchResult::Waiting
            ),
            "{isolated} is alone in its band and must wait"
        );
        assert_eq!(
            zcard(&mut object, 2_008),
            before,
            "a waiting player stays queued; card unchanged"
        );
    }

    assert_eq!(
        zcard(&mut object, 2_009),
        2,
        "two pairs were removed; only the two isolated players remain"
    );
}

#[test]
fn a_matched_player_cannot_be_matched_again() {
    let mut object = open(MemoryObjectStorage::default());
    let enqueue_sha = load_script(&mut object, POOL, ENQUEUE_SCRIPT, 1_000);
    let find_sha = load_script(&mut object, POOL, FIND_MATCH_SCRIPT, 1_000);
    enqueue(&mut object, &enqueue_sha, "p-1000", 1000, 1_001);
    enqueue(&mut object, &enqueue_sha, "p-1040", 1040, 1_002);

    let matched = find_match(&mut object, &find_sha, "p-1000", 2_000);
    assert!(
        matches!(&matched, MatchResult::Matched { opponent, .. } if opponent == "p-1040"),
        "the pair is claimed"
    );
    assert_eq!(zcard(&mut object, 2_001), 0, "both players left the queue");

    assert!(
        matches!(
            find_match(&mut object, &find_sha, "p-1000", 2_002),
            MatchResult::NotQueued
        ),
        "the caller was removed by its own match and is now NOTQUEUED"
    );
    assert!(
        matches!(
            find_match(&mut object, &find_sha, "p-1040", 2_003),
            MatchResult::NotQueued
        ),
        "the opponent was removed by the same match and is now NOTQUEUED"
    );
}

#[test]
fn band_boundary_is_inclusive() {
    let mut object = open(MemoryObjectStorage::default());
    let enqueue_sha = load_script(&mut object, POOL, ENQUEUE_SCRIPT, 1_000);
    let find_sha = load_script(&mut object, POOL, FIND_MATCH_SCRIPT, 1_000);

    enqueue(&mut object, &enqueue_sha, "p-edge-lo", 1000, 1_001);
    enqueue(&mut object, &enqueue_sha, "p-edge-hi", 1000 + BAND, 1_002);

    match find_match(&mut object, &find_sha, "p-edge-lo", 2_000) {
        MatchResult::Matched { opponent, rating } => {
            assert_eq!(
                opponent, "p-edge-hi",
                "a player exactly BAND away matches under inclusive bounds"
            );
            assert_eq!(rating, 1000 + BAND);
            assert_eq!(
                (rating - 1000).abs(),
                BAND,
                "the gap is exactly BAND and still counts as in-band"
            );
        }
        _ => panic!("inclusive band must pair players exactly BAND apart"),
    }
}

#[test]
fn just_outside_the_band_does_not_match() {
    let mut object = open(MemoryObjectStorage::default());
    let enqueue_sha = load_script(&mut object, POOL, ENQUEUE_SCRIPT, 1_000);
    let find_sha = load_script(&mut object, POOL, FIND_MATCH_SCRIPT, 1_000);

    enqueue(&mut object, &enqueue_sha, "p-lo", 1000, 1_001);
    enqueue(&mut object, &enqueue_sha, "p-hi", 1000 + BAND + 1, 1_002);

    assert!(
        matches!(
            find_match(&mut object, &find_sha, "p-lo", 2_000),
            MatchResult::Waiting
        ),
        "a candidate BAND+1 away is outside the inclusive range and must not match"
    );
    assert_eq!(zcard(&mut object, 2_001), 2, "both stay queued");
}

#[test]
fn closest_opponent_is_chosen_over_a_nearer_in_band_neighbour() {
    let mut object = open(MemoryObjectStorage::default());
    let enqueue_sha = load_script(&mut object, POOL, ENQUEUE_SCRIPT, 1_000);
    let find_sha = load_script(&mut object, POOL, FIND_MATCH_SCRIPT, 1_000);

    enqueue(&mut object, &enqueue_sha, "p-1000", 1000, 1_001);
    enqueue(&mut object, &enqueue_sha, "p-1090", 1090, 1_002);
    enqueue(&mut object, &enqueue_sha, "p-1010", 1010, 1_003);

    match find_match(&mut object, &find_sha, "p-1000", 2_000) {
        MatchResult::Matched { opponent, rating } => {
            assert_eq!(
                opponent, "p-1010",
                "the closest in-band opponent (10 away) wins over the farther one (90 away)"
            );
            assert_eq!(rating, 1010);
        }
        _ => panic!("p-1000 must match the closest in-band candidate"),
    }
    assert!(
        matches!(
            find_match(&mut object, &find_sha, "p-1090", 2_001),
            MatchResult::Waiting
        ),
        "1090 is now alone in band (1010 was claimed) and waits"
    );
}

#[test]
fn re_enqueue_is_idempotent_and_card_unchanged() {
    let mut object = open(MemoryObjectStorage::default());
    let enqueue_sha = load_script(&mut object, POOL, ENQUEUE_SCRIPT, 1_000);
    enqueue(&mut object, &enqueue_sha, "p-1000", 1000, 1_001);
    assert_eq!(zcard(&mut object, 1_002), 1);

    enqueue(&mut object, &enqueue_sha, "p-1000", 1000, 1_003);
    assert_eq!(
        zcard(&mut object, 1_004),
        1,
        "re-enqueuing an already-queued player does not duplicate"
    );

    enqueue(&mut object, &enqueue_sha, "p-1040", 1040, 1_005);
    assert_eq!(zcard(&mut object, 1_006), 2, "a new player does enqueue");
}

#[test]
fn queue_survives_cold_start_and_matching_is_unchanged() {
    let mut object = open(MemoryObjectStorage::default());
    let enqueue_sha = load_script(&mut object, POOL, ENQUEUE_SCRIPT, 1_000);
    seed_pool(&mut object, &enqueue_sha, 1_001);
    assert_eq!(zcard(&mut object, 1_100), 6);

    let storage = object.into_storage();
    let mut reopened = open(storage);
    assert_eq!(
        zcard(&mut reopened, 2_000),
        6,
        "the whole queue survived the snapshot + reopen"
    );

    let find_sha = load_script(&mut reopened, POOL, FIND_MATCH_SCRIPT, 2_001);
    let before = zcard(&mut reopened, 2_002);
    match find_match(&mut reopened, &find_sha, "p-1000", 2_003) {
        MatchResult::Matched { opponent, rating } => {
            assert_eq!(opponent, "p-1040", "matching is identical after cold start");
            assert_eq!(rating, 1040);
        }
        _ => panic!("p-1000 must still match p-1040 after reopen"),
    }
    assert_eq!(
        zcard(&mut reopened, 2_004),
        before - 2,
        "a post-restore match still drops the card by exactly two"
    );
}

#[test]
fn every_match_keeps_pairs_in_band_and_drops_card_by_two() {
    let mut object = open(MemoryObjectStorage::default());
    let enqueue_sha = load_script(&mut object, POOL, ENQUEUE_SCRIPT, 1_000);
    let find_sha = load_script(&mut object, POOL, FIND_MATCH_SCRIPT, 1_000);
    seed_pool(&mut object, &enqueue_sha, 1_001);

    let mut seen_opponents: Vec<String> = Vec::new();
    let mut now = 2_000;
    for (player, rating) in SEEDS {
        let before = zcard(&mut object, now);
        now += 1;
        match find_match(&mut object, &find_sha, player, now) {
            MatchResult::Matched {
                opponent,
                rating: opp_rating,
            } => {
                assert!(
                    (opp_rating - rating).abs() <= BAND,
                    "{player}->{opponent}: a matched pair is always within band ({} apart)",
                    (opp_rating - rating).abs()
                );
                assert!(
                    !seen_opponents.contains(&opponent),
                    "no player may appear in two matches: {opponent} matched twice"
                );
                assert!(
                    !seen_opponents.contains(&player.to_owned()),
                    "an already-matched player cannot match again: {player}"
                );
                seen_opponents.push(opponent);
                seen_opponents.push(player.to_owned());
                now += 1;
                assert_eq!(
                    zcard(&mut object, now),
                    before - 2,
                    "ZCARD drops by exactly two on a match"
                );
            }
            MatchResult::Waiting => {
                now += 1;
                assert_eq!(
                    zcard(&mut object, now),
                    before,
                    "a waiting player leaves the card unchanged"
                );
            }
            MatchResult::NotQueued => {
                now += 1;
            }
        }
        now += 1;
    }

    assert_eq!(
        seen_opponents.len(),
        4,
        "exactly two pairs (four players) were matched from the six seeds"
    );
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        extracted from dogfood_scenarios.rs (demo scenario test)
//   target_crate:  edgestash-demo
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         skill-based matchmaking over the EdgeObject HTTP route layer;
//                  ENQUEUE / FIND_MATCH Lua with ZRANGEBYSCORE band query and an
//                  atomic dual-ZREM pairing that prevents double-matching.
// ──────────────────────────────────────────────────────────────────────────
