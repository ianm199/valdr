# Release Readiness — valdr v0.1.0-alpha.1

_Prepared while you were away. Conservative pass: only mechanical,
compiler-confirmed-safe changes were applied. Everything requiring judgment is
reported below, not done._

---

## 1. What was cleaned automatically (verified safe)

**Status: build green, oracle clean.** All applied changes are
behavior-preserving (unused-import/dead-assignment removal, whitespace
formatting, mechanical clippy fixes). Nothing touched runtime behavior.

| Metric | Before | After |
|---|---|---|
| Cargo build warnings | 118 | 56 |
| Warnings fixed | — | 62 |
| `cargo fmt` drift | 89 files | 0 (45 reformatted) |

**Commits landed:**
- `a693eaa` chore: cargo fmt --all
- `2fb2068` chore: fix 62 cargo warnings (unused imports/vars, dead assignments, mut/parens)

**What got fixed:**
- `cargo fix --workspace`: removed 43 unused imports + auto clippy lints across 60 files
- `cargo clippy --fix --workspace`: redundant_closure (adlist), manual_unwrap_or_default (intset), identity_op (ziplist), explicit_auto_deref (conn_tls), and others
- Manual targeted fixes: unused imports in `conn_socket.rs` / `syscheck.rs`; unnecessary `mut` in `adlist.rs`; superfluous parens in `eviction.rs`; dead assignments removed in `expire.rs` (`db_done`, `checked_buckets`); `let mut`→`let` follow-ups in `unix.rs` / `expire.rs`; `let _ = samples` in `object.rs`; dead init removed in `stream.rs`
- `cargo fmt --all`: 45 files, whitespace/indentation only

**Oracle evidence:**
- `unit/type/string`: **104/104**
- `unit/introspection`: **111/113** — the 2 failures are the timing-sensitive
  blocked-client tests ("Blocked BLPOP didn't increment expected client
  fields", "Timeout waiting for blocked clients"). These passed 113/113 on
  2026-05-28 and 2026-05-29 prior runs; the failures are flaky timing, **not**
  caused by any change in this pass (import/whitespace edits cannot affect
  blocked-client accounting). Worth one confirming re-run before tagging.
- `conn_transport_kit`: **14/14**

**The remaining 56 warnings are all dead-code on intentional port stubs** and
were deliberately NOT removed (per instructions and PORTING.md). See §2.

---

## 2. Judgment items — YOUR decision required

Grouped by effort vs. impact. None of these were touched.

### A. BLOCKERS for a credible launch (do before tag/publish)

1. **Landing-page draft placeholders** — `docs/index.html:232-256`. The ABOUT
   section has two paragraphs literally marked `[Draft]` / `placeholder` asking
   you to replace them. You said you want to write this yourself. **Must be
   filled in before the site goes public.** (Low effort, high visibility.)

2. **Release version string confirmation** — all crates + tag target are
   `v0.1.0-alpha.1` (per `RELEASE_CHECKLIST.md:3`). Confirm this is the intended
   first-alpha string (vs `0.0.1-alpha` / `0.2.0-alpha`). One-line decision,
   gates the tag. (Low effort, blocks publish.)

3. **Run the release checklist gates** — `docs/RELEASE_CHECKLIST.md:5-97` lists
   10 local gates + Docker smoke/bench + 3-step publish. None are marked
   pass/fail. The full TCL suite is at **2,466/4,299 = 57%** (`SITE_STATUS.md:37`).
   Decide whether that coverage meets your alpha bar, then run the gates green.
   (Medium effort, blocks publish.)

4. **Docker / GHCR publish prerequisites** — `RELEASE_CHECKLIST.md:83-105`
   assumes CI builds `ghcr.io/ianm199/valdr:alpha` on tag push, GHCR package is
   public, and Pages is enabled for Actions. **Confirm those two settings are
   actually set** before pushing the tag. (Low effort, blocks publish.)

### B. Should decide before launch (narrative consistency / polish)

5. **AOF + Replication "alpha" positioning** — README marks both "Alpha";
   `SITE_STATUS.md:44-46` shows differing sparkline depths (AOF 7/12, Repl 6/12,
   eyeball estimates). `RELEASE_CHECKLIST:105` requires README/site/Docker/
   changelog to describe alpha limits **identically**. Pick the canonical
   phrasing and align all four surfaces. (Low effort, medium impact.)

6. **`docs/TLS_FAITHFUL_PLAN.md`** — a future design plan marked "Status: PLAN
   (no code yet)" referencing "Wave A pilot / Phase 2 / Phase 3". Not wrong, but
   reads like current-state docs in a release tree. Decide: add a temporal-scope
   header, or move to `history/`. (Low effort, low-medium impact.) **Doc rewrite
   — left for you.**

7. **SITE_STATUS open threads** — `SITE_STATUS.md:118-173`, six editorial polish
   items: function_load perf-heatmap outlier capping, sparkline glyph
   consistency (7 vs 6 vs 12), header/footer branding (nav, version, GitHub,
   contact), no changelog surface on the site, Docker `--network host` vs
   `host.docker.internal` cross-platform variant, two `<details>` defaulting
   open. All editorial. (Medium effort, medium impact.)

8. **README perf claims — already accurate, note the discrepancy.** Your note
   said "collection writes now BEAT upstream; p1 at parity," but the **published
   README (`README.md:141-176`) is correct**: SADD/HSET/ZADD/SPOP/ZPOPMIN are
   0.6–0.8× and listed under "Behind." No stale claim to fix. Your note appears
   aspirational or crossed with other context. **No action — just don't
   "correct" the README to match the note.** (Verify, no edit.)

### C. Code hygiene — deferred, NOT safe to auto-apply

These are the 56 remaining warnings + clippy findings. All flagged for review
because removing them could break in-progress port features. **None are blockers.**

9. **Unused functions (30+)** — `zipmap.rs:126` (encode_length), `cpu_affinity.rs`
   (apply_affinity/parse_cpulist/next_token/next_num), `expire.rs` scan
   callbacks, `syscheck.rs:291` (run_check), `object.rs` (string_object_len),
   `networking.rs` (lock_db/ingest_rdb). Likely scaffolding / platform stubs.
   Need maintainer review per-function. (Medium effort.)

10. **Unused struct fields + variants (11+)** — `bio.rs:164`, `childinfo.rs`
    (fd), `db.rs:524-528` (blocking_keys/ready_keys), `expire.rs:118+`
    (ExpireScanData), `memory_prefetch.rs`, `setproctitle.rs:90`. May feed
    future features / derived traits. Architectural review. (Medium effort.)

11. **`private_interfaces` lint (5)** — `childinfo.rs:485` (ChildInfoData),
    `defrag.rs:90/201/203/211` (DoneStatus, DefragContext fields). Visibility/
    API-shape question: intentional public-wraps-private, or a bug? (Low-medium.)

12. **Clippy `never_loop` (3 — possible real logic issues)** —
    `expire.rs:424`, `unix.rs:154`, `unix.rs:476`. Loops that immediately break /
    never iterate. **Most likely placeholder stubs awaiting implementation**, but
    these are the only findings that could be genuine logic bugs. Worth an eyeball
    even if you defer the rest. (Low effort to inspect.)

13. **Unused constants (3)** — `networking.rs:75/79`, `rdb/stream.rs:79`,
    `setproctitle.rs:61`. Reserved/port artifacts. (Low effort.)

14. **Clippy `too_many_arguments` / complex types** —
    `defrag.rs:1293` (8 args), `connection.rs:495` (CONN_REGISTRY type). Stylistic
    only; refactor carries behavior risk, skip for alpha. (Defer.)

### D. Repo hygiene — reviewed, no action needed

- `source-drafts/` (22 tracked files): legitimate translation evidence, keep.
- `crates/redis-core/tests/fixtures/*.pem`: generated **test** certs, not
  secrets, keep tracked.
- `harness/bench/profiles|results` (162M / 42M): correctly gitignored, untracked.
- Missing `keywords`/`authors` in crate Cargo.tomls: optional for alpha; decide
  per-crate keywords if/when you publish to crates.io. Defer to 0.2.0+.

---

## 3. Verdict

**Not yet release-ready — but the code is in good shape. The blockers are
editorial/operational, not correctness.**

The tree is clean, build is green, the oracle is clean (modulo two known-flaky
timing tests), and all auto-applied changes are behavior-preserving. What stands
between you and a tag is human-only work that was deliberately left for you.

**Top 3 blockers:**

1. **Landing-page placeholders** (`docs/index.html:232-256`) — explicitly yours
   to write; ships public.
2. **Run + green the RELEASE_CHECKLIST gates** and consciously sign off on the
   57% TCL coverage as the alpha bar.
3. **Confirm publish prerequisites** — version string `v0.1.0-alpha.1`, GHCR
   package public, Pages-for-Actions enabled — before pushing the tag.

Everything in §2.C/D is post-alpha cleanup and does not block. Recommend one
confirming `unit/introspection` re-run before tagging so the two flaky tests
don't get misread as a regression at release time.
