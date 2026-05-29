# Site status — handoff note

State as of **2026-05-29** after the Option-D iteration session. Picks up
where the live site is, what got changed, the local dev loop, and the
threads I noticed in passing.

---

## Where the site lives

| | |
|---|---|
| **Live** | https://flightdecksystems.github.io/valdr/ |
| **Local preview** | http://127.0.0.1:8765/ (running from `docs/` via `python3 -m http.server 8765`) |
| **Source files** | `docs/index.html`, `docs/style.css`, `docs/.nojekyll` |
| **Deploy** | `.github/workflows/static.yml` — uploads `docs/` as the Pages artifact on every push that touches `docs/**`. ~30 s. |
| **Recently fixed** | Pages workflow was silently failing for 24 h after the 2026-05-28 deletion of repo-root `index.html` / `performance.html` / `convergence.html`. Fixed in commit `c5139f0`. |

Linked docs co-located in `docs/`:
`MLUA_EXIT_PLAN.md`, `TEST_AND_FEATURE_COVERAGE.md`, `STRUCTURE_AUDIT.md`,
`TLS_FAITHFUL_PLAN.md`, `ADR_001_LUA_RUNTIME.md`, `DOCKER.md`,
`RELEASE_CHECKLIST.md`, `TCL_TEST_SUITE_RUNBOOK.md`,
`REPLICATION_WAIT_ARCHITECTURE_20260526.md` (deleted in the cleanup, do
not re-link).

---

## Page structure, top → bottom

1. **Header** — `<h1>Valdr</h1>` + tagline `Valkey, in safe Rust. Alpha.`
2. **Lead** — 3 sentences: safe-Rust data engine/RESP/TLS, no `fork()`,
   no C module ABI, mlua exception with a link to `MLUA_EXIT_PLAN.md`.
3. **Coverage** — `<pre><code>` block with three sparkline rows:
   ```
   Counted assertions          ██████████  3,015 / 3,015
   Single-node-core blocks     █████████░  2,466 / 2,541   · 97%
   Full upstream Valkey suite  ██████░░░░  2,466 / 4,299   · 57%
   ```
   followed by a `<p class="note">` provenance line + commentary slot
   comment.
4. **Scope** — `<pre><code>` block with 14 sparkline rows grouped by blank
   lines into three tiers:
   - **9 full rows** (12-block bar): data types, streams, pub/sub, MULTI,
     scripting+mlua, ACL+AUTH, expiration+eviction, RDB, TLS
   - **2 alpha rows** (7-of-12 / 6-of-12 fill): AOF, replication
   - **3 not-implemented rows** (empty bar): cluster mode, sentinel HA,
     C module ABI
5. **Performance**:
   - **Headline** — `<pre>` with 3 GET-pipeline-depth rows (p=1/16/100,
     dual ratios `0.95× / 0.92×`-style) + default-suite median line
   - **`<p class="note">`** identifying the adversaries + host
   - **`<details>`** wrapping two real `<table class="perf-table">`s
     (one per adversary) with proper `<caption>`, `<thead>`, group-head
     subsection rows, and `<td class="num ratio win|behind">` heatmap
     classes on the ratio cell
6. **Try it** — Docker pull/run/PING + a `valkey-benchmark` invocation
7. **Footer** — one line: `Valdr · alpha · BSD 3-Clause · github.com/flightdeckSystems/valdr`

---

## What changed this session

| Change | Where | Notes |
|---|---|---|
| Replaced bar-wall Coverage with sparkline rows | `docs/index.html` | "Option D" framing — less loud, more confident-by-restraint |
| Added Scope section | `docs/index.html` | Was missing — what's implemented vs alpha vs not |
| Added benchmark command in Try it | `docs/index.html` | One-liner that pairs with the pulled image |
| Performance: `<pre>` → two real `<table>`s | `docs/index.html` + `docs/style.css` | Heatmap on ratio column: pale eggplant for wins, pale rust for behind, parity uncolored |
| Fixed wrong 8.1.7 ratios (had been extrapolated) | `docs/index.html` + `README.md` | E.g. GET p=100 vs 8.1.7 was 1.474×, actually 1.322× |
| Filled in MGET vs 8.1.7 | `docs/index.html` + `README.md` | Required the bench-client swap — 8.1.7's `valkey-benchmark` doesn't know `-t mget` |
| Added `.muted` utility class | `docs/style.css` | Was a stray classname; now defined |
| Fixed Pages workflow | `.github/workflows/static.yml` | Was copying deleted files; now uploads `docs/` directly |

---

## Heatmap classification

In `docs/style.css`:

```css
.perf-table td.ratio.win    { background: rgba(76, 29, 149, 0.07); }   /* ≥ 1.20× */
.perf-table td.ratio.behind { background: rgba(168, 79, 49, 0.09); }   /* ≤ 0.85× */
/* parity (0.85×–1.20×) gets no class — uncolored                       */
```

To re-tag a row, change the `<td class="num ratio">` on that row in
`docs/index.html` to `<td class="num ratio win">` or
`<td class="num ratio behind">`. Both tables (8.1.7 and 9.1.0) are
classified consistently against the same thresholds.

---

## Local dev loop

```bash
# Already running (background):
#   python3 -m http.server 8765 inside docs/
#   pid 26677  → http://127.0.0.1:8765/

# Iterate:
#   edit docs/index.html or docs/style.css → Cmd-R browser

# Stop the local server when done:
pkill -f 'http.server 8765'
```

Push live:

```bash
# gh auth currently active as ianm199 (has write to flightdeckSystems/valdr)
git push origin main
```

Pages auto-deploys on `docs/**` changes (~30 s).

---

## Open threads worth a second look

These are things I noticed in passing but didn't act on. Worth a glance if
you're deciding what "where I want it" actually means.

1. **`function_load` ratio dominates** the perf heatmap at `9.815× / 10.394×`.
   It's a real measurement (FUNCTION LOAD is dramatically faster in Valdr
   for some reason — likely a different code-path optimization) but the
   long bar visually overpowers everything else. Options: cap the
   displayed value at `>3×` with a footnote, suppress the win-tint on
   outliers, or just leave it as honest data.

2. **Scope sparkline glyphs** are slightly inconsistent: full rows show
   `████████████` (12 chars), alpha-AOF shows `███████░░░░░` (7-of-12),
   alpha-Replication shows `██████░░░░░░` (6-of-12). The 7 vs 6 wasn't a
   careful choice — both are eyeball estimates. Could be 9-of-12 or
   8-of-12 for AOF if "alpha but mostly there" is the message you want.

3. **Three commentary slot comments** in the HTML are awaiting your prose:
   ```
   <!-- TODO(commentary): coverage section commentary slot -->
   <!-- TODO(commentary): scope section commentary slot -->
   <!-- TODO(commentary): performance section commentary slot -->
   ```
   These are placeholders where you said you'd add your own framing.
   They're after the `<p class="note">` of each section.

4. **Header is one wordmark + tagline** — no nav, no link to GitHub, no
   versioning info. Pretty restrained. If you want a top-right link to
   the repo or a "release date" stamp, easy add.

5. **Footer is one line** — no contact, no link list, no "questions?". If
   the site is launch material, a `mailto:` or Mastodon/Bsky link goes here.

6. **No changelog / release notes section** on the site. The repo has
   `CHANGELOG.md` but it's not surfaced. If you want a "what changed"
   section under the fold, the markdown is ready to be pulled in.

7. **Try it Docker command uses `--network host`** which doesn't work on
   Docker Desktop for Mac/Windows. For a cross-platform version, swap to:
   ```
   docker run --rm valkey/valkey:8-alpine \
     valkey-benchmark -h host.docker.internal -p 6379 -n 100000 -c 50 -P 100 -t get,set
   ```
   The `host` version is cleaner; the `host.docker.internal` version
   works for more users. Pick by audience.

8. **MGET parse-err `<span class="muted">`** got removed when we filled
   in the real number, but the `.muted` class stays defined in CSS for
   future inline-note use (e.g. caveats, "n/a" markers).

9. **The two `<details>` summaries** read "Full per-row data (9 pipeline
   depths + 23 commands)" — that's the only summary text. You can change
   it to anything; if you want the section to default-open, add
   `<details open>`.

---

## Recent commits (most recent first)

```
c5139f0  ci: fix Pages workflow — serve docs/ directly, not the deleted root files
7bb4b76  bench: one canonical entry point, narrow runs via env vars
d79edc8  docs(README): fix perf numbers — 8.1.7 ratios were extrapolated, not measured
e400c93  docs(site): replace loud bar wall with Option D — sparkline rows
9666290  chore: brand rename — valkey-rs → valdr across all operational files
97a5abe  release: pin Valkey 9.1.0, drop ianm199 namespace from site
86e9009  docs: nuke stale planning docs + old-brand GH-pages
```

All on `main`, all pushed.
