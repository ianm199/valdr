# Sentinel Inventory

**Status:** H1 inventory completed on 2026-06-13.

Valdr does not implement Sentinel mode yet. This note inventories the upstream
test and command surface so later work can start from scoped lanes instead of a
single undifferentiated `sentinel_later` bucket.

## Product Boundary

- No Sentinel runtime, process mode, monitor loop, quorum state, config rewrite,
  notification, or failover orchestration is claimed.
- Parser stubs should wait until R5 server-side failover has a working gate, or
  until a read-only discovery subset can return faithful data and errors.
- Deprecated naming aliases should be handled deliberately: `master`/`primary`,
  `masters`/`primaries`, `slaves`/`replicas`, and
  `get-master-addr-by-name`/`get-primary-addr-by-name`.

## Tcl Buckets

Source: `reference/valkey/tests/sentinel`.

| Bucket | Files | Source tests | Frontier |
|---|---:|---:|---|
| Discovery and read-only introspection | 2 | 23 | Base `SENTINEL` discovery commands plus `INFO` surface. |
| Deprecated command aliases | 1 | 4 | Compatibility for old Sentinel command names. |
| Config and rewrite | 4 | 20 | `sentinel.conf` rewrite, runtime reconfiguration, hostname config, `CONFIG SET/GET`. |
| Replica topology reconfiguration | 2 | 10 | Replica re-pointing and primary reboot behavior. |
| Quorum and down detection | 4 | 12 | `CKQUORUM`, subjective/objective down, port 0, stuck failover. |
| Manual failover and selection | 4 | 19 | Manual failover, coordinated failover, replica priority, selection placeholder. |
| Auth, ACL, and debug | 2 | 4 | ACL/auth coverage and debug command behavior. |
| Harness/includes | 5 | 8 | Sentinel runner/init helper files, not standalone product files. |
| **Total** | **24** | **100** | Matches `sentinel_later` in `TEST_AND_FEATURE_COVERAGE.md`. |

File mapping:

| File | Bucket | Source tests |
|---|---|---:|
| `run.tcl` | Harness/includes | 0 |
| `tests/00-base.tcl` | Discovery and read-only introspection | 20 |
| `tests/01-conf-update.tcl` | Config and rewrite | 4 |
| `tests/02-replicas-reconf.tcl` | Replica topology reconfiguration | 5 |
| `tests/03-runtime-reconf.tcl` | Config and rewrite | 5 |
| `tests/04-slave-selection.tcl` | Manual failover and selection | 0 |
| `tests/05-manual.tcl` | Manual failover and selection | 5 |
| `tests/06-ckquorum.tcl` | Quorum and down detection | 3 |
| `tests/07-down-conditions.tcl` | Quorum and down detection | 6 |
| `tests/08-hostname-conf.tcl` | Config and rewrite | 3 |
| `tests/09-acl-support.tcl` | Auth, ACL, and debug | 3 |
| `tests/10-replica-priority.tcl` | Manual failover and selection | 3 |
| `tests/11-port-0.tcl` | Quorum and down detection | 1 |
| `tests/12-primary-reboot.tcl` | Replica topology reconfiguration | 5 |
| `tests/13-info-command.tcl` | Discovery and read-only introspection | 3 |
| `tests/14-debug-command.tcl` | Auth, ACL, and debug | 1 |
| `tests/15-sentinel-deprecated-commands.tcl` | Deprecated command aliases | 4 |
| `tests/16-config-set-config-get.tcl` | Config and rewrite | 8 |
| `tests/17-manual-coordinated.tcl` | Manual failover and selection | 11 |
| `tests/18-stuck-failover.tcl` | Quorum and down detection | 2 |
| `tests/helpers/check_leaked_fds.tcl` | Harness/includes | 0 |
| `tests/includes/init-tests.tcl` | Harness/includes | 7 |
| `tests/includes/start-init-tests.tcl` | Harness/includes | 1 |
| `tests/includes/utils.tcl` | Harness/includes | 0 |

## Command Surface

Source: `reference/valkey/src/commands/sentinel*.json`.

Discovery/client lookup:

- `SENTINEL get-master-addr-by-name`
- `SENTINEL get-primary-addr-by-name`
- `SENTINEL master`
- `SENTINEL primary`
- `SENTINEL masters`
- `SENTINEL primaries`
- `SENTINEL replicas`
- `SENTINEL slaves`
- `SENTINEL sentinels`
- `SENTINEL myid`
- `SENTINEL info-cache`

Configuration and monitoring:

- `SENTINEL monitor`
- `SENTINEL set`
- `SENTINEL config`
- `SENTINEL flushconfig`
- `SENTINEL reset`
- `SENTINEL remove`

Quorum, failover, and scripts:

- `SENTINEL ckquorum`
- `SENTINEL failover`
- `SENTINEL is-master-down-by-addr`
- `SENTINEL is-primary-down-by-addr`
- `SENTINEL simulate-failure`
- `SENTINEL pending-scripts`

Diagnostics:

- `SENTINEL debug`
- `SENTINEL help`

## Suggested Implementation Order

1. Read-only parser and faithful errors for the discovery subset, gated without
   claiming failover.
2. Sentinel config model and `monitor`/`set`/`config`/`flushconfig` only after
   a Sentinel runtime owner exists.
3. Quorum/down-state probes after replication link observability and R5
   failover primitives are green.
4. Manual failover and coordinated failover last, because they depend on data
   convergence, promotion, demotion, and client discovery correctness.

## Inventory Commands

```bash
find reference/valkey/tests/sentinel -name '*.tcl' -print | sort
find reference/valkey/src/commands -maxdepth 1 -name 'sentinel*.json' -print | sort
```

Per-file source-test counts were collected with:

```bash
for f in $(find reference/valkey/tests/sentinel -name '*.tcl' -print | sort); do
  n=$(rg -c '^\\s*test\\s+(\\{|\\\")' "$f" || true)
  printf '%s %s\\n' "${f#reference/valkey/tests/sentinel/}" "${n:-0}"
done
```
