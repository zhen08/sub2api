# Hourly OpenAI policy controller

Standalone Python 3 controller and SSH/cron wrappers. This directory is source
only: adding it to the repository does **not** install a cron job, deploy the
controller, alter running users, or build an application image.

## Policy

Usage is aggregated per user across OpenAI keys/groups, including input, output,
cache-creation and cache-read tokens. Each run uses the immediately preceding
**complete UTC hourly window** `[hour_start, hour_end)`, pinned across retries.
Unknown groups, incomplete pagination, invalid tokens and policy drift fail closed.
An absent user aggregate is zero only after the complete usage snapshot passes
validation.

- More than 20,000,000 tokens: `terra` (an existing `luna` stays `luna`).
- More than 50,000,000 tokens: `luna`.
- Recovery from `terra` or `luna`: **eight consecutive complete hourly windows,
  each with exactly zero OpenAI tokens**, then `full` (all models).
- Any positive usage, even one token, clears the recovery streak.
- Skipped hours restart the streak; processing an hour again cannot advance it.
- Existing recovery-window guards, group-8 bootstrap and severity thresholds are
  unchanged. No bulk policy reset is performed.

### Temporary suspension of automatic Luna

Optional remote `<state-dir>/config.json` contains exactly
`{"luna_enabled": false}` to suspend automatic Luna indefinitely, until an
operator explicitly restores `true` (no expiry). Missing config preserves the
original enabled behavior; invalid config fails closed. The >20M Terra cap stays
in force even above 50M. Managed users whose live policy matches stored Luna are
relaxed to Terra, including a rerun of the already-processed hour; this preserves
zero-window evidence and does not grant Full. Normal eight-zero-hour recovery is
unchanged. Unmanaged/manual policy drift still aborts without writes. A pending
Luna escalation is preserved and blocks execution for operator reconciliation;
valid Terra relaxation journals retain normal readback and retry semantics.

Install only after independent review and verification of the active API target
and state directory. Do not overwrite an immutable old controller release or
point the default Docker discovery at an inactive gateway. Update configuration
atomically while holding the controller's state-directory lock.

### Model targets and channel migration

The persisted/API policy name `terra` caps higher-ranked text models to
`gpt-6.1-terra`. Astra, legacy 5.6 Sol/Terra, 6.1 Sol and 6 Sol are above
this cap; 5.6 Luna and 6 Luna remain below it. Explicit lower-tier requests
are not upgraded. `luna` still caps to `gpt-6-luna`, including requests for
6.1 Terra; `original` and `full` retain their existing semantics.
Unknown models and dedicated image/audio billing are unchanged.
Restricted text charges use the actual dispatched model and exact same-model
pricing (including known spelling/effort/date/compact aliases), never another
Sol generation, tier, wildcard, or generic fallback when its price is absent.

For each source channel, inventory accepts only the complete three-entry OpenAI
mapping: `codex-auto-review` and `gpt-5.5` → `gpt-6-luna`, plus `gpt-5.6-sol` →
`gpt-6.1-sol` (new) or `gpt-6-sol` (current live compatibility). Each channel may
migrate independently. All membership, status, billing and feature/pricing guards
remain enforced. The dated live fixture is historical evidence and is not rewritten.
This source change does not modify live channel mappings or deploy anything;
production migration requires separate authorization and exact-price provisioning.

### Existing state and interrupted runs

State remains version 1 for compatibility. The legacy names `low` and
`low_windows` are retained, but the count is not trusted: old counts may represent
nonzero usage below the former threshold. Only the verified contiguous suffix of
explicit canonical zero-token evidence ending at the immediately preceding hour
can count. Scan backward through at most seven prior windows, stopping at the
first missing, nonzero, malformed, noncontiguous or pre-guard window. Hours and
token totals must be integers (not booleans or floats), with exact UTC complete-
window timestamps and no extra fields. Append the current complete zero window;
never synthesize missing history. A missing/invalid final prior window or a gap
starts a new one-window streak. A valid suffix after an invalid older window can
still count. `low` is derived from evidence, never trusted as input. Normally the
old two-hour controller retained at most one zero window for restricted users:
that one may carry forward, but it does not imply any earlier zero hours.
Existing `full` users are not reset by this migration; normal high-usage rules
still apply. State is migrated lazily while processing each new hour, with no
bulk rewrite or historical re-query.

Before any write in a plan, every `full` operation (including journal retries)
must carry exactly eight canonical contiguous zero windows in its planned user state,
ending at the planned hour and respecting `recovery_from_hour`. Invalid legacy
recovery intents fail closed with `invalid recovery evidence`; the journal is
left untouched for **manual reconciliation**. This also applies if the remote
write already happened before a lost response. Do not delete the journal or
reset users as an automatic migration. Stale-hour journals remain blocked.
Valid eight-window zero-evidence intents can resume without duplicate writes.
Old two-window `full` intents remain blocked for operator reconciliation, even
if the remote policy is already `full`; do not expand them using assumed hours.
Recovery validation precedes any journal save and any policy mutation in the
entire write set. Transition evidence retains all eight hours; new recovery
reasons are `eight_consecutive_hours_eq_0`.

## Configuration and installation assumptions

`run_remote.py` requires deployment configuration supplied outside the repository:

- `HOURLY_POLICY_SSH_HOST`: required hostname, IPv4 address or SSH-config alias,
  optionally prefixed with `user@`. No default destination or login is shipped.
  For IPv6, use an SSH-config alias.
- `HOURLY_POLICY_SSH_PORT`: decimal port 1–65535, default `22`.
- `HOURLY_POLICY_LOCAL_STATE_DIR`: optional **existing absolute local directory**
  for sanitized retry evidence, e.g. `/home/zhen/Repo/aiproxy-hourly-policy/state`.
  Provision it separately with owner-only permissions (`0700`), owned by the
  runner. It is not the remote controller state directory. No directory is
  automatically created, and there is no default or home-directory fallback.

For example, `operator@policy.example.test` is a documentation placeholder, not
an installed destination. Missing/invalid configuration fails before SSH starts,
without retries or echoing configuration values. SSH uses batch authentication;
credentials remain outside this directory. Default wrapper mode is dry-run;
`run_cron.py` explicitly requests apply mode and must only be installed after a
separate operational review/approval.

Remote prerequisites are Python 3, noninteractive sudo, Docker, and a `sub2api`
container exposing the per-user `/openai-model-policy` admin API. Credentials
and database connection configuration are discovered inside that container.
Loopback and the conventional Docker bridge gateway are the only accepted local
API endpoints. The default remote state directory is
`/var/lib/aiproxy-hourly-policy`; it must be retained across upgrades.

The inventory assertions are installation-specific: group 6 (`openai-default`),
group 8 (`openai-max-terra`), channels 2/1, and their explicit model mappings in
`inventory()` must match the installation. They deliberately reject drift rather
than silently editing groups/channels/keys. Review these assumptions before any
installation; the controller is not a universal auto-configurator.

Successful cron transitions emit only `email`, `from`, `to`; no-change runs are
silent, including successful retries. Failed-attempt diagnostics are buffered:
success emits only the successful attempt's stdout; three failed attempts emit
all sanitized attempt diagnostics followed by `retries_exhausted`. Attempts are
scheduled at monotonic offsets **0, 300, 630 seconds** from the first attempt's
budget start, not relative delays after failures. This spreads fast connection
failures across ten and a half minutes while preserving up to 269 seconds of
process time plus a one-second cleanup reserve per attempt within the existing
900-second total budget. Sleeps and attempt timeouts are clipped/refused at the
pinned hour boundary; an overslept deadline cannot launch another attempt.
Only SSH exit 255 with a transient static classification (`timeout`, `refused`,
`network_unreachable`, `connection_closed`) is retried. Unknown/auth/host-key
failures, remote application errors, unexpected successful stderr, local process
errors, output-limit failures and local watchdog expiry terminate with
`non_retryable_failure`. A local watchdog expiry is not proof of an SSH transport
failure. Pending journals remain untouched for reconciliation. Permanent security
classifications take precedence over transient words in the captured prefix.
Hour/deadline guard exits retain prior diagnostics followed by the guard error.
The pinned hour, remote idempotency, byte bounds and process-group cleanup remain
unchanged. A 15-minute retry budget starting at :05 cannot outwait a sustained
outage through :22; this is dispersed retry, not a guarantee against that outage.
Before installation, verify the scheduler's **script** timeout exceeds 900 seconds
plus startup/diagnostic overhead (agent inactivity timeouts are a different limit).

SSH diagnostics expose only static `ssh_error` codes: `timeout`, `refused`,
`network_unreachable`, `auth_failed`, `host_key`, `connection_closed`, `unknown`.
Classification examines at most the first 4096 stderr bytes in memory, across
read chunks, while the existing combined output limit remains enforced. Raw
stderr is neither returned nor printed nor persisted. Classification is a
best-effort English-message heuristic, not proof of an underlying network cause;
unrecognized, localized or prefix-truncated messages classify as `unknown`.

When local state is configured, `last-retry-failure.json` retains the most recent
failure-bearing run (recovered or terminal), at most three sanitized attempt
records and 4096 bytes. It includes the pinned hour and outcome, not SSH host,
credentials, remote stdout, stderr or transition emails. Clean first-attempt
success leaves it intact. Replacement uses a private `0600` file and a directory
descriptor; final-component directory symlinks are rejected and target-file
symlinks are replaced, not followed. Root and the home directory itself are
rejected. Use a trusted local filesystem and trusted parent directories.

This record is optional, best-effort, and not a durable audit journal: missing,
unsafe or unwritable directories do not change policy execution or notification
output. One exclusive `.last-retry-failure.tmp` slot bounds temporary disk use;
an interrupted writer can leave it behind. In that case recording is skipped
until an operator verifies no writer is active and removes the stale slot.
No automatic cleanup of another writer's slot is performed.

State, dry-run output and operational logs
can contain personal data and must never be committed.

## Local verification (no deployment)

From the repository root:

```sh
cd deploy/hourly-policy
python3 -m unittest -v
python3 -m compileall -q .
```

Run tests from this directory because two retained baseline tests use relative
source paths. Tests use temporary state, fake APIs, local loopback HTTP fixtures
and bounded local subprocesses; no real deployment SSH is performed.

See [TESTING.md](TESTING.md) for the recorded RED/GREEN checks.

### Temporary Terra suspension

The state-directory config also accepts `{"luna_enabled": true, "terra_enabled": false}`.
Missing `terra_enabled` defaults to true. With this setting, >20M does not escalate;
>50M still escalates to Luna. Matching controller-managed Terra users relax to Full
unless their latest complete hour exceeds 50M (then Luna), including same-hour reruns.
Luna retains the eight-complete-zero-hour recovery rule. Manual policy drift and
pending Terra escalations fail closed. Do not disable both flags together; that
combination is not supported. Restore Terra only on explicit user instruction by
setting `terra_enabled` to true; do not restore old state snapshots.

### Luna recovery duration

`luna_recovery_hours` accepts integer 8 (backwards-compatible default) or 12.
The active policy uses `{"luna_enabled": true, "terra_enabled": false, "luna_recovery_hours": 12}`.
Luna requires 12 consecutive complete zero-token hours across all OpenAI keys,
including cache tokens. Valid prior windows are retained; positive usage or missing
hours breaks the streak. Pending recovery intents with only eight windows are rejected
under the 12-hour setting. Terra recovery, if enabled later, remains eight hours.

### Restoring the 20M Terra tier

After the gateway carrying the Terra 6.1 cap has been deployed and verified,
atomically set the remote state-directory config under the controller lock to
`{"luna_enabled": true, "terra_enabled": true, "luna_recovery_hours": 12}`.
This is an activation target, not a claim about live configuration. Do not enable
it against the previous gateway, whose `terra` label still caps to Sol 6.1.
The controller code and thresholds need no change: strictly >20M selects Terra,
strictly >50M selects Luna, and all OpenAI keys/cache tokens are included.
Terra retains eight zero-use hours; Luna retains twelve. Preserve state and
pending journals; do not backfill processed hours or reset managed policies.
Verify dry-run, apply, post-apply dry-run and scheduler state separately.
