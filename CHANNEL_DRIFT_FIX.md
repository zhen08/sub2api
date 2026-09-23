# Channel inventory drift fix — ready for parent review, not deployed

Baseline HEAD: `bf390f45d9f73126ce2adc76c1099ff1d3eb55d7`.
Worktree: `/home/zhen/Repo/sub2api-eight-hour-recovery`.

## Finding and bounded change

`inventory()` only accepted the legacy `codex-auto-review -> gpt-5.6-luna`
mapping. The live source channels instead map both `codex-auto-review` and
`gpt-5.5` to `gpt-6-luna`. Channel 1 / group 8 still additionally maps
`gpt-5.6-sol -> gpt-5.6-terra`; channel 2 / group 6 must not contain that rule.

The controller now accepts exactly either complete legacy or complete current
mapping for each source channel. No subset, wildcard, arbitrary target, partial
alias migration, extra alias or extra platform is accepted. All original group,
channel identity, exact group membership, status, restriction, feature, billing,
and pricing guards are retained verbatim. Eight-consecutive-exact-zero recovery,
journal handling, thresholds, state and policy writes are unchanged.

## Evidence

Read both supplied preflight artifacts. They establish current mappings but do
not contain all channel guard fields. Collected a fresh read-only API projection
at `2026-09-23T02:39:56.412292+00:00` through SSH alias `vm`, using controller API
container discovery and `GET /api/v1/admin/channels`. The local collector executes
controller definitions under a non-main module name: it does not enter the
controller CLI, touch state, change channels, or run inference. Only the exact
11 guard fields for both channels are emitted; credentials stay on the VM.
`fixtures/live_channels_20260923.json` contains every field used by the guards,
not unrelated channel metadata. Every non-mapping field matches the old guard.

## TDD and verification

Run from `deploy/hourly-policy`:

- Baseline `python3 -m unittest -q`: **54 passed**.
- RED `python3 -m unittest test_channel_inventory -v`: live fixture rejected with
  `PolicyError: source channel configuration drift` at the existing guard.
- Minimal production change, then the same focused test: **1 passed**.
- Added regression tests for exact legacy acceptance, wrong/random/partial alias
  targets, missing/extra mappings, group-specific sol rule, non-mapping guards,
  and membership drift. Acceptance and rejection tests assert no API writes.
- Final `python3 -m unittest -v`: **61 passed** (2.645 seconds), including all
  original 54 tests and all eight-zero recovery/journal tests.
- `python3 -m compileall -q .`: passed.
- `git diff --check`: passed.
- Reviewed production diff: only channel mapping construction/comparison changed.

## Files and SHA-256

Paths below are relative to `deploy/hourly-policy` unless absolute.

| File | SHA-256 |
|---|---|
| `controller.py` | `35c15c5cd48408a888478366a52e0a103883f36f0662404221f5dbb73f1b5185` |
| `test_channel_inventory.py` | `2b803095704f54efea12252e94944f0406bd71586a5f0a6c5be98b9de71a9f55` |
| `collect_channel_fixture.py` | `b986c13f5b6aaa3e6fb0042e5ea661a403a748151de5dd573581a71e9a7261de` |
| `fixtures/live_channels_20260923.json` | `5208acc6c2da945a18550bbb2bf453a8c0a97d40de2b449ea76d25491b016c3a` |
| `/home/zhen/Repo/aiproxy-hourly-policy/GPT6_LUNA_LIVE_PREFLIGHT.json` | `b3cd8e9c86074db989191688e6787afd1c7ee40de0da1af5f14ad0df0d2e1c64` |
| `/home/zhen/Repo/aiproxy-hourly-policy/build/luna_readonly_evidence.json` | `f2f3122ff7d18e0563f307cf14299c1312175a5e195e7a913085a9f27c4c9eea` |

## Scope / remaining gate

No commit, staging, push, backend edit, remote file write, deployment, channel
mutation, scheduler/cron change, or immutable-release modification performed.
The parent's paused scheduler remains outside this task. No live controller
apply/dry-run was invoked; only read-only channel capture. Parent must review the
local diff and rerun acceptance before deciding on commit or deployment.
