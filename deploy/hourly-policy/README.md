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
- Recovery from `terra` or `luna`: **two consecutive complete hourly windows,
  each with exactly zero OpenAI tokens**, then `full` (all models).
- Any positive usage, even one token, clears the recovery streak.
- Skipped hours restart the streak; processing an hour again cannot advance it.
- Existing recovery-window guards, group-8 bootstrap and severity thresholds are
  unchanged. No bulk policy reset is performed.

### Existing state and interrupted runs

State remains version 1 for compatibility. The legacy names `low` and
`low_windows` are retained, but the count is not trusted: old counts may represent
nonzero usage below the former threshold. Only explicit zero-token evidence for
the immediately preceding hour, with matching complete-window timestamps and
within the recovery guard, can count. Missing, legacy/null, nonzero or mismatched
evidence starts a new one-window streak without lifting restrictions. Existing
`full` users are not reset.

Before any write in a plan, every `full` operation (including journal retries)
must carry two canonical contiguous zero windows in its planned user state,
ending at the planned hour and respecting `recovery_from_hour`. Invalid legacy
recovery intents fail closed with `invalid recovery evidence`; the journal is
left untouched for **manual reconciliation**. This also applies if the remote
write already happened before a lost response. Do not delete the journal or
reset users as an automatic migration. Stale-hour journals remain blocked.
Valid zero-evidence intents can resume without duplicate writes. Transition
evidence retains both hours; new recovery reasons are
`two_consecutive_hours_eq_0`.

## Configuration and installation assumptions

`run_remote.py` requires deployment configuration supplied outside the repository:

- `HOURLY_POLICY_SSH_HOST`: required hostname, IPv4 address or SSH-config alias,
  optionally prefixed with `user@`. No default destination or login is shipped.
  For IPv6, use an SSH-config alias.
- `HOURLY_POLICY_SSH_PORT`: decimal port 1–65535, default `22`.

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
silent. Diagnostics remain visible. State, dry-run output and operational logs
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
