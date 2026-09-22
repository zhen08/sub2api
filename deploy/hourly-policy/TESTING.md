# Verification record

All commands were run with the working directory `deploy/hourly-policy`.
Only local tests ran; no deployment, production API calls, state changes or
schedule changes were performed.

## Baseline

`python3 -m unittest -q`: **34 tests passed** before changing the copied sources.
An initial repository-root discovery run exposed two pre-existing relative-path
assumptions; using the documented test directory resolved both without changes
to those assertions.

## Observed RED → GREEN slices

1. `test_zero_recovery.ZeroRecoveryTests.test_one_token_resets_zero_streak`
   - RED: one token incorrectly restored both `terra` and `luna` to `full`.
   - Changed the recovery predicate to exactly zero.
   - GREEN: 1 test passed (both levels).
2. `test_legacy_counter_requires_contiguous_positive_zero_evidence` and
   `test_proven_zero_counts_without_trusting_legacy_counter`
   - RED: seven invalid legacy-evidence cases incorrectly restored `full`,
     while valid zero evidence with an untrusted zero counter failed to recover.
   - Replaced count-based carryover and synthesized legacy evidence with
     contiguous canonical zero-window evidence and the recovery-hour guard.
   - GREEN: 4 focused tests passed, including the retained repeat/gap test.
3. `test_pending_recovery_requires_two_zero_windows_before_any_write` and
   `test_invalid_recovery_rejects_entire_write_set`
   - RED: eight unsafe recovery journal scenarios were accepted; the mixed
     write-set test reached policy reads instead of rejecting bad evidence.
   - Added whole-plan recovery-evidence validation before reads/writes or
     journal replacement, including already-applied journal retries.
   - GREEN: all 5 zero-recovery tests at this point passed.
4. Updated retained `test_fixes.FixTests.test_bootstrap_and_two_zero_windows_evidence`
   to the new recovery requirement.
   - RED: transition still reported the former `<10M` reason.
   - Updated reason to `two_consecutive_hours_eq_0`.
   - GREEN: full 39-test suite passed.
5. `python3 -m unittest test_remote_config -v`
   - RED: destination ignored configuration; missing and invalid configuration
     was not rejected. The missing-host test was tightened to assert if a
     process starts, then rerun and observed failing on that assertion.
   - Replaced the installation-specific SSH endpoint with required host and
     validated port environment configuration; missing/invalid configuration
     exits before process creation with a sanitized error.
   - GREEN: all 3 configuration tests passed.
   - Retained wrapper tests now supply a reserved example hostname fixture.

Additional regression coverage exercises valid zero-evidence journal retries
both before and after a lost write response, positive-token ranges, repeat/gap
handling, and migration preserving restricted/already-full users. These required
no further production changes.

## Final local checks

- `python3 -m unittest -v`: **45 tests passed**.
- `python3 -m compileall -q .`: passed.
- Existing compact cron alert tests remain unchanged and pass.
- No backend/frontend application code or workflows were modified.

## Branch-push workflow review

The checked-in workflows were inspected:

- `backend-ci.yml`: branch pushes run tests and lint on GitHub-hosted runners.
- `security-scan.yml`: branch pushes run dependency/security checks.
- `release.yml`: triggered only by `v*` tags or explicit workflow dispatch,
  not an ordinary feature-branch push.
- `cla.yml`: pull-request/comment events, with upstream-repository guards;
  not triggered by an ordinary branch push.

No checked-in feature-branch push deployment job was found. This is a review of
the repository workflow files, not a claim about external webhooks or deployment
systems outside the repository. No push or workflow dispatch was performed.
