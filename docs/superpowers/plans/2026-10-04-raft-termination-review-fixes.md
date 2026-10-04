# Raft Termination Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Simplify fatal and normal Raft-node termination, guarantee API-future ordering, and remove the temporary cleanup-failure metrics before folding the fatal-store stack into one commit.

**Architecture:** `RaftNodeExecutor` owns accepted immediate tasks and must drain them during lifecycle termination. `RaftNodeImpl` therefore keeps only terminal-state and termination-barrier state; queued API wrappers run, observe the cached terminal state, and settle themselves. Store cleanup remains best-effort and logged, without embedding counters in the Raft log or public reports.

**Tech Stack:** Java, JUnit 4, AssertJ, Gradle, Git worktrees.

---

### Task 1: Define and test accepted-task termination semantics

**Files:**
- Modify: `microraft/src/main/java/io/microraft/executor/RaftNodeExecutor.java`
- Modify: `microraft/src/test/java/io/microraft/impl/RaftStoreFailureTest.java`

- [x] Replace the discard-on-termination fixture with an orderly, controllable executor that calls `DefaultRaftNodeExecutor.onRaftNodeTerminate()`.
- [x] Change the fatal-termination test to require the active operation to fail as indeterminate and an accepted but not-yet-started operation to fail as not running.
- [x] Run the focused test and observe it fail against the current `acceptedApiCalls` implementation.
- [x] Document that normally accepted immediate tasks run exactly once during termination; later submissions may be rejected and delayed work may be cancelled.

### Task 2: Remove the shadow API registry and simplify terminal transitions

**Files:**
- Modify: `microraft/src/main/java/io/microraft/impl/RaftNodeImpl.java`
- Modify: `microraft/src/test/java/io/microraft/impl/RaftStoreFailureTest.java`
- Modify: `microraft/src/test/java/io/microraft/impl/RaftNodeLifecycleAwareTest.java`

- [x] Remove `ApiCallType`, `acceptedApiCalls`, `componentsTerminated`, and pending-call completion from terminal transitions.
- [x] Retain a small termination lock, the cached terminal report, and `terminationCompleted`.
- [x] Recheck node status inside queued start, operation, report, and terminate tasks.
- [x] Catch executor submission rejection and settle every returned future.
- [x] Complete the active API future from `finally` after fatal termination returns.
- [x] Preserve the rule that lifecycle termination completes before fatal-operation callbacks and detached Raft-state callbacks run.
- [x] Run focused lifecycle, report, membership, and fatal-store tests.

### Task 3: Remove cleanup-failure counters and gauges

**Files:**
- Modify: `microraft/src/main/java/io/microraft/impl/log/RaftLog.java`
- Modify: `microraft/src/main/java/io/microraft/impl/handler/InstallSnapshotRequestHandler.java`
- Modify: `microraft/src/main/java/io/microraft/impl/RaftNodeImpl.java`
- Modify: `microraft/src/main/java/io/microraft/report/RaftLogStats.java`
- Modify: `microraft/src/main/java/io/microraft/impl/report/RaftLogStatsImpl.java`
- Modify: `microraft/src/test/java/io/microraft/impl/RaftStoreFailureTest.java`
- Modify: `microraft-metrics/build.gradle.kts`
- Modify: `microraft-metrics/src/main/java/io/microraft/metrics/RaftNodeMetrics.java`
- Delete: `microraft-metrics/src/test/java/io/microraft/metrics/RaftNodeMetricsGaugeTest.java`

- [x] Remove the two cleanup counters from the Raft log, public report API, report implementation, and Micrometer binder.
- [x] Keep prefix-truncation and obsolete-snapshot-deletion failures nonfatal and logged.
- [x] Keep tests proving that both cleanup failures leave the node active and its installed snapshot usable.
- [x] Run the focused core and metrics tests.

### Task 4: Verify, commit, fold, and rebase

- [x] Run formatting/checkstyle and fix new findings.
- [x] Run `:microraft:assemble` and `:microraft-metrics:assemble`.
- [x] Run the full `:microraft:test` and `:microraft-metrics:test` suites with task reruns.
- [ ] Commit the review fixes once with a comprehensive test plan.
- [ ] Fold `master..fix/fatal-raft-store-failures` into one fatal-store commit and verify it independently.
- [ ] Rebase `feature/s3-raft-archive` from old base `d766f1f` onto the folded commit.
- [ ] Re-run lint, build, and the recursive affected tests on the rebased feature branch.

## Execution log

- Baseline: `:microraft:test` and `:microraft-metrics:test` completed successfully before edits.
- The original untracked follow-up note was stashed before history rewriting so the feature worktree remains clean.
- RED: the focused fatal-termination test failed because the registry completed an accepted, not-yet-started operation with `IndeterminateStateException`; the required result is the normal terminal/not-running failure after the executor drains it.
- GREEN: the registry and enum were removed and the focused lifecycle, report, membership, and fatal-store suites passed. Review then found a status-check/submission rejection race and a test callback-ordering race; both are being corrected before metric removal.
- REVIEW: deterministic start and operation submission-race tests failed RED with leaked executor rejection, then passed after lifecycle-aware translation. The lifecycle diff passed spec and code-quality review.
- METRICS: cleanup counters and gauges were removed. Focused core and metrics tests passed, and review confirmed that cleanup remains logged, nonfatal, and behaviorally covered.
- VERIFY: Checkstyle reported only the unchanged repository baseline (core: 25 main / 535 test; metrics: 1 main / 5 test), with no new violation in the held diff. Fresh assembly succeeded, and the complete core and metrics test suites passed with task reruns.
