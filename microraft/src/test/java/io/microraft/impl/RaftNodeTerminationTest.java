/*
 * Copyright (c) 2026, MicroRaft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.microraft.impl;

import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.RaftNodeStatus.ACTIVE;
import static io.microraft.RaftNodeStatus.TERMINATED;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEADER;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.report.RaftNodeReport.RaftNodeReportReason.FATAL_FAILURE;
import static io.microraft.report.RaftNodeReport.RaftNodeReportReason.STATUS_CHANGE;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.readRaftState;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.NotLeaderException;
import io.microraft.exception.RaftNodeFatalException;
import io.microraft.executor.impl.DefaultRaftNodeExecutor;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.LocalTransport;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.persistence.RaftStore;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReportListener;
import io.microraft.test.util.BaseTest;

public class RaftNodeTerminationTest extends BaseTest {

    private static final int ASSERT_TIMEOUT_SECONDS = 10;
    private static final int PENDING_COMMIT_INDEX_OFFSET = 10;
    private static final int QUERY_TIMEOUT_MINUTES = 5;

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void whenFatalFailureTerminatesComponentsBeforeCompletingActiveCallAndDrainsQueuedCalls()
            throws Exception {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        ControllableRaftNodeExecutor executor = new ControllableRaftNodeExecutor();
        executor.blockTermination();
        BlockingFailingRaftStore store = new BlockingFailingRaftStore();
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                .setInitialGroupMembers(List.of(endpoint)).setExecutor(executor)
                .setTransport(new LocalTransport(endpoint)).setStateMachine(new SimpleStateMachine()).setStore(store)
                .build();
        try {
            node.start().join();
            eventually(() -> assertThat(node.state().role()).isEqualTo(LEADER), ASSERT_TIMEOUT_SECONDS);

            store.blockAndFailPersistLogEntries();
            CompletableFuture<Ordered<Object>> failingFuture = node.replicate(applyValue("first"));
            assertThat(store.awaitPersistLogEntries()).isTrue();
            CompletableFuture<Ordered<Object>> queuedFuture = node.replicate(applyValue("second"));
            CompletableFuture<Ordered<RaftNodeReport>> reportFuture = node.getReport();
            CompletableFuture<Ordered<Object>> terminationFuture = node.terminate();
            CountDownLatch queueDrained = new CountDownLatch(1);
            executor.execute(queueDrained::countDown);
            AtomicBoolean componentsTerminatedBeforeActiveCall = new AtomicBoolean();
            CountDownLatch activeCallCompleted = new CountDownLatch(1);
            failingFuture.whenComplete((result, failure) -> {
                componentsTerminatedBeforeActiveCall.set(executor.isTerminated() && store.isTerminated());
                activeCallCompleted.countDown();
            });

            store.releasePersistLogEntries();

            assertThat(store.awaitFailure()).isTrue();
            assertThat(executor.awaitTerminationStarted()).isTrue();
            assertThat(failingFuture).isNotDone();
            assertThat(queuedFuture).isNotDone();
            assertThat(reportFuture).isNotDone();
            assertThat(terminationFuture).isNotDone();

            executor.releaseTermination();

            assertThat(executor.awaitTermination()).isTrue();
            assertThat(store.awaitTermination()).isTrue();
            assertThatThrownBy(() -> failingFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                    .hasCauseInstanceOf(IndeterminateStateException.class);
            assertThat(activeCallCompleted.await(ASSERT_TIMEOUT_SECONDS, SECONDS)).isTrue();
            assertThat(componentsTerminatedBeforeActiveCall).isTrue();
            assertThat(queueDrained.await(ASSERT_TIMEOUT_SECONDS, SECONDS)).isTrue();
            assertThatThrownBy(() -> queuedFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(reportFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult().getReason())
                    .isEqualTo(FATAL_FAILURE);
            assertThat(terminationFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS).getCommitIndex())
                    .isEqualTo(node.state().commitIndex());
        } finally {
            store.releasePersistLogEntries();
            executor.releaseTermination();
            node.terminate().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }
    }

    @Test(timeout = 300_000)
    public void whenFatalFailureOccursThenAllPendingRaftOperationsFail() throws Exception {
        AtomicReference<BlockingFailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            BlockingFailingRaftStore store = new BlockingFailingRaftStore();
            storeRef.set(store);
            return store;
        }).start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        node.replicate(applyValue("seed")).get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        long commitIndex = node.state().commitIndex();

        CompletableFuture<Ordered<Object>> scheduledQuery = node.query(queryLastValue(), EVENTUAL_CONSISTENCY,
                Optional.of(commitIndex + PENDING_COMMIT_INDEX_OFFSET),
                Optional.of(Duration.ofMinutes(QUERY_TIMEOUT_MINUTES)));
        OrderedFuture<Object> linearizableQuery = new OrderedFuture<>();
        OrderedFuture<Object> leadershipTransfer = new OrderedFuture<>();
        readRaftState(node, () -> {
            node.state().leaderState().queryState().addQuery(commitIndex, "query", linearizableQuery);
            assertThat(node.state().initLeadershipTransfer(node.getLocalEndpoint(), leadershipTransfer)).isTrue();
            return null;
        });

        BlockingFailingRaftStore store = storeRef.get();
        store.failFlush();
        CompletableFuture<Ordered<RaftNodeReport>> snapshot = node.takeSnapshot();

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        List.of(snapshot, scheduledQuery, linearizableQuery, leadershipTransfer).forEach(future ->
            assertThatThrownBy(() -> future.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                    .hasCauseInstanceOf(IndeterminateStateException.class));
    }

    @Test(timeout = 300_000)
    public void whenNormalTerminationDrainsQueuedTasksThenAcceptedApiFuturesComplete() throws Exception {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        ControllableRaftNodeExecutor executor = new ControllableRaftNodeExecutor();
        executor.blockTermination();
        ReentrantTerminationStateMachine stateMachine = new ReentrantTerminationStateMachine();
        ReentrantTerminationReportListener reportListener = new ReentrantTerminationReportListener();
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                .setInitialGroupMembers(List.of(endpoint)).setExecutor(executor)
                .setTransport(new LocalTransport(endpoint)).setStateMachine(stateMachine)
                .setRaftNodeReportListener(reportListener).build();
        stateMachine.setNode(node);
        reportListener.setNode(node);
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        try {
            node.start().join();
            eventually(() -> assertThat(node.state().role()).isEqualTo(LEADER), ASSERT_TIMEOUT_SECONDS);
            reportListener.arm();

            executor.execute(() -> {
                taskStarted.countDown();
                try {
                    releaseTask.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(taskStarted.await(ASSERT_TIMEOUT_SECONDS, SECONDS)).isTrue();

            CompletableFuture<Ordered<Object>> terminationFuture = node.terminate();
            CompletableFuture<Ordered<Object>> queuedOperation = node.replicate(applyValue("value"));
            CompletableFuture<Ordered<RaftNodeReport>> reportFuture = node.getReport();
            CompletableFuture<Ordered<Object>> queuedTermination = node.terminate();
            CountDownLatch reentrantTermination = new CountDownLatch(1);
            queuedOperation.whenComplete((result, failure) -> {
                terminationFuture.join();
                reentrantTermination.countDown();
            });
            releaseTask.countDown();

            assertThat(executor.awaitTerminationStarted()).isTrue();
            CompletableFuture<Ordered<Object>> lateTermination = node.terminate();
            assertThat(lateTermination).isNotDone();
            executor.releaseTermination();

            terminationFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            assertThatThrownBy(() -> queuedOperation.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            RaftNodeReport terminalReport = reportFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult();
            assertThat(terminalReport.getReason()).isEqualTo(STATUS_CHANGE);
            queuedTermination.get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            lateTermination.get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            assertThat(executor.awaitTermination()).isTrue();
            assertThat(reentrantTermination.await(ASSERT_TIMEOUT_SECONDS, SECONDS)).isTrue();
            assertThat(stateMachine.awaitReentrantTermination()).isTrue();
            stateMachine.getTerminationFuture().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            assertThat(reportListener.awaitReentrantTermination()).isTrue();
            reportListener.getTerminationFuture().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            assertThat(reportListener.getObservedReport().getReason()).isEqualTo(STATUS_CHANGE);
            assertThat(reportListener.getObservedReport().getStatus()).isEqualTo(TERMINATED);
        } finally {
            releaseTask.countDown();
            executor.releaseTermination();
            node.terminate().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }
    }

    @Test(timeout = 300_000)
    public void whenDetachedRaftCallbacksWaitForTerminationThenTerminationCompletesFirst() throws Exception {
        group = LocalRaftGroup.newBuilder(1).start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        OrderedFuture<Object> pendingOperation = new OrderedFuture<>();
        OrderedFuture<Object> pendingQuery = new OrderedFuture<>();
        OrderedFuture<Object> pendingLeadershipTransfer = new OrderedFuture<>();
        readRaftState(node, () -> {
            node.state().registerFuture(node.state().commitIndex() + 1, pendingOperation);
            node.state().leaderState().queryState().addQuery(node.state().commitIndex(), "query", pendingQuery);
            assertThat(node.state().initLeadershipTransfer(node.getLocalEndpoint(), pendingLeadershipTransfer))
                    .isTrue();
            return null;
        });

        AtomicReference<CompletableFuture<Ordered<Object>>> terminationFutureRef = new AtomicReference<>();
        CountDownLatch terminationFutureAssigned = new CountDownLatch(1);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        CountDownLatch callbacksCompleted = new CountDownLatch(3);
        List.of(pendingLeadershipTransfer, pendingQuery, pendingOperation).forEach(pending ->
            pending.whenComplete((result, failure) -> {
                try {
                    terminationFutureAssigned.await();
                    if (!terminationFutureRef.get().isDone()) {
                        throw new AssertionError("Pending callback ran before termination completed");
                    }
                } catch (Throwable t) {
                    callbackFailure.compareAndSet(null, t);
                } finally {
                    callbacksCompleted.countDown();
                }
            }));

        CompletableFuture<Ordered<Object>> terminationFuture = node.terminate();
        terminationFutureRef.set(terminationFuture);
        terminationFutureAssigned.countDown();

        terminationFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        assertThat(callbacksCompleted.await(ASSERT_TIMEOUT_SECONDS, SECONDS)).isTrue();
        assertThat(callbackFailure).hasValue(null);
        assertThat(pendingLeadershipTransfer.get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult()).isNull();
        assertThatThrownBy(() -> pendingQuery.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                .hasCauseInstanceOf(NotLeaderException.class);
        assertThatThrownBy(() -> pendingOperation.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                .hasCauseInstanceOf(IndeterminateStateException.class);
        RaftNodeReport terminalReport = node.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult();
        assertThat(terminalReport.getRole()).isEqualTo(FOLLOWER);
        assertThat(terminalReport.getTerm().getLeaderEndpoint()).isNull();
    }

    @Test(timeout = 300_000)
    public void whenStartIsQueuedBehindTerminationThenStartFutureFails() throws Exception {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        ControllableRaftNodeExecutor executor = new ControllableRaftNodeExecutor();
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                .setInitialGroupMembers(List.of(endpoint)).setExecutor(executor)
                .setTransport(new LocalTransport(endpoint)).setStateMachine(new SimpleStateMachine()).build();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        executor.execute(() -> {
            taskStarted.countDown();
            try {
                releaseTask.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            assertThat(taskStarted.await(ASSERT_TIMEOUT_SECONDS, SECONDS)).isTrue();

            CompletableFuture<Ordered<Object>> terminationFuture = node.terminate();
            CompletableFuture<Ordered<Object>> startFuture = node.start();
            releaseTask.countDown();

            terminationFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            assertThatThrownBy(() -> startFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
        } finally {
            releaseTask.countDown();
            executor.onRaftNodeTerminate();
        }
    }

    @Test(timeout = 300_000)
    public void whenTerminationWinsStartSubmissionRaceThenStartFailsWithLifecycleError() throws Exception {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        BlockingRejectingRaftNodeExecutor executor = new BlockingRejectingRaftNodeExecutor();
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                .setInitialGroupMembers(List.of(endpoint)).setExecutor(executor)
                .setTransport(new LocalTransport(endpoint)).setStateMachine(new SimpleStateMachine()).build();

        executor.rejectNextSubmission();
        CompletableFuture<CompletableFuture<Ordered<Object>>> startCall = CompletableFuture.supplyAsync(node::start);
        try {
            assertThat(executor.awaitRejectedSubmission()).isTrue();

            node.terminate().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            executor.releaseRejectedSubmission();

            CompletableFuture<Ordered<Object>> startFuture = startCall.get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            assertThatThrownBy(() -> startFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                    .hasCauseExactlyInstanceOf(IllegalStateException.class);
        } finally {
            executor.releaseRejectedSubmission();
            executor.onRaftNodeTerminate();
        }
    }

    @Test(timeout = 300_000)
    public void whenTerminationWinsOperationSubmissionRaceThenOperationFailsWithLifecycleError() throws Exception {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        BlockingRejectingRaftNodeExecutor executor = new BlockingRejectingRaftNodeExecutor();
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                .setInitialGroupMembers(List.of(endpoint)).setExecutor(executor)
                .setTransport(new LocalTransport(endpoint)).setStateMachine(new SimpleStateMachine()).build();
        try {
            node.start().join();
            eventually(() -> assertThat(node.state().role()).isEqualTo(LEADER), ASSERT_TIMEOUT_SECONDS);

            executor.rejectNextSubmission();
            CompletableFuture<CompletableFuture<Ordered<Object>>> replicateCall = CompletableFuture
                    .supplyAsync(() -> node.replicate(applyValue("value")));
            assertThat(executor.awaitRejectedSubmission()).isTrue();

            node.terminate().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            executor.releaseRejectedSubmission();

            CompletableFuture<Ordered<Object>> replicateFuture = replicateCall.get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            assertThatThrownBy(() -> replicateFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                    .hasCauseExactlyInstanceOf(IllegalStateException.class);
        } finally {
            executor.releaseRejectedSubmission();
            node.terminate().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }
    }

    @Test(timeout = 300_000)
    public void whenOperationSubmissionThrowsFatalExceptionThenNodeRemainsActive() throws Exception {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        FatalRejectingRaftNodeExecutor executor = new FatalRejectingRaftNodeExecutor();
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                .setInitialGroupMembers(List.of(endpoint)).setExecutor(executor)
                .setTransport(new LocalTransport(endpoint)).setStateMachine(new SimpleStateMachine()).build();
        try {
            node.start().join();
            eventually(() -> assertThat(node.state().role()).isEqualTo(LEADER), ASSERT_TIMEOUT_SECONDS);

            executor.rejectNextSubmission();
            CompletableFuture<Ordered<Object>> future = node.replicate(applyValue("rejected"));

            assertThatThrownBy(() -> future.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                    .hasCauseExactlyInstanceOf(RaftNodeFatalException.class);
            assertThat(node.getStatus()).isEqualTo(ACTIVE);
            node.replicate(applyValue("accepted")).get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        } finally {
            if (node.getStatus() != TERMINATED) {
                node.terminate().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            }
        }
    }

    @Test(timeout = 300_000)
    public void whenTerminationSubmissionThrowsFatalExceptionThenNodeRemainsActive() throws Exception {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        FatalRejectingRaftNodeExecutor executor = new FatalRejectingRaftNodeExecutor();
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                .setInitialGroupMembers(List.of(endpoint)).setExecutor(executor)
                .setTransport(new LocalTransport(endpoint)).setStateMachine(new SimpleStateMachine()).build();
        try {
            node.start().join();
            eventually(() -> assertThat(node.state().role()).isEqualTo(LEADER), ASSERT_TIMEOUT_SECONDS);

            executor.rejectNextSubmission();
            CompletableFuture<Ordered<Object>> future = node.terminate();

            assertThatThrownBy(() -> future.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                    .hasCauseExactlyInstanceOf(RaftNodeFatalException.class);
            assertThat(node.getStatus()).isEqualTo(ACTIVE);
            node.replicate(applyValue("accepted")).get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        } finally {
            if (node.getStatus() != TERMINATED) {
                node.terminate().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            }
        }
    }

    @Test(timeout = 300_000)
    public void whenStatusIsSetDirectlyToTerminatedThenItIsRejected() throws Exception {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                .setInitialGroupMembers(List.of(endpoint)).setTransport(new LocalTransport(endpoint))
                .setStateMachine(new SimpleStateMachine()).build();
        try {
            node.start().join();

            assertThatThrownBy(() -> node.setStatus(TERMINATED)).isInstanceOf(IllegalArgumentException.class);
            assertThat(node.getStatus()).isEqualTo(ACTIVE);
        } finally {
            if (node.getStatus() != TERMINATED) {
                node.terminate().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
            }
        }
    }

    @Test(timeout = 300_000)
    public void whenImmediateTasksAreAcceptedThenTerminationDrainsEachExactlyOnce() throws Exception {
        DefaultRaftNodeExecutor executor = new DefaultRaftNodeExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        AtomicInteger executeCount = new AtomicInteger();
        AtomicInteger submitCount = new AtomicInteger();
        try {
            executor.execute(() -> {
                taskStarted.countDown();
                try {
                    releaseTask.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(taskStarted.await(ASSERT_TIMEOUT_SECONDS, SECONDS)).isTrue();

            executor.execute(executeCount::incrementAndGet);
            executor.submit(submitCount::incrementAndGet);
            executor.onRaftNodeTerminate();
            releaseTask.countDown();

            assertThat(executor.getExecutor().awaitTermination(ASSERT_TIMEOUT_SECONDS, SECONDS)).isTrue();
            assertThat(executeCount).hasValue(1);
            assertThat(submitCount).hasValue(1);
        } finally {
            releaseTask.countDown();
            executor.onRaftNodeTerminate();
        }
    }

    private static final class BlockingFailingRaftStore implements RaftStore, RaftNodeLifecycleAware {

        private final AtomicBoolean blockPersistLogEntries = new AtomicBoolean();
        private final AtomicBoolean failPersistLogEntries = new AtomicBoolean();
        private final AtomicBoolean failFlush = new AtomicBoolean();
        private final CountDownLatch failure = new CountDownLatch(1);
        private final CountDownLatch persistLogEntries = new CountDownLatch(1);
        private final CountDownLatch releasePersistLogEntries = new CountDownLatch(1);
        private final CountDownLatch termination = new CountDownLatch(1);

        void blockAndFailPersistLogEntries() {
            blockPersistLogEntries.set(true);
            failPersistLogEntries.set(true);
        }

        void failFlush() {
            failFlush.set(true);
        }

        boolean awaitFailure() throws InterruptedException {
            return failure.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        boolean awaitPersistLogEntries() throws InterruptedException {
            return persistLogEntries.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        void releasePersistLogEntries() {
            releasePersistLogEntries.countDown();
        }

        boolean awaitTermination() throws InterruptedException {
            return termination.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        boolean isTerminated() {
            return termination.getCount() == 0;
        }

        @Override
        public void onRaftNodeTerminate() {
            termination.countDown();
        }

        @Override
        public void persistAndFlushLocalEndpoint(@Nonnull RaftEndpointPersistentState localEndpointPersistentState) {
        }

        @Override
        public void persistAndFlushInitialGroupMembers(@Nonnull RaftGroupMembersView initialGroupMembers) {
        }

        @Override
        public void persistAndFlushTerm(@Nonnull RaftTermPersistentState termPersistentState) {
        }

        @Override
        public void persistLogEntries(@Nonnull List<LogEntry> logEntries) throws IOException {
            if (blockPersistLogEntries.get()) {
                persistLogEntries.countDown();
                try {
                    releasePersistLogEntries.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while injecting persistLogEntries failure", e);
                }
            }
            if (failPersistLogEntries.get()) {
                failure.countDown();
                throw new IOException("injected persistLogEntries failure");
            }
        }

        @Override
        public void persistSnapshotChunk(@Nonnull SnapshotChunk snapshotChunk) {
        }

        @Override
        public void truncateLogEntriesFrom(long logIndexInclusive) {
        }

        @Override
        public void truncateLogEntriesUntil(long logIndexInclusive) {
        }

        @Override
        public void deleteSnapshotChunks(long logIndex, int snapshotChunkCount) {
        }

        @Override
        public void flush() throws IOException {
            if (failFlush.get()) {
                failure.countDown();
                throw new IOException("injected flush failure");
            }
        }
    }

    private static final class ControllableRaftNodeExecutor extends DefaultRaftNodeExecutor {

        private final AtomicBoolean blockTermination = new AtomicBoolean();
        private final CountDownLatch releaseTermination = new CountDownLatch(1);
        private final CountDownLatch termination = new CountDownLatch(1);
        private final CountDownLatch terminationStarted = new CountDownLatch(1);

        void blockTermination() {
            blockTermination.set(true);
        }

        @Override
        public void onRaftNodeTerminate() {
            terminationStarted.countDown();
            if (blockTermination.get()) {
                try {
                    releaseTermination.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            super.onRaftNodeTerminate();
            termination.countDown();
        }

        boolean awaitTerminationStarted() throws InterruptedException {
            return terminationStarted.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        void releaseTermination() {
            releaseTermination.countDown();
        }

        boolean awaitTermination() throws InterruptedException {
            return termination.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        boolean isTerminated() {
            return termination.getCount() == 0;
        }
    }

    private static final class BlockingRejectingRaftNodeExecutor extends DefaultRaftNodeExecutor {

        private final AtomicBoolean rejectNextSubmission = new AtomicBoolean();
        private final CountDownLatch rejectedSubmission = new CountDownLatch(1);
        private final CountDownLatch releaseRejectedSubmission = new CountDownLatch(1);

        void rejectNextSubmission() {
            rejectNextSubmission.set(true);
        }

        @Override
        public void execute(@Nonnull Runnable task) {
            if (!rejectNextSubmission.compareAndSet(true, false)) {
                super.execute(task);
                return;
            }

            rejectedSubmission.countDown();
            try {
                releaseRejectedSubmission.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException(e);
            }
            throw new RejectedExecutionException("injected submission rejection");
        }

        boolean awaitRejectedSubmission() throws InterruptedException {
            return rejectedSubmission.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        void releaseRejectedSubmission() {
            releaseRejectedSubmission.countDown();
        }
    }

    private static final class FatalRejectingRaftNodeExecutor extends DefaultRaftNodeExecutor {

        private final AtomicBoolean rejectNextSubmission = new AtomicBoolean();

        void rejectNextSubmission() {
            rejectNextSubmission.set(true);
        }

        @Override
        public void execute(@Nonnull Runnable task) {
            if (rejectNextSubmission.compareAndSet(true, false)) {
                throw new RaftNodeFatalException("injected submission rejection", null, null);
            }

            super.execute(task);
        }
    }

    private static final class ReentrantTerminationStateMachine extends SimpleStateMachine {

        private final CountDownLatch reentrantTermination = new CountDownLatch(1);
        private final AtomicReference<CompletableFuture<Ordered<Object>>> terminationFuture = new AtomicReference<>();
        private RaftNodeImpl node;

        void setNode(RaftNodeImpl node) {
            this.node = node;
        }

        @Override
        public void onRaftNodeTerminate() {
            super.onRaftNodeTerminate();
            terminationFuture.set(node.terminate());
            reentrantTermination.countDown();
        }

        boolean awaitReentrantTermination() throws InterruptedException {
            return reentrantTermination.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        CompletableFuture<Ordered<Object>> getTerminationFuture() {
            return terminationFuture.get();
        }
    }

    private static final class ReentrantTerminationReportListener implements RaftNodeReportListener {

        private final AtomicBoolean armed = new AtomicBoolean();
        private final CountDownLatch reentrantTermination = new CountDownLatch(1);
        private final AtomicReference<CompletableFuture<Ordered<Object>>> terminationFuture = new AtomicReference<>();
        private final AtomicReference<RaftNodeReport> observedReport = new AtomicReference<>();
        private RaftNodeImpl node;

        void setNode(RaftNodeImpl node) {
            this.node = node;
        }

        void arm() {
            armed.set(true);
        }

        @Override
        public void accept(RaftNodeReport report) {
            if (!armed.compareAndSet(true, false)) {
                return;
            }

            observedReport.set(report);
            terminationFuture.set(node.terminate());
            reentrantTermination.countDown();
        }

        boolean awaitReentrantTermination() throws InterruptedException {
            return reentrantTermination.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        CompletableFuture<Ordered<Object>> getTerminationFuture() {
            return terminationFuture.get();
        }

        RaftNodeReport getObservedReport() {
            return observedReport.get();
        }
    }

}
