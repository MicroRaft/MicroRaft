/*
 * Copyright (c) 2020, MicroRaft.
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

import static io.microraft.report.RaftNodeReport.RaftNodeReportReason.FATAL_FAILURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.RaftNodeStatus;
import io.microraft.executor.impl.DefaultRaftNodeExecutor;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;
import io.microraft.model.impl.DefaultRaftModelFactory;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.LogEntry.LogEntryBuilder;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotChunk.SnapshotChunkBuilder;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;
import io.microraft.model.message.AppendEntriesFailureResponse.AppendEntriesFailureResponseBuilder;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.InstallSnapshotRequest.InstallSnapshotRequestBuilder;
import io.microraft.model.message.InstallSnapshotResponse.InstallSnapshotResponseBuilder;
import io.microraft.model.message.PreVoteRequest.PreVoteRequestBuilder;
import io.microraft.model.message.PreVoteResponse.PreVoteResponseBuilder;
import io.microraft.model.message.RaftMessage;
import io.microraft.model.message.TriggerLeaderElectionRequest.TriggerLeaderElectionRequestBuilder;
import io.microraft.model.message.VoteRequest.VoteRequestBuilder;
import io.microraft.model.message.VoteResponse.VoteResponseBuilder;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.persistence.RaftStore;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReportListener;
import io.microraft.statemachine.StateMachine;
import io.microraft.test.util.BaseTest;
import io.microraft.transport.Transport;

public class RaftNodeLifecycleAwareTest extends BaseTest {

    private static final int ASSERT_TIMEOUT_SECONDS = 10;
    private static final int LIFECYCLE_COMPONENT_COUNT = 6;

    private final RaftEndpoint localEndpoint = LocalRaftEndpoint.newEndpoint();
    private final List<RaftEndpoint> initialMembers = List.of(localEndpoint, LocalRaftEndpoint.newEndpoint(),
            LocalRaftEndpoint.newEndpoint());

    private final TerminationFailureInjector terminationFailureInjector = new TerminationFailureInjector();
    private final DelegatingRaftNodeExecutor executor = new DelegatingRaftNodeExecutor();
    private final NopTransport transport = new NopTransport();
    private final NopStateMachine stateMachine = new NopStateMachine();
    private final DelegatingRaftModelFactory modelFactory = new DelegatingRaftModelFactory();
    private final NopRaftNodeReportListener reportListener = new NopRaftNodeReportListener();
    private final NopRaftStore store = new NopRaftStore();

    private RaftNode raftNode;

    @Before
    public void init() {
        raftNode = RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(localEndpoint)
                .setInitialGroupMembers(initialMembers).setExecutor(executor).setTransport(transport)
                .setStateMachine(stateMachine).setModelFactory(modelFactory).setRaftNodeReportListener(reportListener)
                .setStore(store).build();
    }

    @After
    public void tearDown() {
        if (raftNode != null) {
            raftNode.terminate();
        }
    }

    @Test
    public void testExecutorStart() {
        raftNode.start().join();

        assertThat(executor.startCall).isGreaterThan(0);
        assertThat(executor.executeCall).isGreaterThan(0);
        assertThat(executor.executeCall).isLessThan(executor.startCall);
    }

    @Test
    public void testExecutorTerminate() {
        raftNode.start().join();
        raftNode.terminate().join();

        assertThat(executor.startCall).isGreaterThan(0);
        assertThat(executor.executeCall).isGreaterThan(0);
        assertThat(executor.terminateCall).isGreaterThan(0);
        assertThat(executor.executeCall).isLessThan(executor.startCall);
        assertThat(executor.lastExecuteCall).isLessThan(executor.terminateCall);
    }

    @Test
    public void testTransportStart() {
        raftNode.start().join();

        assertThat(transport.startCall).isGreaterThan(0);
        assertThat(transport.sendCall).isGreaterThan(0);
        assertThat(transport.startCall).isLessThan(transport.sendCall);
    }

    @Test
    public void testTransportTerminate() {
        raftNode.start().join();
        raftNode.terminate().join();

        assertThat(transport.startCall).isGreaterThan(0);
        assertThat(transport.sendCall).isGreaterThan(0);
        assertThat(transport.terminateCall).isGreaterThan(0);
        assertThat(transport.startCall).isLessThan(transport.sendCall);
        assertThat(transport.sendCall).isLessThan(transport.terminateCall);
        assertThat(transport.lastSendCall).isLessThan(transport.terminateCall);
    }

    @Test
    public void testStateMachineStart() {
        raftNode.start().join();

        assertThat(stateMachine.startCall).isGreaterThan(0);
    }

    @Test
    public void testStateMachineTerminate() {
        raftNode.start().join();
        raftNode.terminate().join();

        assertThat(stateMachine.startCall).isGreaterThan(0);
        assertThat(stateMachine.terminateCall).isGreaterThan(0);
        assertThat(stateMachine.startCall).isLessThan(stateMachine.terminateCall);
    }

    @Test
    public void testModelFactoryStart() {
        raftNode.start().join();

        assertThat(modelFactory.startCall).isGreaterThan(0);
        assertThat(modelFactory.createCall).isGreaterThan(0);
        assertThat(modelFactory.startCall).isLessThan(modelFactory.createCall);
    }

    @Test
    public void testModelFactoryTerminate() {
        raftNode.start().join();
        raftNode.terminate().join();

        assertThat(modelFactory.startCall).isGreaterThan(0);
        assertThat(modelFactory.createCall).isGreaterThan(0);
        assertThat(modelFactory.terminateCall).isGreaterThan(0);
        assertThat(modelFactory.startCall).isLessThan(modelFactory.createCall);
        assertThat(modelFactory.createCall).isLessThan(modelFactory.terminateCall);
        assertThat(modelFactory.lastCreateCall).isLessThan(modelFactory.terminateCall);
    }

    @Test
    public void testReportListenerStart() {
        raftNode.start().join();

        assertThat(reportListener.startCall).isGreaterThan(0);
        assertThat(reportListener.acceptCall).isGreaterThan(0);
        assertThat(reportListener.startCall).isLessThan(reportListener.acceptCall);
    }

    @Test
    public void testReportListenerTerminate() {
        raftNode.start().join();
        raftNode.terminate().join();

        assertThat(reportListener.startCall).isGreaterThan(0);
        assertThat(reportListener.acceptCall).isGreaterThan(0);
        assertThat(reportListener.terminateCall).isGreaterThan(0);
        assertThat(reportListener.startCall).isLessThan(reportListener.acceptCall);
        assertThat(reportListener.acceptCall).isLessThan(reportListener.terminateCall);
        assertThat(reportListener.lastAcceptCall).isLessThan(reportListener.terminateCall);
    }

    @Test
    public void testLifecycleComponentsTerminateWhenTerminalReportPublicationFails() {
        FailingToStringGroupId groupId = new FailingToStringGroupId();
        raftNode = RaftNode.newBuilder().setGroupId(groupId).setLocalEndpoint(localEndpoint)
                .setInitialGroupMembers(initialMembers).setExecutor(executor).setTransport(transport)
                .setStateMachine(stateMachine).setModelFactory(modelFactory).setRaftNodeReportListener(reportListener)
                .setStore(store).build();
        raftNode.start().join();

        groupId.failToString = true;
        raftNode.terminate().join();

        assertThat(stateMachine.terminateCall).isGreaterThan(0);
        assertThat(executor.terminateCall).isGreaterThan(0);
        assertThat(transport.terminateCall).isGreaterThan(0);
        assertThat(store.terminateCall).isGreaterThan(0);
        assertThat(modelFactory.terminateCall).isGreaterThan(0);
        assertThat(reportListener.terminateCall).isGreaterThan(0);
    }

    @Test
    public void testTerminationCallbacksFinishExactlyOnceBeforeTerminateFutureCompletesWhenCallbackFails()
            throws Exception {
        raftNode.start().join();
        terminationFailureInjector.blockAndFailNextCallback();

        CompletableFuture<?> terminationFuture = raftNode.terminate();
        CompletableFuture<Void> completion;
        try {
            assertThat(terminationFailureInjector.awaitCallback()).isTrue();
            assertThat(terminationFuture).isNotDone();
            assertThat(reportListener.lastReport.getStatus()).isEqualTo(RaftNodeStatus.TERMINATED);
            completion = terminationFuture.thenRun(() -> {
                assertLifecycleComponentsTerminatedExactlyOnce();
                assertThat(reportListener.lastReport.getStatus()).isEqualTo(RaftNodeStatus.TERMINATED);
            });
        } finally {
            terminationFailureInjector.releaseCallback();
        }

        completion.get(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void assertLifecycleComponentsTerminatedExactlyOnce() {
        assertThat(executor.terminateCall).isPositive();
        assertThat(transport.terminateCall).isPositive();
        assertThat(stateMachine.terminateCall).isPositive();
        assertThat(store.terminateCall).isPositive();
        assertThat(modelFactory.terminateCall).isPositive();
        assertThat(reportListener.terminateCall).isPositive();
        assertThat(terminationFailureInjector.callbackCount).isEqualTo(LIFECYCLE_COMPONENT_COUNT);
        assertThat(terminationFailureInjector.failureCount).isEqualTo(1);
    }

    @Test
    public void testStoreStart() {
        raftNode.start().join();

        assertThat(store.startCall).isGreaterThan(0);
        assertThat(store.persistCall).isGreaterThan(0);
        assertThat(store.startCall).isLessThan(store.persistCall);
    }

    @Test
    public void testStoreTerminate() {
        raftNode.start().join();
        raftNode.terminate().join();

        assertThat(store.startCall).isGreaterThan(0);
        assertThat(store.persistCall).isGreaterThan(0);
        assertThat(store.terminateCall).isGreaterThan(0);
        assertThat(store.startCall).isLessThan(store.persistCall);
        assertThat(store.persistCall).isLessThan(store.terminateCall);
        assertThat(store.lastPersistCall).isLessThan(store.terminateCall);
    }

    @Test
    public void testTerminateCalledForAllComponentsWhenStartFails() {
        stateMachine.failOnStart = true;
        try {
            raftNode.start().join();
            fail("Start should fail when any component fails on start");
        } catch (CompletionException e) {
            assertThat(e.getCause()).isSameAs(stateMachine.startFailure);
        }

        assertThat(raftNode.getStatus()).isEqualTo(RaftNodeStatus.TERMINATED);
        assertThat(raftNode.getReport().join().getResult().getReason()).isEqualTo(FATAL_FAILURE);
        assertThat(stateMachine.terminateCall).isGreaterThan(0);
        if (executor.startCall > 0) {
            assertThat(executor.terminateCall).isGreaterThan(0);
        }
        if (transport.startCall > 0) {
            assertThat(transport.terminateCall).isGreaterThan(0);
        }
        if (store.startCall > 0) {
            assertThat(store.terminateCall).isGreaterThan(0);
        }
        if (modelFactory.startCall > 0) {
            assertThat(modelFactory.terminateCall).isGreaterThan(0);
        }
        if (reportListener.startCall > 0) {
            assertThat(reportListener.terminateCall).isGreaterThan(0);
        }
    }

    @Test
    public void testTerminateCalledForAllComponentsWhenStartAndTerminateFails() {
        stateMachine.failOnStart = true;
        stateMachine.failOnTerminate = true;
        try {
            raftNode.start().join();
            fail("Start should fail when any component fails on start");
        } catch (CompletionException ignored) {
        }

        assertThat(raftNode.getStatus()).isEqualTo(RaftNodeStatus.TERMINATED);
        assertThat(stateMachine.terminateCall).isGreaterThan(0);
        if (executor.startCall > 0) {
            assertThat(executor.terminateCall).isGreaterThan(0);
        }
        if (transport.startCall > 0) {
            assertThat(transport.terminateCall).isGreaterThan(0);
        }
        if (store.startCall > 0) {
            assertThat(store.terminateCall).isGreaterThan(0);
        }
        if (modelFactory.startCall > 0) {
            assertThat(modelFactory.terminateCall).isGreaterThan(0);
        }
        if (reportListener.startCall > 0) {
            assertThat(reportListener.terminateCall).isGreaterThan(0);
        }
    }

    private class NopTransport implements Transport, RaftNodeLifecycleAware {
        private volatile int startCall;
        private volatile int terminateCall;
        private volatile int sendCall;
        private volatile int lastSendCall;
        private volatile int callOrder;

        @Override
        public void onRaftNodeStart() {
            if (startCall == 0) {
                startCall = ++callOrder;
            }
        }

        @Override
        public void onRaftNodeTerminate() {
            if (terminateCall == 0) {
                terminateCall = ++callOrder;
            }
            terminationFailureInjector.onTerminate();
        }

        @Override
        public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
            lastSendCall = ++callOrder;
            if (sendCall == 0) {
                sendCall = lastSendCall;
            }
        }

        @Override
        public boolean isReachable(@Nonnull RaftEndpoint endpoint) {
            return false;
        }
    }

    private class NopStateMachine implements StateMachine, RaftNodeLifecycleAware {

        private final RuntimeException startFailure = new RuntimeException("failed on purpose!");
        private volatile int startCall;
        private volatile int terminateCall;
        private volatile int callOrder;
        private volatile boolean failOnStart;
        private volatile boolean failOnTerminate;

        @Override
        public void onRaftNodeStart() {
            if (startCall == 0) {
                startCall = ++callOrder;
            }
            if (failOnStart) {
                throw startFailure;
            }
        }

        @Override
        public void onRaftNodeTerminate() {
            if (terminateCall == 0) {
                terminateCall = ++callOrder;
            }
            terminationFailureInjector.onTerminate();
            if (failOnTerminate) {
                throw new RuntimeException("failed on purpose!");
            }
        }

        @Override
        public Object runOperation(long commitIndex, @Nonnull Object operation) {
            return null;
        }

        @Override
        public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        }

        @Override
        public void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks) {
        }

        @Nonnull
        @Override
        public Object getNewTermOperation() {
            return null;
        }
    }

    private class DelegatingRaftNodeExecutor extends DefaultRaftNodeExecutor implements RaftNodeLifecycleAware {
        private volatile int startCall;
        private volatile int terminateCall;
        private volatile int executeCall;
        private volatile int lastExecuteCall;
        private volatile int callOrder;

        @Override
        public void onRaftNodeStart() {
            if (startCall == 0) {
                startCall = ++callOrder;
            }
        }

        @Override
        public void onRaftNodeTerminate() {
            if (terminateCall == 0) {
                terminateCall = ++callOrder;
            }
            terminationFailureInjector.onTerminate();
        }

        @Override
        public void execute(@Nonnull Runnable task) {
            lastExecuteCall = ++callOrder;
            if (executeCall == 0) {
                executeCall = lastExecuteCall;
            }
            super.execute(task);
        }

        @Override
        public void submit(@Nonnull Runnable task) {
            lastExecuteCall = ++callOrder;
            if (executeCall == 0) {
                executeCall = lastExecuteCall;
            }
            super.submit(task);
        }

        @Override
        public void schedule(@Nonnull Runnable task, long delay, @Nonnull TimeUnit timeUnit) {
            lastExecuteCall = ++callOrder;
            if (executeCall == 0) {
                executeCall = lastExecuteCall;
            }
            super.schedule(task, delay, timeUnit);
        }
    }

    private class DelegatingRaftModelFactory extends DefaultRaftModelFactory implements RaftNodeLifecycleAware {
        private volatile int startCall;
        private volatile int terminateCall;
        private volatile int createCall;
        private volatile int lastCreateCall;
        private volatile int callOrder;

        @Override
        public void onRaftNodeStart() {
            if (startCall == 0) {
                startCall = ++callOrder;
            }
        }

        @Override
        public void onRaftNodeTerminate() {
            if (terminateCall == 0) {
                terminateCall = ++callOrder;
            }
            terminationFailureInjector.onTerminate();
        }

        @Nonnull
        @Override
        public LogEntryBuilder createLogEntryBuilder() {
            recordCall();
            return super.createLogEntryBuilder();
        }

        @Nonnull
        @Override
        public SnapshotEntryBuilder createSnapshotEntryBuilder() {
            recordCall();
            return super.createSnapshotEntryBuilder();
        }

        @Nonnull
        @Override
        public SnapshotChunkBuilder createSnapshotChunkBuilder() {
            recordCall();
            return super.createSnapshotChunkBuilder();
        }

        @Nonnull
        @Override
        public AppendEntriesRequest.AppendEntriesRequestBuilder createAppendEntriesRequestBuilder() {
            recordCall();
            return super.createAppendEntriesRequestBuilder();
        }

        @Nonnull
        @Override
        public AppendEntriesSuccessResponse.AppendEntriesSuccessResponseBuilder createAppendEntriesSuccessResponseBuilder() {
            recordCall();
            return super.createAppendEntriesSuccessResponseBuilder();
        }

        @Nonnull
        @Override
        public AppendEntriesFailureResponseBuilder createAppendEntriesFailureResponseBuilder() {
            recordCall();
            return super.createAppendEntriesFailureResponseBuilder();
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder createInstallSnapshotRequestBuilder() {
            recordCall();
            return super.createInstallSnapshotRequestBuilder();
        }

        @Nonnull
        @Override
        public InstallSnapshotResponseBuilder createInstallSnapshotResponseBuilder() {
            recordCall();
            return super.createInstallSnapshotResponseBuilder();
        }

        @Nonnull
        @Override
        public PreVoteRequestBuilder createPreVoteRequestBuilder() {
            recordCall();
            return super.createPreVoteRequestBuilder();
        }

        @Nonnull
        @Override
        public PreVoteResponseBuilder createPreVoteResponseBuilder() {
            recordCall();
            return super.createPreVoteResponseBuilder();
        }

        @Nonnull
        @Override
        public TriggerLeaderElectionRequestBuilder createTriggerLeaderElectionRequestBuilder() {
            recordCall();
            return super.createTriggerLeaderElectionRequestBuilder();
        }

        @Nonnull
        @Override
        public VoteRequestBuilder createVoteRequestBuilder() {
            recordCall();
            return super.createVoteRequestBuilder();
        }

        @Nonnull
        @Override
        public VoteResponseBuilder createVoteResponseBuilder() {
            recordCall();
            return super.createVoteResponseBuilder();
        }

        @Nonnull
        @Override
        public UpdateRaftGroupMembersOpBuilder createUpdateRaftGroupMembersOpBuilder() {
            recordCall();
            return super.createUpdateRaftGroupMembersOpBuilder();
        }

        private void recordCall() {
            lastCreateCall = ++callOrder;
            if (createCall == 0) {
                createCall = lastCreateCall;
            }
        }
    }

    private class NopRaftNodeReportListener implements RaftNodeReportListener, RaftNodeLifecycleAware {
        private volatile int startCall;
        private volatile int terminateCall;
        private volatile int acceptCall;
        private volatile int lastAcceptCall;
        private volatile int callOrder;
        private volatile RaftNodeReport lastReport;

        @Override
        public void onRaftNodeStart() {
            if (startCall == 0) {
                startCall = ++callOrder;
            }
        }

        @Override
        public void onRaftNodeTerminate() {
            if (terminateCall == 0) {
                terminateCall = ++callOrder;
            }
            terminationFailureInjector.onTerminate();
        }

        @Override
        public void accept(RaftNodeReport report) {
            lastReport = report;
            lastAcceptCall = ++callOrder;
            if (acceptCall == 0) {
                acceptCall = lastAcceptCall;
            }
        }
    }

    private static final class TerminationFailureInjector {
        private final CountDownLatch callbackStarted = new CountDownLatch(1);
        private final CountDownLatch callbackRelease = new CountDownLatch(1);
        private volatile boolean failNextCallback;
        private volatile int callbackCount;
        private volatile int failureCount;

        private void blockAndFailNextCallback() {
            failNextCallback = true;
        }

        private void onTerminate() {
            callbackCount++;
            if (!failNextCallback) {
                return;
            }

            failNextCallback = false;
            callbackStarted.countDown();
            try {
                if (!callbackRelease.await(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new AssertionError("Timed out waiting to release termination callback");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }

            failureCount++;
            throw new RuntimeException("failed on purpose!");
        }

        private boolean awaitCallback() throws InterruptedException {
            return callbackStarted.await(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        private void releaseCallback() {
            callbackRelease.countDown();
        }
    }

    private static final class FailingToStringGroupId {

        private boolean failToString;

        @Override
        public String toString() {
            if (failToString) {
                throw new IllegalStateException("injected toString failure");
            }

            return "default";
        }
    }

    private class NopRaftStore implements RaftStore, RaftNodeLifecycleAware {
        private volatile int startCall;
        private volatile int terminateCall;
        private volatile int persistCall;
        private volatile int lastPersistCall;
        private volatile int callOrder;

        @Override
        public void onRaftNodeStart() {
            if (startCall == 0) {
                startCall = ++callOrder;
            }
        }

        @Override
        public void onRaftNodeTerminate() {
            if (terminateCall == 0) {
                terminateCall = ++callOrder;
            }
            terminationFailureInjector.onTerminate();
        }

        @Override
        public void persistAndFlushLocalEndpoint(@Nonnull RaftEndpointPersistentState localEndpointPersistentState) {
            recordCall();
        }

        @Override
        public void persistAndFlushInitialGroupMembers(@Nonnull RaftGroupMembersView initialGroupMembers) {
            recordCall();
        }

        @Override
        public void persistAndFlushTerm(@Nonnull RaftTermPersistentState termPersistentState) {
            recordCall();
        }

        @Override
        public void persistLogEntries(@Nonnull List<LogEntry> logEntries) throws IOException {
            recordCall();
        }

        @Override
        public void persistSnapshotChunk(@Nonnull SnapshotChunk snapshotChunk) {
            recordCall();
        }

        @Override
        public void truncateLogEntriesFrom(long logIndexInclusive) {
            recordCall();
        }

        @Override
        public void truncateLogEntriesUntil(long logIndexInclusive) {
            recordCall();
        }

        @Override
        public void deleteSnapshotChunks(long logIndex, int snapshotChunkCount) {
            recordCall();
        }

        @Override
        public void flush() {
            recordCall();
        }

        private void recordCall() {
            lastPersistCall = ++callOrder;
            if (persistCall == 0) {
                persistCall = lastPersistCall;
            }
        }

    }

}
