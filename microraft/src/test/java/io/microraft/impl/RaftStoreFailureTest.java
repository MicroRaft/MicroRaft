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

import static io.microraft.MembershipChangeMode.ADD_LEARNER;
import static io.microraft.MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER;
import static io.microraft.MembershipChangeMode.REMOVE_MEMBER;
import static io.microraft.RaftNodeStatus.ACTIVE;
import static io.microraft.RaftNodeStatus.TERMINATED;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.report.RaftNodeReport.RaftNodeReportReason.FATAL_FAILURE;
import static io.microraft.report.RaftNodeReport.RaftNodeReportReason.STATUS_CHANGE;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNodeStatus;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.RaftStoreException;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.state.RaftState;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.impl.DefaultRaftModelFactory;
import io.microraft.model.log.BaseLogEntry;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.VoteRequest;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.persistence.RaftStore;
import io.microraft.report.RaftNodeReport;
import io.microraft.test.util.BaseTest;

public class RaftStoreFailureTest extends BaseTest {

    private static final int ASSERT_TIMEOUT_SECONDS = 10;
    private static final int LOG_CAPACITY = 100;
    private static final long SNAPSHOT_INDEX_OFFSET = 10;

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void whenPersistingLogEntryFailsThenNodeTerminatesWithCachedFatalReport() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            storeRef.set(store);
            return store;
        }).start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        FailingRaftStore store = storeRef.get();

        store.failPersistLogEntries();
        AtomicReference<RaftNodeStatus> statusOnFailure = new AtomicReference<>();
        CountDownLatch futureFailed = new CountDownLatch(1);
        node.replicate(applyValue("value")).whenComplete((result, failure) -> {
            statusOnFailure.set(node.getStatus());
            futureFailed.countDown();
        });

        assertThat(store.awaitFailure()).isTrue();
        assertThat(futureFailed.await(ASSERT_TIMEOUT_SECONDS, SECONDS)).isTrue();
        assertThat(statusOnFailure).hasValue(TERMINATED);
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        eventually(() -> assertThat(terminalReports(node)).hasSize(1), ASSERT_TIMEOUT_SECONDS);

        RaftNodeReport terminalReport = terminalReports(node).get(0);
        Ordered<RaftNodeReport> first = node.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        Ordered<RaftNodeReport> second = node.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS);

        assertThat(terminalReport.getReason()).isEqualTo(FATAL_FAILURE);
        assertThat(first.getResult()).isSameAs(terminalReport);
        assertThat(second.getResult()).isSameAs(terminalReport);
    }

    @Test(timeout = 300_000)
    public void whenStoreThrowsUncheckedExceptionThenNodeTerminatesWithFatalReport() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            storeRef.set(store);
            return store;
        }).start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        FailingRaftStore store = storeRef.get();

        store.failPersistLogEntriesUnchecked();
        node.replicate(applyValue("value"));

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        eventually(() -> assertThat(terminalReports(node)).singleElement()
                .extracting(RaftNodeReport::getReason).isEqualTo(FATAL_FAILURE), ASSERT_TIMEOUT_SECONDS);
    }

    @Test(timeout = 300_000)
    public void whenMembershipChangePersistenceFailsThenNodeTerminatesAndFutureIsIndeterminate() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            storeRef.set(store);
            return store;
        }).start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        FailingRaftStore store = storeRef.get();
        node.replicate(applyValue("seed")).get(ASSERT_TIMEOUT_SECONDS, SECONDS);

        store.failPersistLogEntries();
        CompletableFuture<?> future = node.changeMembership(LocalRaftEndpoint.newEndpoint(), ADD_LEARNER, 0);

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThatThrownBy(() -> future.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                .hasCauseInstanceOf(IndeterminateStateException.class);
        assertThat(node.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult().getReason())
                .isEqualTo(FATAL_FAILURE);
    }

    @Test(timeout = 300_000)
    public void whenFollowerLogEntryPersistenceFailsThenFollowerTerminatesWithFatalReport() throws Exception {
        Map<RaftEndpoint, FailingRaftStore> stores = new ConcurrentHashMap<>();
        group = LocalRaftGroup.newBuilder(3).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            stores.put(endpoint, store);
            return store;
        }).start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint()).get(0);
        FailingRaftStore store = stores.get(follower.getLocalEndpoint());
        long lastLogIndex = follower.state().log().lastLogOrSnapshotIndex();

        group.splitMembers(follower.getLocalEndpoint());
        leader.replicate(applyValue("value-1")).get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        leader.replicate(applyValue("value-2")).get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        store.failPersistLogEntries();
        group.merge();

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(follower.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThat(store.failedPersistLogEntryCount()).isGreaterThan(1);
        assertThat(follower.state().log().lastLogOrSnapshotIndex()).isEqualTo(lastLogIndex);
        assertThat(follower.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult().getReason())
                .isEqualTo(FATAL_FAILURE);
    }

    @Test(timeout = 300_000)
    public void whenActiveNodeLogFlushFailsThenNodeTerminatesWithFatalReport() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            storeRef.set(store);
            return store;
        }).start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        FailingRaftStore store = storeRef.get();
        long commitIndex = node.state().commitIndex();

        store.failFlush();
        CompletableFuture<Ordered<Object>> future = node.replicate(applyValue("value"));

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThatThrownBy(() -> future.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                .hasCauseInstanceOf(IndeterminateStateException.class);
        assertThat(node.state().commitIndex()).isEqualTo(commitIndex);
        assertThat(node.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult().getReason())
                .isEqualTo(FATAL_FAILURE);
    }

    @Test(timeout = 300_000)
    public void whenInitialStatePersistenceFailsThenNodeTerminatesWithFatalReport() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            store.failInitialStatePersistence();
            storeRef.set(store);
            return store;
        }).build();
        RaftNodeImpl node = group.getNodes().get(0);

        group.start();

        assertThat(storeRef.get().awaitFailure()).isTrue();
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        eventually(() -> assertThat(terminalReports(node)).singleElement()
                .extracting(RaftNodeReport::getReason).isEqualTo(FATAL_FAILURE), ASSERT_TIMEOUT_SECONDS);
    }

    @Test(timeout = 300_000)
    public void whenInitialGroupPersistenceFailsThenNodeTerminatesWithFatalReport() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            store.failInitialGroupPersistence();
            storeRef.set(store);
            return store;
        }).build();
        RaftNodeImpl node = group.getNodes().get(0);

        group.start();

        assertThat(storeRef.get().awaitFailure()).isTrue();
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        eventually(() -> assertThat(terminalReports(node)).singleElement()
                .extracting(RaftNodeReport::getReason).isEqualTo(FATAL_FAILURE), ASSERT_TIMEOUT_SECONDS);
    }

    @Test(timeout = 300_000)
    public void whenTermPersistenceFailsThenNodeTerminatesWithFatalReport() throws Exception {
        Map<RaftEndpoint, FailingRaftStore> stores = new ConcurrentHashMap<>();
        group = LocalRaftGroup.newBuilder(2).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            stores.put(endpoint, store);
            return store;
        }).start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint()).get(0);
        FailingRaftStore store = stores.get(follower.getLocalEndpoint());
        BaseLogEntry lastEntry = follower.state().log().lastLogOrSnapshotEntry();
        VoteRequest request = follower.getModelFactory().createVoteRequestBuilder().setGroupId(follower.getGroupId())
                .setSender(leader.getLocalEndpoint()).setTerm(follower.state().term() + 1)
                .setLastLogTerm(lastEntry.getTerm()).setLastLogIndex(lastEntry.getIndex()).setSticky(false).build();

        store.failTermPersistence();
        follower.handle(request);

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(follower.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThat(terminalReports(follower)).singleElement()
                .extracting(RaftNodeReport::getReason).isEqualTo(FATAL_FAILURE);
    }

    @Test(timeout = 300_000)
    public void whenLocalSnapshotFlushFailsThenNodeTerminatesWithFatalReport() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            storeRef.set(store);
            return store;
        }).start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        node.replicate(applyValue("value")).join();
        FailingRaftStore store = storeRef.get();

        store.failFlush();
        CompletableFuture<Ordered<RaftNodeReport>> snapshotFuture = node.takeSnapshot();

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThatThrownBy(() -> snapshotFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                .hasCauseInstanceOf(IndeterminateStateException.class);
        assertThat(terminalReports(node)).singleElement()
                .extracting(RaftNodeReport::getReason).isEqualTo(FATAL_FAILURE);
    }

    @Test(timeout = 300_000)
    public void whenLocalSnapshotPersistenceFailsThenNodeTerminatesWithFatalReport() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            storeRef.set(store);
            return store;
        }).start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        node.replicate(applyValue("value")).join();
        FailingRaftStore store = storeRef.get();

        store.failSnapshotPersistence();
        CompletableFuture<Ordered<RaftNodeReport>> snapshotFuture = node.takeSnapshot();

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThatThrownBy(() -> snapshotFuture.get(ASSERT_TIMEOUT_SECONDS, SECONDS))
                .hasCauseInstanceOf(IndeterminateStateException.class);
        assertThat(terminalReports(node)).singleElement()
                .extracting(RaftNodeReport::getReason).isEqualTo(FATAL_FAILURE);
    }

    @Test(timeout = 300_000)
    public void whenVotingRolePersistenceFailsThenNodeTerminatesWithFatalReport() throws Exception {
        Map<RaftEndpoint, FailingRaftStore> stores = new ConcurrentHashMap<>();
        group = LocalRaftGroup.newBuilder(3).enableNewTermOperation().setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            stores.put(endpoint, store);
            return store;
        }).start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl learner = group.createNewNode();
        long membersIndex = leader.changeMembership(learner.getLocalEndpoint(), ADD_LEARNER, 0).join()
                .getCommitIndex();
        FailingRaftStore store = stores.get(learner.getLocalEndpoint());

        store.failLocalEndpointPersistence();
        leader.changeMembership(learner.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, membersIndex).join();

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(learner.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThat(learner.getReport().join().getResult().getReason()).isEqualTo(FATAL_FAILURE);
    }

    @Test(timeout = 300_000)
    public void whenVotingRoleDemotionPersistenceFailsThenFatalStoreExceptionIsThrown() throws Exception {
        RaftEndpoint localEndpoint = LocalRaftEndpoint.newEndpoint();
        RaftEndpoint initialMember = LocalRaftEndpoint.newEndpoint();
        DefaultRaftModelFactory modelFactory = new DefaultRaftModelFactory();
        RaftGroupMembersView initialMembers = modelFactory.createRaftGroupMembersViewBuilder().setLogIndex(0)
                .setMembers(List.of(initialMember)).setVotingMembers(List.of(initialMember)).build();
        FailingRaftStore store = new FailingRaftStore();
        RaftState state = RaftState.create("default", localEndpoint, initialMembers, LOG_CAPACITY, store, modelFactory);
        List<RaftEndpoint> promotedMembers = List.of(initialMember, localEndpoint);
        state.updateGroupMembers(1, promotedMembers, promotedMembers, 0);
        assertThat(state.role()).isEqualTo(FOLLOWER);

        store.failLocalEndpointPersistence();

        assertThatThrownBy(state::revertGroupMembers).isInstanceOf(RaftStoreException.class)
                .hasMessage("Demotion to non-voting member failed").hasCauseInstanceOf(IOException.class);
        assertThat(store.awaitFailure()).isTrue();
        assertThat(state.role()).isEqualTo(FOLLOWER);
        assertThat(state.effectiveGroupMembers().getVotingMembers()).contains(localEndpoint);
        assertThat(state.committedGroupMembers().getVotingMembers()).doesNotContain(localEndpoint);
    }

    @Test(timeout = 300_000)
    public void whenSingletonStartupFlushFailsThenStartFailsWithFatalReport() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).enableNewTermOperation().setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            store.failFlush();
            storeRef.set(store);
            return store;
        }).build();
        RaftNodeImpl node = group.getNodes().get(0);

        CompletableFuture<Ordered<Object>> startFuture = node.start();

        assertThat(storeRef.get().awaitFailure()).isTrue();
        eventually(() -> assertThat(node.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        eventually(() -> assertThat(startFuture).isCompletedExceptionally(), ASSERT_TIMEOUT_SECONDS);
        eventually(() -> assertThat(terminalReports(node)).singleElement()
                .extracting(RaftNodeReport::getReason).isEqualTo(FATAL_FAILURE), ASSERT_TIMEOUT_SECONDS);
    }

    @Test(timeout = 300_000)
    public void whenLeaderLeavesGroupThenSameTermIsNotPersistedAgain() throws Exception {
        Map<RaftEndpoint, FailingRaftStore> stores = new ConcurrentHashMap<>();
        group = LocalRaftGroup.newBuilder(3).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            stores.put(endpoint, store);
            return store;
        }).start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        FailingRaftStore store = stores.get(leader.getLocalEndpoint());

        leader.replicate(applyValue("value")).join();
        store.failTermPersistence();
        leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0).join();

        assertThat(store.awaitTermination()).isTrue();
        assertThat(store.hasFailed()).isFalse();
        assertThat(terminalReports(leader)).singleElement().extracting(RaftNodeReport::getReason)
                .isEqualTo(STATUS_CHANGE);
    }

    @Test(timeout = 300_000)
    public void whenConflictingSuffixTruncationFailsThenNodeTerminatesWithFatalReport() throws Exception {
        Map<RaftEndpoint, FailingRaftStore> stores = new ConcurrentHashMap<>();
        group = LocalRaftGroup.newBuilder(3).setConfig(TEST_RAFT_CONFIG)
                .setRaftStoreFactory((endpoint, config) -> {
                    FailingRaftStore store = new FailingRaftStore();
                    stores.put(endpoint, store);
                    return store;
                }).start();
        RaftNodeImpl oldLeader = group.waitUntilLeaderElected();
        oldLeader.replicate(applyValue("initial")).join();
        eventually(() -> group.getNodes().forEach(node -> assertThat(node.state().commitIndex()).isEqualTo(1)));

        List<RaftNodeImpl> followers = group.getNodesExcept(oldLeader.getLocalEndpoint());
        group.splitMembers(oldLeader.getLocalEndpoint());
        oldLeader.replicate(applyValue("isolated-1"));
        oldLeader.replicate(applyValue("isolated-2"));
        eventually(() -> assertThat(oldLeader.state().log().lastLogOrSnapshotIndex()).isEqualTo(3));
        eventually(() -> assertThat(followers.get(0).getLeaderEndpoint()).isNotNull()
                .isNotEqualTo(oldLeader.getLocalEndpoint()));

        RaftNodeImpl newLeader = group.getNode(followers.get(0).getLeaderEndpoint());
        newLeader.replicate(applyValue("replacement-1")).join();
        newLeader.replicate(applyValue("replacement-2")).join();
        FailingRaftStore store = stores.get(oldLeader.getLocalEndpoint());
        store.failTruncateLogEntriesFrom();

        group.merge();

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(oldLeader.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThat(oldLeader.getReport().join().getResult().getReason()).isEqualTo(FATAL_FAILURE);
    }

    @Test(timeout = 300_000)
    public void whenFinalReceivedSnapshotChunkCannotBeFlushedThenNodeTerminatesWithoutInstallingSnapshot()
            throws Exception {
        Map<RaftEndpoint, FailingRaftStore> stores = new ConcurrentHashMap<>();
        group = LocalRaftGroup.newBuilder(2).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            stores.put(endpoint, store);
            return store;
        }).start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint()).get(0);
        FailingRaftStore store = stores.get(follower.getLocalEndpoint());
        long commitIndex = follower.state().commitIndex();
        InstallSnapshotRequest request = newReceivedSnapshotRequest(follower, leader);

        store.failFlush();
        follower.handle(request);

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(follower.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThat(follower.state().commitIndex()).isEqualTo(commitIndex);
        assertThat(follower.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult().getReason())
                .isEqualTo(FATAL_FAILURE);

        follower.handle(request);
        assertThat(follower.state().commitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void whenReceivedSnapshotChunkCannotBePersistedThenNodeTerminates() throws Exception {
        Map<RaftEndpoint, FailingRaftStore> stores = new ConcurrentHashMap<>();
        group = LocalRaftGroup.newBuilder(2).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            stores.put(endpoint, store);
            return store;
        }).start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint()).get(0);
        FailingRaftStore store = stores.get(follower.getLocalEndpoint());
        long commitIndex = follower.state().commitIndex();
        InstallSnapshotRequest request = newReceivedSnapshotRequest(follower, leader);

        store.failSnapshotPersistence();
        follower.handle(request);

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> assertThat(follower.getStatus()).isEqualTo(TERMINATED), ASSERT_TIMEOUT_SECONDS);
        assertThat(follower.state().commitIndex()).isEqualTo(commitIndex);
        assertThat(follower.getReport().join().getResult().getReason()).isEqualTo(FATAL_FAILURE);
    }

    @Test(timeout = 300_000)
    public void whenLogCompactionCleanupFailsThenSnapshotSucceedsAndNodeRemainsActive() throws Exception {
        AtomicReference<FailingRaftStore> storeRef = new AtomicReference<>();
        group = LocalRaftGroup.newBuilder(1).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            storeRef.set(store);
            return store;
        }).start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        node.replicate(applyValue("value-1")).join();
        FailingRaftStore store = storeRef.get();

        store.failTruncateLogEntriesUntil();
        RaftNodeReport report = node.takeSnapshot().get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult();

        assertThat(store.awaitFailure()).isTrue();
        assertThat(node.getStatus()).isEqualTo(ACTIVE);
        assertThat(report.getLog().getLastSnapshotIndex()).isEqualTo(1);
        node.replicate(applyValue("value-2")).join();
    }

    @Test(timeout = 300_000)
    public void whenReceivedSnapshotLogCleanupFailsThenSnapshotIsInstalledAndNodeRemainsActive() throws Exception {
        Map<RaftEndpoint, FailingRaftStore> stores = new ConcurrentHashMap<>();
        group = LocalRaftGroup.newBuilder(2).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            stores.put(endpoint, store);
            return store;
        }).start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint()).get(0);
        FailingRaftStore store = stores.get(follower.getLocalEndpoint());
        InstallSnapshotRequest request = newReceivedSnapshotRequest(follower, leader);

        store.failTruncateLogEntriesUntil();
        follower.handle(request);

        assertThat(store.awaitFailure()).isTrue();
        eventually(() -> {
            assertThat(follower.state().commitIndex()).isEqualTo(request.getSnapshotIndex());
            assertThat(follower.state().log().snapshotIndex()).isEqualTo(request.getSnapshotIndex());
            assertThat(follower.state().snapshotChunkCollector()).isNull();
        }, ASSERT_TIMEOUT_SECONDS);
        assertThat(follower.getStatus()).isEqualTo(ACTIVE);
        assertThat(terminalReports(follower)).isEmpty();
    }

    @Test(timeout = 300_000)
    public void whenSnapshotChunkDeletionFailsThenNewerSnapshotCollectionContinues() throws Exception {
        Map<RaftEndpoint, FailingRaftStore> stores = new ConcurrentHashMap<>();
        group = LocalRaftGroup.newBuilder(2).setRaftStoreFactory((endpoint, config) -> {
            FailingRaftStore store = new FailingRaftStore();
            stores.put(endpoint, store);
            return store;
        }).start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint()).get(0);
        FailingRaftStore store = stores.get(follower.getLocalEndpoint());
        int term = follower.state().term();
        long snapshotIndex = follower.state().commitIndex() + SNAPSHOT_INDEX_OFFSET;
        long newerSnapshotIndex = snapshotIndex + SNAPSHOT_INDEX_OFFSET;
        RaftGroupMembersView membersView = follower.state().committedGroupMembers()
                .populate(follower.getModelFactory().createRaftGroupMembersViewBuilder());
        SnapshotChunk chunk = follower.getModelFactory().createSnapshotChunkBuilder().setTerm(term)
                .setIndex(snapshotIndex).setOperation(Map.of()).setSnapshotChunkIndex(0).setSnapshotChunkCount(2)
                .setGroupMembersView(membersView).build();
        InstallSnapshotRequest firstRequest = newInstallSnapshotRequest(follower, leader.getLocalEndpoint(), term,
                snapshotIndex, 2, chunk, membersView);
        follower.handle(firstRequest);
        eventually(() -> {
            assertThat(follower.state().snapshotChunkCollector()).isNotNull();
            assertThat(follower.state().snapshotChunkCollector().getChunks()).hasSize(1);
        }, ASSERT_TIMEOUT_SECONDS);

        store.failDeleteSnapshotChunks();
        InstallSnapshotRequest newerRequest = newInstallSnapshotRequest(follower, leader.getLocalEndpoint(), term,
                newerSnapshotIndex, 2, null, membersView);
        follower.handle(newerRequest);

        assertThat(store.awaitFailure()).isTrue();
        assertThat(follower.getStatus()).isEqualTo(ACTIVE);
        eventually(() -> {
            assertThat(follower.state().snapshotChunkCollector()).isNotNull();
            assertThat(follower.state().snapshotChunkCollector().getSnapshotIndex()).isEqualTo(newerSnapshotIndex);
        }, ASSERT_TIMEOUT_SECONDS);
    }

    private InstallSnapshotRequest newInstallSnapshotRequest(RaftNodeImpl follower, RaftEndpoint leader, int term,
            long snapshotIndex, int chunkCount, SnapshotChunk chunk, RaftGroupMembersView membersView) {
        return follower.getModelFactory().createInstallSnapshotRequestBuilder().setGroupId(follower.getGroupId())
                .setSender(leader).setTerm(term).setSenderLeader(true).setSnapshotTerm(term)
                .setSnapshotIndex(snapshotIndex).setTotalSnapshotChunkCount(chunkCount).setSnapshotChunk(chunk)
                .setSnapshottedMembers(List.of(leader)).setGroupMembersView(membersView).setQuerySequenceNumber(0)
                .setFlowControlSequenceNumber(0).build();
    }

    private InstallSnapshotRequest newReceivedSnapshotRequest(RaftNodeImpl follower, RaftNodeImpl leader) {
        long snapshotIndex = follower.state().commitIndex() + SNAPSHOT_INDEX_OFFSET;
        int term = follower.state().term();
        RaftGroupMembersView membersView = follower.state().committedGroupMembers()
                .populate(follower.getModelFactory().createRaftGroupMembersViewBuilder());
        SnapshotChunk chunk = follower.getModelFactory().createSnapshotChunkBuilder().setTerm(term)
                .setIndex(snapshotIndex).setOperation(Map.of()).setSnapshotChunkIndex(0).setSnapshotChunkCount(1)
                .setGroupMembersView(membersView).build();
        return newInstallSnapshotRequest(follower, leader.getLocalEndpoint(), term, snapshotIndex, 1, chunk,
                membersView);
    }

    private List<RaftNodeReport> terminalReports(RaftNodeImpl node) {
        return group.getRaftNodeReports(node.getLocalEndpoint()).stream()
                .filter(report -> report.getStatus() == TERMINATED).collect(toList());
    }

    private static final class FailingRaftStore implements RaftStore, RaftNodeLifecycleAware {

        private final AtomicBoolean failPersistLogEntries = new AtomicBoolean();
        private final AtomicBoolean failPersistLogEntriesUnchecked = new AtomicBoolean();
        private final AtomicBoolean failFlush = new AtomicBoolean();
        private final AtomicBoolean failDeleteSnapshotChunks = new AtomicBoolean();
        private final AtomicBoolean failInitialGroupPersistence = new AtomicBoolean();
        private final AtomicBoolean failInitialStatePersistence = new AtomicBoolean();
        private final AtomicBoolean failLocalEndpointPersistence = new AtomicBoolean();
        private final AtomicBoolean failSnapshotPersistence = new AtomicBoolean();
        private final AtomicBoolean failTermPersistence = new AtomicBoolean();
        private final AtomicBoolean failTruncateLogEntriesFrom = new AtomicBoolean();
        private final AtomicBoolean failTruncateLogEntriesUntil = new AtomicBoolean();
        private final AtomicInteger failedPersistLogEntryCount = new AtomicInteger();
        private final CountDownLatch failure = new CountDownLatch(1);
        private final CountDownLatch termination = new CountDownLatch(1);

        void failPersistLogEntries() {
            failPersistLogEntries.set(true);
        }

        void failPersistLogEntriesUnchecked() {
            failPersistLogEntriesUnchecked.set(true);
        }

        void failFlush() {
            failFlush.set(true);
        }

        void failDeleteSnapshotChunks() {
            failDeleteSnapshotChunks.set(true);
        }

        void failInitialStatePersistence() {
            failInitialStatePersistence.set(true);
        }

        void failInitialGroupPersistence() {
            failInitialGroupPersistence.set(true);
        }

        void failLocalEndpointPersistence() {
            failLocalEndpointPersistence.set(true);
        }

        void failSnapshotPersistence() {
            failSnapshotPersistence.set(true);
        }

        void failTermPersistence() {
            failTermPersistence.set(true);
        }

        void failTruncateLogEntriesFrom() {
            failTruncateLogEntriesFrom.set(true);
        }

        void failTruncateLogEntriesUntil() {
            failTruncateLogEntriesUntil.set(true);
        }

        boolean awaitFailure() throws InterruptedException {
            return failure.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        boolean awaitTermination() throws InterruptedException {
            return termination.await(ASSERT_TIMEOUT_SECONDS, SECONDS);
        }

        boolean hasFailed() {
            return failure.getCount() == 0;
        }

        int failedPersistLogEntryCount() {
            return failedPersistLogEntryCount.get();
        }

        @Override
        public void onRaftNodeTerminate() {
            termination.countDown();
        }

        @Override
        public void persistAndFlushLocalEndpoint(@Nonnull RaftEndpointPersistentState localEndpointPersistentState)
                throws IOException {
            if (failInitialStatePersistence.get()) {
                failure.countDown();
                throw new IOException("injected initial state persistence failure");
            }
            if (failLocalEndpointPersistence.get()) {
                failure.countDown();
                throw new IOException("injected local endpoint persistence failure");
            }
        }

        @Override
        public void persistAndFlushInitialGroupMembers(@Nonnull RaftGroupMembersView initialGroupMembers)
                throws IOException {
            if (failInitialGroupPersistence.get()) {
                failure.countDown();
                throw new IOException("injected initial group persistence failure");
            }
        }

        @Override
        public void persistAndFlushTerm(@Nonnull RaftTermPersistentState termPersistentState) throws IOException {
            if (failTermPersistence.get()) {
                failure.countDown();
                throw new IOException("injected term persistence failure");
            }
        }

        @Override
        public void persistLogEntries(@Nonnull List<LogEntry> logEntries) throws IOException {
            if (failPersistLogEntriesUnchecked.get()) {
                failedPersistLogEntryCount.set(logEntries.size());
                failure.countDown();
                throw new IllegalStateException("injected unchecked persistLogEntries failure");
            }
            if (failPersistLogEntries.get()) {
                failedPersistLogEntryCount.set(logEntries.size());
                failure.countDown();
                throw new IOException("injected persistLogEntries failure");
            }
        }

        @Override
        public void persistSnapshotChunk(@Nonnull SnapshotChunk snapshotChunk) throws IOException {
            if (failSnapshotPersistence.get()) {
                failure.countDown();
                throw new IOException("injected snapshot persistence failure");
            }
        }

        @Override
        public void truncateLogEntriesFrom(long logIndexInclusive) throws IOException {
            if (failTruncateLogEntriesFrom.get()) {
                failure.countDown();
                throw new IOException("injected suffix truncation failure");
            }
        }

        @Override
        public void truncateLogEntriesUntil(long logIndexInclusive) throws IOException {
            if (failTruncateLogEntriesUntil.get()) {
                failure.countDown();
                throw new IOException("injected log compaction cleanup failure");
            }
        }

        @Override
        public void deleteSnapshotChunks(long logIndex, int snapshotChunkCount) throws IOException {
            if (failDeleteSnapshotChunks.get()) {
                failure.countDown();
                throw new IOException("injected snapshot chunk deletion failure");
            }
        }

        @Override
        public void flush() throws IOException {
            if (failFlush.get()) {
                failure.countDown();
                throw new IOException("injected flush failure");
            }
        }
    }

}
