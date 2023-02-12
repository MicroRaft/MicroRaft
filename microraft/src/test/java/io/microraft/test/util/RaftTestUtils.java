/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
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

package io.microraft.test.util;

import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.function.Function;

import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.executor.impl.DefaultRaftNodeExecutor;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.local.InMemoryRaftStore;
import io.microraft.impl.log.SnapshotChunkCollector;
import io.microraft.impl.state.LeaderState;
import io.microraft.impl.state.RaftGroupMembersState;
import io.microraft.model.log.BaseLogEntry;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.report.RaftNodeReport;

public final class RaftTestUtils {

    public static final RaftConfig TEST_RAFT_CONFIG = RaftConfig.newBuilder().setLeaderElectionTimeoutMillis(2000)
            .setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(5).build();

    private RaftTestUtils() {
    }

    public static RaftRole getRole(RaftNodeImpl node) {
        return readRaftReport(node, RaftNodeReport::getRole);
    }

    private static <T> T readRaftReport(RaftNodeImpl node, Function<RaftNodeReport, T> query) {
        try {
            return query.apply(node.getReport().join().getResult());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static BaseLogEntry getLastLogOrSnapshotEntry(RaftNodeImpl node) {
        return readRaftState(node, () -> node.state().log().lastLogOrSnapshotEntry());
    }

    public static <T> T readRaftState(RaftNodeImpl node, Callable<T> task) {
        try {

            RaftNodeExecutor executor = getExecutor(node);
            if (executor instanceof DefaultRaftNodeExecutor) {
                DefaultRaftNodeExecutor e = (DefaultRaftNodeExecutor) executor;
                if (e.getExecutor().isShutdown()) {
                    return task.call();
                }
            }

            FutureTask<T> futureTask = new FutureTask<>(task);
            executor.submit(futureTask);

            return futureTask.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static RaftNodeExecutor getExecutor(RaftNodeImpl node) throws NoSuchFieldException, IllegalAccessException {
        Field field = RaftNodeImpl.class.getDeclaredField("executor");
        field.setAccessible(true);
        return (RaftNodeExecutor) field.get(node);
    }

    public static SnapshotEntry getSnapshotEntry(RaftNodeImpl node) {
        return readRaftState(node, () -> node.state().log().snapshotEntry());
    }

    public static SnapshotChunkCollector getSnapshotChunkCollector(RaftNodeImpl node) {
        return readRaftState(node, () -> {
            SnapshotChunkCollector snapshotChunkCollector = node.state().snapshotChunkCollector();
            if (snapshotChunkCollector == null) {
                return null;
            }

            return snapshotChunkCollector.copy();
        });
    }

    public static long getCommitIndex(RaftNodeImpl node) {
        return readRaftReport(node, report -> report.getLog().getCommitIndex());
    }

    public static long getLastApplied(RaftNodeImpl node) {
        return readRaftState(node, () -> node.state().lastApplied());
    }

    public static int getTerm(RaftNodeImpl node) {
        return readRaftReport(node, report -> report.getTerm().getTerm());
    }

    public static RaftEndpoint getVotedEndpoint(RaftNodeImpl node) {
        return readRaftReport(node, report -> report.getTerm().getVotedEndpoint());
    }

    public static long getMatchIndex(RaftNodeImpl leader, RaftEndpoint follower) {
        Callable<Long> task = () -> {
            LeaderState leaderState = leader.state().leaderState();
            return leaderState.getFollowerState(follower).matchIndex();
        };

        return readRaftState(leader, task);
    }

    public static long getLeaderQuerySequenceNumber(RaftNodeImpl leader) {
        Callable<Long> task = () -> {
            LeaderState leaderState = leader.state().leaderState();
            assertNotNull(leader.getLocalEndpoint() + " has no leader state!", leaderState);
            return leaderState.querySequenceNumber();
        };

        return readRaftState(leader, task);
    }

    public static RaftNodeStatus getStatus(RaftNodeImpl node) {
        return readRaftReport(node, RaftNodeReport::getStatus);
    }

    public static RaftGroupMembersState getEffectiveGroupMembers(RaftNodeImpl node) {
        Callable<RaftGroupMembersState> task = () -> node.state().effectiveGroupMembers();
        return readRaftState(node, task);
    }

    public static RaftGroupMembersState getCommittedGroupMembers(RaftNodeImpl node) {
        Callable<RaftGroupMembersState> task = () -> node.state().committedGroupMembers();
        return readRaftState(node, task);
    }

    public static <T extends RaftStore> T getRaftStore(RaftNode node) {
        Callable<RaftStore> task = () -> ((RaftNodeImpl) node).state().store();
        return (T) readRaftState((RaftNodeImpl) node, task);
    }

    public static RestoredRaftState getRestoredState(RaftNode node) {
        Callable<RestoredRaftState> task = () -> {
            InMemoryRaftStore store = (InMemoryRaftStore) ((RaftNodeImpl) node).state().store();
            return store.toRestoredRaftState();
        };

        return readRaftState((RaftNodeImpl) node, task);
    }

    public static int minority(int count) {
        return count - majority(count);
    }

    public static int majority(int count) {
        return count / 2 + 1;
    }

}
