package com.hazelcast.raft.impl.util;

import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftGroupMembers;
import com.hazelcast.raft.RaftIntegration;
import com.hazelcast.raft.RaftNodeStatus;
import com.hazelcast.raft.RaftRole;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.local.LocalRaftGroup;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.state.LeaderState;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.raft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static org.junit.Assert.assertNotNull;

/**
 * @author mdogan
 * @author metanet
 */
public class RaftUtil {

    public static RaftRole getRole(RaftNodeImpl node) {
        Callable<RaftRole> task = () -> node.state().role();
        return readRaftState(node, task);
    }

    public static <T extends RaftEndpoint> T getLeaderMember(RaftNodeImpl node) {
        Callable<RaftEndpoint> task = () -> node.state().leader();
        return (T) readRaftState(node, task);
    }

    public static LogEntry getLastLogOrSnapshotEntry(RaftNodeImpl node) {
        Callable<LogEntry> task = () -> node.state().log().lastLogOrSnapshotEntry();

        return readRaftState(node, task);
    }

    public static LogEntry getSnapshotEntry(RaftNodeImpl node) {
        Callable<LogEntry> task = () -> node.state().log().snapshot();

        return readRaftState(node, task);
    }

    public static long getCommitIndex(RaftNodeImpl node) {
        Callable<Long> task = () -> node.state().commitIndex();

        return readRaftState(node, task);
    }

    public static int getTerm(RaftNodeImpl node) {
        Callable<Integer> task = () -> node.state().term();

        return readRaftState(node, task);
    }

    public static long getMatchIndex(RaftNodeImpl leader, RaftEndpoint follower) {
        Callable<Long> task = () -> {
            LeaderState leaderState = leader.state().leaderState();
            return leaderState.getFollowerState(follower).matchIndex();
        };

        return readRaftState(leader, task);
    }

    public static long getLeaderQueryRound(RaftNodeImpl leader) {
        Callable<Long> task = () -> {
            LeaderState leaderState = leader.state().leaderState();
            assertNotNull(leader.getLocalEndpoint() + " has no leader state!", leaderState);
            return leaderState.queryRound();
        };

        return readRaftState(leader, task);
    }

    public static RaftNodeStatus getStatus(RaftNodeImpl node) {
        Callable<RaftNodeStatus> task = node::getStatus;

        return readRaftState(node, task);
    }

    public static RaftGroupMembers getLastGroupMembers(RaftNodeImpl node) {
        Callable<RaftGroupMembers> task = () -> node.state().lastGroupMembers();

        return readRaftState(node, task);
    }

    public static RaftGroupMembers getCommittedGroupMembers(RaftNodeImpl node) {
        Callable<RaftGroupMembers> task = () -> node.state().committedGroupMembers();

        return readRaftState(node, task);
    }

    public static void waitUntilLeaderElected(RaftNodeImpl node) {
        AssertUtil.eventually(() -> assertNotNull(getLeaderMember(node)));
    }

    private static <T> T readRaftState(RaftNodeImpl node, Callable<T> task) {
        FutureTask<T> futureTask = new FutureTask<>(task);
        try {
            Field field = RaftNodeImpl.class.getDeclaredField("integration");
            field.setAccessible(true);
            RaftIntegration integration = (RaftIntegration) field.get(node);
            integration.execute(futureTask);
            return futureTask.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int majority(int count) {
        return count / 2 + 1;
    }

    public static int minority(int count) {
        return count - majority(count);
    }

    public static LocalRaftGroup newGroup(int nodeCount) {
        return newGroup(nodeCount, DEFAULT_RAFT_CONFIG, false);
    }

    public static LocalRaftGroup newGroup(int nodeCount, RaftConfig raftConfig) {
        return newGroup(nodeCount, raftConfig, false);
    }

    public static LocalRaftGroup newGroup(int nodeCount, RaftConfig raftConfig, boolean appendNopEntryOnLeaderElection) {
        return new LocalRaftGroup(nodeCount, raftConfig, appendNopEntryOnLeaderElection);
    }
}
