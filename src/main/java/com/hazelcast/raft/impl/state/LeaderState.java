package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.RaftEndpoint;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * State maintained by the Raft group leader.
 *
 * @author mdogan
 * @author metanet
 * @see FollowerState
 */
public class LeaderState {

    /**
     * A {@link FollowerState} object will be maintained for each follower.
     */
    private final Map<RaftEndpoint, FollowerState> followerStates = new HashMap<>();

    /**
     * Contains inflight queries that are waiting to be executed without
     * appending entries to the Raft log.
     */
    private final QueryState queryState = new QueryState();

    LeaderState(Collection<RaftEndpoint> remoteMembers, long lastLogIndex) {
        remoteMembers.forEach(follower -> followerStates.put(follower, new FollowerState(0L, lastLogIndex + 1)));
    }

    /**
     * Add a new follower with the leader's {@code lastLogIndex}.
     * Follower's {@code nextIndex} will be set to {@code lastLogIndex + 1}
     * and {@code matchIndex} to 0.
     */
    public void add(RaftEndpoint follower, long lastLogIndex) {
        assert !followerStates.containsKey(follower) : "Already known follower " + follower;
        followerStates.put(follower, new FollowerState(0L, lastLogIndex + 1));
    }

    /**
     * Removes a follower from leader maintained state.
     */
    public void remove(RaftEndpoint follower) {
        FollowerState removed = followerStates.remove(follower);
        queryState.removeAck(follower);
        assert removed != null : "Unknown follower " + follower;
    }

    /**
     * Returns an array of match indices for all followers.
     * Additionally an empty slot is added at the end of indices array for leader itself.
     */
    public long[] matchIndices() {
        // Leader index is appended at the end of array in AppendSuccessResponseHandlerTask
        // That's why we add one more empty slot.
        long[] indices = new long[followerStates.size() + 1];
        int ix = 0;
        for (FollowerState state : followerStates.values()) {
            indices[ix++] = state.matchIndex();
        }
        return indices;
    }

    /**
     * Returns a non-null follower state object for the given follower.
     */
    public FollowerState getFollowerState(RaftEndpoint follower) {
        FollowerState followerState = followerStates.get(follower);
        assert followerState != null : "Unknown follower " + follower;
        return followerState;
    }

    /**
     * Returns all follower state objects.
     */
    public Map<RaftEndpoint, FollowerState> getFollowerStates() {
        return followerStates;
    }

    /**
     * Returns the state object that contains inflight queries.
     */
    public QueryState queryState() {
        return queryState;
    }

    /**
     * Returns the index of the heartbeat round to execute the currently
     * waiting queries.
     */
    public long queryRound() {
        return queryState.queryRound();
    }
}
