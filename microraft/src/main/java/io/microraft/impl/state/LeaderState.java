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

package io.microraft.impl.state;

import io.microraft.RaftEndpoint;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * State maintained by the Raft group leader.
 *
 * @see FollowerState
 */
public final class LeaderState {

    /**
     * A {@link FollowerState} object will be maintained for each follower.
     */
    private final Map<RaftEndpoint, FollowerState> followerStates = new HashMap<>();

    /**
     * Contains inflight queries that are waiting to be executed without
     * appending entries to the Raft log.
     */
    private final QueryState queryState = new QueryState();

    private boolean requestBackoffResetTaskScheduled;

    private boolean flushTaskSubmitted;

    private long flushedLogIndex;

    LeaderState(Collection<RaftEndpoint> remoteMembers, long lastLogIndex) {
        remoteMembers.forEach(follower -> followerStates.put(follower, new FollowerState(0L, lastLogIndex + 1)));
        flushedLogIndex = lastLogIndex;
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
     * Returns the query sequence number to be acked by the majority to execute
     * the currently waiting queries.
     */
    public long querySeqNo() {
        return queryState.querySeqNo();
    }

    public boolean isRequestBackoffResetTaskScheduled() {
        return requestBackoffResetTaskScheduled;
    }

    public void requestBackoffResetTaskScheduled(boolean backoffResetTaskScheduled) {
        this.requestBackoffResetTaskScheduled = backoffResetTaskScheduled;
    }

    public boolean isFlushTaskSubmitted() {
        return flushTaskSubmitted;
    }

    public void flushTaskSubmitted(boolean flushTaskSubmitted) {
        this.flushTaskSubmitted = flushTaskSubmitted;
    }

    public void flushedLogIndex(long flushedLogIndex) {
        assert flushedLogIndex > this.flushedLogIndex;
        this.flushedLogIndex = flushedLogIndex;
    }

    public long flushedLogIndex() {
        return flushedLogIndex;
    }

    /**
     * Returns the earliest append entries response timestamp of the majority nodes.
     */
    public long majorityAppendEntriesResponseTimestamp(int majority) {
        long[] timestamps = new long[followerStates.size() + 1];
        int i = 0;
        // for the local RaftNode
        timestamps[i] = Long.MAX_VALUE;
        for (FollowerState followerState : followerStates.values()) {
            timestamps[++i] = followerState.responseTimestamp();
        }

        Arrays.sort(timestamps);

        return timestamps[timestamps.length - majority];
    }

}
