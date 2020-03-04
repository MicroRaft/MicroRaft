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

import static java.lang.Math.min;

/**
 * State maintained for each follower by the Raft group leader.
 *
 * @author mdogan
 * @author metanet
 */
public class FollowerState {

    // TODO [basri] make this configurable
    private static final int MAX_BACKOFF_ROUND = 20;

    /**
     * index of highest log entry known to be replicated
     * on server (initialized to 0, increases monotonically)
     */
    private long matchIndex;

    /**
     * index of the next log entry to send to that server
     * (initialized to leader's {@code lastLogIndex + 1})
     */
    private long nextIndex;

    /**
     * denotes the current round in the ongoing backoff period
     */
    private int backoffRound;

    /**
     * used for calculating how many rounds will be used
     * in the next backoff period
     */
    private int nextBackoffPower;

    /**
     * the timestamp of the last append entries response
     */
    private long responseTimestamp;

    FollowerState(long matchIndex, long nextIndex) {
        this.matchIndex = matchIndex;
        this.nextIndex = nextIndex;
        this.responseTimestamp = System.currentTimeMillis();
    }

    /**
     * Returns the match index for follower.
     */
    public long matchIndex() {
        return matchIndex;
    }

    /**
     * Sets the match index for follower.
     */
    public void matchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    /**
     * Returns the next index for follower.
     */
    public long nextIndex() {
        return nextIndex;
    }

    /**
     * Sets the next index for follower.
     */
    public void nextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    /**
     * Returns whether leader is waiting for response
     * of the last append entries request.
     */
    public boolean isRequestBackoffSet() {
        return backoffRound > 0;
    }

    /**
     * Starts a new append entries request backoff period.
     * No new append entries request will be sent to this follower
     * either until it sends a response or the backoff period times out.
     */
    public void setRequestBackoff() {
        backoffRound = nextBackoffRound();
        nextBackoffPower++;
    }

    private int nextBackoffRound() {
        return min(1 << nextBackoffPower, MAX_BACKOFF_ROUND);
    }

    /**
     * Enables the longest append entries request backoff period.
     */
    public void setMaxRequestBackoff() {
        backoffRound = MAX_BACKOFF_ROUND;
    }

    /**
     * Completes a single round of the append entries request backoff period.
     *
     * @return true if the current backoff period is completed, false otherwise
     */
    public boolean completeBackoffRound() {
        return --backoffRound == 0;
    }

    /**
     * Clears the flag for the append entries request backoff period
     * and updates the timestamp of append entries response.
     */
    public void responseReceived() {
        backoffRound = 0;
        nextBackoffPower = 0;
        responseTimestamp = Math.max(responseTimestamp, System.currentTimeMillis());
    }

    /**
     * Returns the timestamp of the last append entries response.
     */
    public long responseTimestamp() {
        return responseTimestamp;
    }

    @Override
    public String toString() {
        return "FollowerState{" + "matchIndex=" + matchIndex + ", nextIndex=" + nextIndex + ", backoffRound=" + backoffRound
                + ", nextBackoffPower=" + nextBackoffPower + ", responseTimestamp=" + responseTimestamp + '}';
    }

}
