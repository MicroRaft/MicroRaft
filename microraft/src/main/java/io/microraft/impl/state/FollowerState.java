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
 */
public final class FollowerState {

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
     * the timestamp of the last append entries or install snapshot response
     */
    private long responseTimestamp;

    /**
     * the flow control sequence number sent to the follower in the last append
     * entries or install snapshot request
     */
    private long flowControlSeqNo;

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
     * Starts a new request backoff period.
     * No new append entries or install snapshot request will be sent to
     * this follower either until it sends a response or the backoff timeout
     * elapses.
     * <p>
     * If the "extraRound" is true and the backoff state is initialized with
     * only 1 round, one more backoff round is added.
     * <p>
     * Returns the flow control sequence number to be put into the append
     * entries or install snapshot request which is to be sent to the follower.
     */
    public long setRequestBackoff(int minRounds, int maxRounds) {
        assert backoffRound == 0 : "backoff round: " + backoffRound;
        backoffRound = min((1 << (nextBackoffPower++)) * minRounds, maxRounds);

        return ++flowControlSeqNo;
    }

    /**
     * Completes a single round of the request backoff period.
     *
     * @return true if the current backoff period is completed, false otherwise
     */
    public boolean completeBackoffRound() {
        assert backoffRound > 0;
        return --backoffRound == 0;
    }

    /**
     * Updates the timestamp of the last received append entries or install
     * snapshot response. In addition, if the received flow control sequence
     * number is equal to the last sent flow sequence number, the internal
     * request backoff state is also reset.
     */
    public boolean responseReceived(long flowControlSeqNo) {
        responseTimestamp = Math.max(responseTimestamp, System.currentTimeMillis());
        boolean success = this.flowControlSeqNo == flowControlSeqNo;
        if (success) {
            resetRequestBackoff();
        }

        return success;
    }

    /**
     * Clears the request backoff state.
     */
    public void resetRequestBackoff() {
        backoffRound = 0;
        nextBackoffPower = 0;
    }

    /**
     * Returns the timestamp of the last append entries response.
     */
    public long responseTimestamp() {
        return responseTimestamp;
    }

    int backoffRound() {
        return backoffRound;
    }

    long flowControlSeqNo() {
        return flowControlSeqNo;
    }

    @Override
    public String toString() {
        return "FollowerState{" + "matchIndex=" + matchIndex + ", nextIndex=" + nextIndex + ", backoffRound=" + backoffRound
                + ", nextBackoffPower=" + nextBackoffPower + ", responseTimestamp=" + responseTimestamp + ", flowControlSeqNo="
                + flowControlSeqNo + '}';
    }

}
