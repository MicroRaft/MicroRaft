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

    // TODO [basri] make this configurable???
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
    private int nextBackoffRoundPower;

    /**
     * the timestamp of the last append entries or install snapshot response
     */
    private long responseTimestamp;

    /**
     * the flow control sequence number sent to the follower in the last append
     * entries or install snapshot request
     */
    private long flowControlSeqNo;

    /**
     * the last flow control sequence number for which the request backoff
     * reset task is scheduled. this field may be behind
     * {@link #flowControlSeqNo} if the follower sends a response and another
     * request is sent to it before the request backoff task of the first
     * request runs.
     */
    private long resetBackOffTaskScheduledFlowControlSeqNo;

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
     * If the "twice" parameter is set and the backoff state is initialized
     * with only 1 round, one more backoff round is added.
     */
    public void setRequestBackoff(boolean twice) {
        backoffRound = nextRequestBackoffRound();
        if (twice && backoffRound == 1) {
            backoffRound++;
        }
        nextBackoffRoundPower++;
    }

    private int nextRequestBackoffRound() {
        return min(1 << nextBackoffRoundPower, MAX_BACKOFF_ROUND);
    }

    /**
     * Enables the longest request backoff period.
     */
    public void setMaxRequestBackoff() {
        backoffRound = MAX_BACKOFF_ROUND;
    }

    /**
     * Completes a single round of the request backoff period.
     *
     * @return true if the current backoff period is completed, false otherwise
     */
    public boolean completeBackoffRound() {
        assert resetBackOffTaskScheduledFlowControlSeqNo != 0;

        if (resetBackOffTaskScheduledFlowControlSeqNo != flowControlSeqNo) {
            // The leader has sent a new request after the last request sent
            // before the request backoff task is executed. In this case,
            // we cannot complete this current backoff round, and we should
            // go for one more backoff round.
            resetBackOffTaskScheduledFlowControlSeqNo = flowControlSeqNo;
            return false;
        } else if (--backoffRound > 0) {
            return false;
        }

        resetBackOffTaskScheduledFlowControlSeqNo = 0;
        return true;
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
        nextBackoffRoundPower = 0;
        resetBackOffTaskScheduledFlowControlSeqNo = 0;
    }

    /**
     * Returns the timestamp of the last append entries response.
     */
    public long responseTimestamp() {
        return responseTimestamp;
    }

    /**
     * Returns the next available flow control sequence number for the append
     * entries or install snapshot request about to be sent.
     */
    public long nextFlowControlSeqNo() {
        long nextFlowControlSeqNo = ++flowControlSeqNo;
        if (resetBackOffTaskScheduledFlowControlSeqNo == 0) {
            resetBackOffTaskScheduledFlowControlSeqNo = nextFlowControlSeqNo;
        }

        return nextFlowControlSeqNo;
    }

    @Override
    public String toString() {
        return "FollowerState{" + "matchIndex=" + matchIndex + ", nextIndex=" + nextIndex + ", backoffRound=" + backoffRound
                + ", nextBackoffPower=" + nextBackoffRoundPower + ", responseTimestamp=" + responseTimestamp
                + ", flowControlSeqNo=" + flowControlSeqNo + ", resetBackOffTaskScheduledFlowControlSeqNo="
                + resetBackOffTaskScheduledFlowControlSeqNo + '}';
    }

}
