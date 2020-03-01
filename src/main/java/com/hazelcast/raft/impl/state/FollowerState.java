package com.hazelcast.raft.impl.state;

import static java.lang.Math.min;

/**
 * State maintained for each follower by the Raft group leader.
 *
 * @author mdogan
 * @author metanet
 */
public class FollowerState {

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

    FollowerState(long matchIndex, long nextIndex) {
        this.matchIndex = matchIndex;
        this.nextIndex = nextIndex;
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
    public boolean isAppendEntriesRequestBackoffSet() {
        return backoffRound > 0;
    }

    /**
     * Starts a new append entries request backoff period.
     * No new append entries request will be sent to this follower
     * either until it sends a response or the backoff period times out.
     */
    public void setAppendEntriesRequestBackoff() {
        backoffRound = nextBackoffRound();
        nextBackoffPower++;
    }

    /**
     * Enables the longest append entries request backoff period.
     */
    public void setMaxAppendEntriesRequestBackoff() {
        backoffRound = MAX_BACKOFF_ROUND;
    }

    /**
     * Completes a single round of the append entries request backoff period.
     *
     * @return true if the current backoff period is completed, false otherwise
     */
    public boolean completeAppendEntriesRequestBackoffRound() {
        return --backoffRound == 0;
    }

    /**
     * Completes the current backoff period.
     */
    public void resetAppendEntriesRequestBackoff() {
        backoffRound = 0;
        nextBackoffPower = 0;
    }

    private int nextBackoffRound() {
        return min(1 << nextBackoffPower, MAX_BACKOFF_ROUND);
    }

    @Override
    public String toString() {
        return "FollowerState{" + "matchIndex=" + matchIndex + ", nextIndex=" + nextIndex + ", backoffRound=" + backoffRound
                + ", nextBackoffRound=" + nextBackoffRound() + '}';
    }
}
