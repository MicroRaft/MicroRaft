package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.util.InternallyCompletableFuture;

import java.util.function.Function;

import static java.lang.Math.max;

/**
 * State maintained by the Raft group leader during leadership transfer.
 *
 * @author mdogan
 * @author metanet
 */
public class LeadershipTransferState {

    private static final int RETRY_LIMIT = 5;

    private int term;
    private RaftEndpoint endpoint;
    private InternallyCompletableFuture<Object> resultFuture;
    private int tryCount;

    LeadershipTransferState(int term, RaftEndpoint endpoint, InternallyCompletableFuture<Object> resultFuture) {
        this.term = term;
        this.endpoint = endpoint;
        this.resultFuture = resultFuture;
    }

    /**
     * Returns the term in which this leadership transfer process is triggered.
     */
    public int term() {
        return term;
    }

    /**
     * Returns the endpoint that is supposed to be the new Raft group leader.
     */
    public RaftEndpoint endpoint() {
        return endpoint;
    }

    /**
     * Returns how many times the target endpoint is notified
     * for the leadership transfer.
     */
    public int tryCount() {
        return tryCount;
    }

    /**
     * Returns if we can retry leadership transfer on the target endpoint.
     */
    public boolean retry() {
        return tryCount++ < RETRY_LIMIT;
    }

    /**
     * Returns a duration in milliseconds to delay the next retry.
     */
    public long retryDelay(long leaderElectionTimeoutMs) {
        return max(1, leaderElectionTimeoutMs * 2 / RETRY_LIMIT);
    }

    /**
     * Completes the current leadership transfer process with the given result.
     */
    void complete(Object result) {
        if (result instanceof Throwable) {
            resultFuture.internalCompleteExceptionally((Throwable) result);
            return;
        }

        resultFuture.internalComplete(result);
    }

    /**
     * Attaches the given Future object to the current leadership transfer
     * process, if it aims the same target endpoint. Otherwise, the given
     * future object is notified with {@link IllegalStateException}.
     */
    void andThen(RaftEndpoint targetEndpoint, InternallyCompletableFuture<Object> otherFuture) {
        if (this.endpoint.equals(targetEndpoint)) {
            resultFuture.thenApply((Function<Object, Object>) otherFuture::internalComplete)
                        .exceptionally(otherFuture::internalCompleteExceptionally);
        } else {
            otherFuture.internalCompleteExceptionally(
                    new IllegalStateException("There is an ongoing leadership transfer process to " + endpoint));
        }
    }

}
