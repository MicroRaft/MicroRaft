package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.util.InternallyCompletableFuture;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This class is used to keep query operations until a heartbeat round is
 * completed. These query operations are not appended to the Raft log but
 * they still achieve linearizability.
 * <p>
 * Section 6.4 of the Raft Dissertation:
 * ...
 * Linearizability requires the results of a read to reflect a state of the
 * system sometime after the read was initiated; each read must at least return
 * the results of the latest committed write.
 * ...
 * Fortunately, it is possible to bypass the Raft log for read-only queries and
 * still preserve linearizability.
 *
 * @author mdogan
 * @author metanet
 */
public class QueryState {

    /**
     * The minimum commit index required on the leader to execute the queries.
     */
    private long queryCommitIndex;

    /**
     * The index of the heartbeat round to execute the currently waiting
     * queries. When a query is received and there is no other query waiting
     * to be executed, a new heartbeat round is started by incrementing this
     * field.
     * <p>
     * Value of this field is put into AppendEntriesRPCs sent to followers and
     * bounced back to the leader to complete the heartbeat round and execute
     * the queries.
     */
    private long queryRound;

    /**
     * Queries waiting to be executed.
     */
    private final List<Entry<Object, InternallyCompletableFuture<Object>>> operations = new ArrayList<>();

    /**
     * The set of followers acknowledged the leader in the current query round.
     */
    private final Set<Object> acks = new HashSet<>();

    /**
     * Adds the given query to the collection of queries and returns the number
     * of queries waiting to be executed. Also updates the minimum commit index
     * that is expected on the leader to execute the queries.
     */
    public int addQuery(long commitIndex, Object operation, InternallyCompletableFuture<Object> resultFuture) {
        if (commitIndex < queryCommitIndex) {
            throw new IllegalArgumentException(
                    "Cannot execute query: " + operation + " at commit index because of the current " + this);
        }

        if (queryCommitIndex < commitIndex) {
            queryCommitIndex = commitIndex;
        }

        operations.add(new SimpleImmutableEntry<>(operation, resultFuture));
        int size = operations.size();
        if (size == 1) {
            queryRound++;
        }

        return size;
    }

    /**
     * Returns {@code true} if the given follower's ack is accepted
     * for the current query round. It is accepted only if there are
     * waiting queries to be executed and the {@code queryRound} argument
     * matches to the current query round.
     */
    public boolean tryAck(long queryRound, Object follower) {
        // If there is no query waiting to be executed or the received ack
        // belongs to an earlier query round, we ignore it.
        if (operations.isEmpty() || this.queryRound > queryRound) {
            return false;
        }

        if (queryRound != this.queryRound) {
            throw new IllegalStateException(this + ", acked query round: " + queryRound + ", follower: " + follower);
        }

        return acks.add(follower);
    }

    /**
     * Returns {@code true} if the given follower is removed from the ack list.
     */
    public boolean removeAck(Object follower) {
        return acks.remove(follower);
    }

    /**
     * Returns the number of queries waiting for execution.
     */
    public int queryCount() {
        return operations.size();
    }

    /**
     * Returns the index of the heartbeat round to execute the currently
     * waiting queries.
     */
    public long queryRound() {
        return queryRound;
    }

    /**
     * Returns {@code true} if there are queries waiting and acks are received
     * from the majority. Fails with {@link IllegalStateException} if
     * the given commit index is smaller than {@link #queryCommitIndex}.
     */
    public boolean isMajorityAcked(long commitIndex, int majority) {
        if (queryCommitIndex > commitIndex) {
            throw new IllegalStateException("Cannot execute: " + this + ", current commit index: " + commitIndex);
        }

        return operations.size() > 0 && majority <= ackCount();
    }

    /**
     * Returns {@code true} if more acks are needed to achieve the given
     * majority.
     */
    public boolean isAckNeeded(Object follower, int majority) {
        return !acks.contains(follower) && ackCount() < majority;
    }

    /**
     * Returns the number of collected acks for the current query round.
     */
    private int ackCount() {
        return acks.size() + 1;
    }

    /**
     * Returns the queries waiting to be executed.
     */
    public Collection<Entry<Object, InternallyCompletableFuture<Object>>> operations() {
        return operations;
    }

    /**
     * Resets the collection of waiting queries and acks.
     */
    public void reset() {
        operations.clear();
        acks.clear();
    }

    @Override
    public String toString() {
        return "QueryState{" + "queryCommitIndex=" + queryCommitIndex + ", queryRound=" + queryRound + ", queryCount="
                + queryCount() + ", acks=" + acks + '}';
    }
}
