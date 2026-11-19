/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright (c) 2020, MicroRaft.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.microraft.RaftEndpoint;
import io.microraft.impl.statemachine.NoOp;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.statemachine.StateMachine;


/**
 * This class is used to keep query operations until a heartbeat round is
 * completed. These query operations are executed with the linearizability
 * guarantee without growing the Raft log.
 * <p>
 * Section 6.4 of the Raft Dissertation: ... Linearizability requires the
 * results of a read to reflect a state of the system sometime after the read
 * was initiated; each read must at least return the results of the latest
 * committed write. ... Fortunately, it is possible to bypass the Raft log for
 * read-only queries and still preserve linearizability.
 */
public final class QueryState {
    /**
     * Queries currently ready for execution. We call this the live batch.
     */
    private final QueryBatch liveQueryBatch = new QueryBatch(new ArrayList<>(), 0);
    /**
     * Queries not ready for execution. Pending the completion of the previous batch.
     * These are queued but the execution is not started yet.
     */
    private final QueryBatch waitingQueryBatch = new QueryBatch(new ArrayList<>(), 0);
    /**
     * The set of followers acknowledged the leader in the current query round.
     */
    private final Set<RaftEndpoint> acks = new HashSet<>();
    /**
     * The index of the heartbeat round to execute the currently waiting queries.
     * When a query is received and there is no other query waiting to be executed,
     * a new heartbeat round is started by incrementing this field.
     * <p>
     * Value of this field is put into AppendEntriesRPCs sent to followers and
     * bounced back to the leader to complete the heartbeat round and execute the
     * queries.
     */
    private long querySequenceNumber;
    /**
     * Denotes if the current query batch is executing or not.
     */
    private boolean isBatchExecuting;

    /**
     * Adds the given query to the collection of pending queries.
     * Also updates the minimum commit index that is expected on the leader to execute the queries.
     */
    public void addQuery(long commitIndex, Object query, OrderedFuture resultFuture) {
        waitingQueryBatch.addQuery(commitIndex, query, resultFuture);
    }

    /**
     * Any caller that wants to execute the queries must call this method first.
     * If true, the caller must start the execution of the queries.
     */
    public boolean executeQueryIfNotRunning() {
        if (isBatchExecuting) {
            return false;
        }

        // If the current batch is empty, we can start executing the next batch.
        if (liveQueryBatch.isEmpty()) {
            liveQueryBatch.copy(waitingQueryBatch);
            waitingQueryBatch.clear();
        }

        if (liveQueryBatch.isEmpty()) {
            return false;
        }

        querySequenceNumber++;
        isBatchExecuting = true;
        return true;
    }

    /**
     * Returns {@code true} if the given follower's ack is accepted for the current
     * query round. It is accepted only if there are waiting queries to be executed
     * and the {@code querySequenceNumber} argument matches to the current query
     * round.
     */
    public boolean tryAck(long querySequenceNumber, RaftEndpoint follower) {
        // If there is no query waiting to be executed or the received ack
        // belongs to an earlier query, we ignore it.
        if (liveQueryBatch.isEmpty() || this.querySequenceNumber > querySequenceNumber) {
            return false;
        }

        if (querySequenceNumber != this.querySequenceNumber) {
            throw new IllegalStateException(
                    this + ", acked query sequence number: " + querySequenceNumber + ", follower: " + follower);
        }

        return acks.add(follower);
    }

    /**
     * Returns {@code true} if the given follower is removed from the ack list.
     */
    public boolean removeAck(RaftEndpoint follower) {
        return acks.remove(follower);
    }

    /**
     * Returns the index of the heartbeat round to execute the currently waiting
     * queries.
     */
    public long querySequenceNumber() {
        return querySequenceNumber;
    }

    /**
     * Returns {@code true} if there are queries waiting and acks are received from
     * the log replication quorum.
     * <p>
     * Fails with {@link IllegalStateException} if the given commit index is smaller
     * than the live batch's read index.
     */
    public boolean isQuorumAckReceived(long commitIndex, int quorumSize) {
        if (liveQueryBatch.readIndex > commitIndex) {
            throw new IllegalStateException("Cannot execute: " + this + ", current commit index: " + commitIndex);
        }

        return !liveQueryBatch.isEmpty() && quorumSize <= ackCount();
    }

    /**
     * Returns the number of collected acks for the current query round.
     */
    private int ackCount() {
        // +1 is for the leader itself.
        return acks.size() + 1;
    }

    /**
     * Returns {@code true} if more acks are needed to complete the given quorum
     * size.
     */
    public boolean isAckNeeded(RaftEndpoint follower, int quorumSize) {
        return liveQueriesCount() > 0 && !acks.contains(follower) && ackCount() < quorumSize;
    }

    /**
     * Returns the number of live queries.
     */
    public int liveQueriesCount() {
        return liveQueryBatch.queries.size();
    }

    /**
     * Returns the number of queries waiting for execution.
     */
    public int waitingQueriesCount() {
        return waitingQueryBatch.queries.size();
    }

    /**
     * Returns the live queries which are ready for execution.
     * Once the queries are executed, completeLiveBatch must be called to mark the batch as complete.
     */
    public Collection<QueryContainer> liveQueries() {
        return liveQueryBatch.queries;
    }

    /**
     * Returns the minimum log index that must be applied to the state machine
     * before the live queries can run.
     */
    public long liveQueriesReadIndex() {
        return liveQueryBatch.readIndex;
    }

    /**
     * Fails the live query futures with the given throwable and completes the
     * live batch.
     */
    public void failLiveQueries(Throwable t) {
        for (QueryContainer query : liveQueryBatch.queries) {
            query.fail(t);
        }

        completeLiveBatch();
    }

    /**
     * Fails the live and waiting query futures with the given throwable.
     * This method is called when the leader steps down.
     */
    public void fail(Throwable t) {
        for (QueryContainer query : liveQueryBatch.queries) {
            query.fail(t);
        }

        for (QueryContainer query : waitingQueryBatch.queries) {
            query.fail(t);
        }

        completeLiveBatch();
        waitingQueryBatch.clear();
    }

    /**
     * Marks the execution of the live batch of queries as complete
     */
    public void completeLiveBatch() {
        isBatchExecuting = false;
        liveQueryBatch.clear();
        acks.clear();
    }

    @Override
    public String toString() {
        return "QueryState{" + "liveQueryBatch=" + liveQueryBatch + ", waitingQueryBatch=" + waitingQueryBatch
                + ", querySequenceNumber=" + querySequenceNumber + ", isBatchExecuting=" + isBatchExecuting
                + ", acks=" + acks + '}';
    }

    public static class QueryContainer {
        final Object operation;
        final OrderedFuture future;

        public QueryContainer(Object operation, OrderedFuture future) {
            this.operation = operation;
            this.future = future;
        }

        public void run(long commitIndex, StateMachine stateMachine) {
            try {
                Object result = null;
                if (!(operation instanceof NoOp)) {
                    result = stateMachine.runOperation(commitIndex, operation);
                }
                future.complete(commitIndex, result);
            } catch (Throwable t) {
                fail(t);
            }
        }

        public void fail(Throwable t) {
            future.fail(t);
        }
    }

    private static final class QueryBatch {
        /**
         * List of queries in the batch.
         */
        private final List<QueryContainer> queries;
        /**
         * The minimum log index required to be committed and applied on the leader to
         * execute the queries.
         * The read index is always increasing across copy and clear operations.
         */
        private long readIndex;

        private QueryBatch(List<QueryContainer> queries, long readIndex) {
            this.queries = queries;
            this.readIndex = readIndex;
        }

        public void addQuery(long commitIndex, Object query, OrderedFuture resultFuture) {
            if (commitIndex < readIndex) {
                throw new IllegalArgumentException(
                        "Cannot execute query: " + query + " at commit index because of the current " + this);
            }

            queries.add(new QueryContainer(query, resultFuture));
            readIndex = Math.max(readIndex, commitIndex);
        }

        public boolean isEmpty() {
            return queries.isEmpty();
        }

        public void copy(QueryBatch other) {
            queries.addAll(other.queries);
            readIndex = Math.max(readIndex, other.readIndex);
        }

        public void clear() {
            queries.clear();
        }

        @Override
        public String toString() {
            return "QueryBatch{" + "size=" + queries.size() + ", readIndex=" + readIndex + '}';
        }
    }

}
