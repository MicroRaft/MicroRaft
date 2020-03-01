package com.hazelcast.raft.impl.log;

import java.io.Serializable;

/**
 * Represents an entry in the {@link RaftLog}.
 * Each log entry stores an operation that will be executed on the state
 * machine along with the term number when the operation was received by
 * the leader. Term numbers are used to detect inconsistencies between logs.
 * Each log entry also has an integer index identifying its position
 * in the Raft log.
 *
 * @author mdogan
 * @author metanet
 */
public class LogEntry
        implements Serializable {

    private static final long serialVersionUID = -8159410085055850819L;

    private int term;
    private long index;
    private Object operation;

    public LogEntry() {
    }

    public LogEntry(int term, long index, Object operation) {
        this.term = term;
        this.index = index;
        this.operation = operation;
    }

    public long index() {
        return index;
    }

    public int term() {
        return term;
    }

    public Object operation() {
        return operation;
    }

    @Override
    public String toString() {
        return "LogEntry{" + "term=" + term + ", index=" + index + ", operation=" + operation + '}';
    }
}
