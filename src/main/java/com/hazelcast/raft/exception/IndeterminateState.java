package com.hazelcast.raft.exception;

/**
 * Marker interface for exceptions that imply an appended operation's commit
 * status is not known (i.e., it is not known of the operation is executed
 * or not) and therefore a retry can cause a duplicate commit.
 * In this case, it is up to the caller to decide on retry.
 *
 * @author mdogan
 * @author metanet
 */
public interface IndeterminateState {
}
