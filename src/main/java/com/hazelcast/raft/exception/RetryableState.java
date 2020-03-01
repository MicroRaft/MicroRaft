package com.hazelcast.raft.exception;

/**
 * Marker interface for exceptions that imply an operation is not committed
 * and therefore can be retried.
 *
 * @author mdogan
 * @author metanet
 */
public interface RetryableState {
}
