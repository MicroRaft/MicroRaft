package com.hazelcast.raft.exception;

/**
 * Thrown when a request is sent to a Raft node of a terminated Raft group.
 *
 * @author mdogan
 * @author metanet
 */
public class RaftGroupTerminatedException
        extends RaftException {

    private static final long serialVersionUID = -5363753263443789491L;

    public RaftGroupTerminatedException() {
        super(null);
    }

}
