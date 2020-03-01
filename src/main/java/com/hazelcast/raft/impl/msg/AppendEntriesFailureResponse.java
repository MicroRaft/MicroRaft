package com.hazelcast.raft.impl.msg;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftMsg;
import com.hazelcast.raft.impl.handler.AppendEntriesRequestHandler;

/**
 * Response for a failed {@link AppendEntriesRequest}.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @author mdogan
 * @author metanet
 * @see AppendEntriesRequest
 * @see AppendEntriesRequestHandler
 */
public class AppendEntriesFailureResponse
        implements RaftMsg {

    private static final long serialVersionUID = 6474500004225054055L;

    private RaftEndpoint follower;
    private int term;
    private long expectedNextIndex;

    public AppendEntriesFailureResponse() {
    }

    public AppendEntriesFailureResponse(RaftEndpoint follower, int term, long expectedNextIndex) {
        this.follower = follower;
        this.term = term;
        this.expectedNextIndex = expectedNextIndex;
    }

    public RaftEndpoint follower() {
        return follower;
    }

    public int term() {
        return term;
    }

    public long expectedNextIndex() {
        return expectedNextIndex;
    }

    @Override
    public String toString() {
        return "AppendFailureResponse{" + "follower=" + follower + ", term=" + term + ", expectedNextIndex=" + expectedNextIndex
                + '}';
    }

}
