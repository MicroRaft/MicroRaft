package com.hazelcast.raft.impl.msg;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftMsg;
import com.hazelcast.raft.impl.handler.AppendEntriesRequestHandler;

/**
 * Response for a successful {@link AppendEntriesRequest}.
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
public class AppendEntriesSuccessResponse
        implements RaftMsg {

    private static final long serialVersionUID = -1676469490714477695L;

    private RaftEndpoint follower;
    private int term;
    private long lastLogIndex;
    private long queryRound;

    public AppendEntriesSuccessResponse() {
    }

    public AppendEntriesSuccessResponse(RaftEndpoint follower, int term, long lastLogIndex, long queryRound) {
        this.follower = follower;
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.queryRound = queryRound;
    }

    public RaftEndpoint follower() {
        return follower;
    }

    public int term() {
        return term;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    public long queryRound() {
        return queryRound;
    }

    @Override
    public String toString() {
        return "AppendSuccessResponse{" + "follower=" + follower + ", term=" + term + ", lastLogIndex=" + lastLogIndex
                + ", queryRound=" + queryRound + '}';
    }

}
