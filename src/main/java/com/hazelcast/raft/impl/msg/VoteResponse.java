package com.hazelcast.raft.impl.msg;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftMsg;

/**
 * Response for {@link VoteRequest}.
 * <p>
 * See <i>5.2 Leader election</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @author mdogan
 * @author metanet
 * @see VoteRequest
 */
public class VoteResponse
        implements RaftMsg {

    private static final long serialVersionUID = -8421085416867681688L;

    private RaftEndpoint voter;
    private int term;
    private boolean granted;

    public VoteResponse() {
    }

    public VoteResponse(RaftEndpoint voter, int term, boolean granted) {
        this.voter = voter;
        this.term = term;
        this.granted = granted;
    }

    public RaftEndpoint voter() {
        return voter;
    }

    public int term() {
        return term;
    }

    public boolean granted() {
        return granted;
    }

    @Override
    public String toString() {
        return "VoteResponse{" + "voter=" + voter + ", term=" + term + ", granted=" + granted + '}';
    }

}
