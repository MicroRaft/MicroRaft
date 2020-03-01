package com.hazelcast.raft.impl.msg;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftMsg;

/**
 * Response for {@link PreVoteRequest}.
 * <p>
 * See <i>Four modifications for the Raft consensus algorithm</i>
 * by Henrik Ingo.
 *
 * @author mdogan
 * @author metanet
 * @see PreVoteRequest
 * @see VoteResponse
 */
public class PreVoteResponse
        implements RaftMsg {

    private static final long serialVersionUID = 4945993553410362596L;

    private RaftEndpoint voter;
    private int term;
    private boolean granted;

    public PreVoteResponse() {
    }

    public PreVoteResponse(RaftEndpoint voter, int term, boolean granted) {
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
        return "PreVoteResponse{" + "voter=" + voter + ", term=" + term + ", granted=" + granted + '}';
    }

}
