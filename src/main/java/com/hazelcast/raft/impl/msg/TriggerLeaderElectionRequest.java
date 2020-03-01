package com.hazelcast.raft.impl.msg;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftMsg;

/**
 * Struct for the leadership transfer logic.
 * <p>
 * See <i>4.2.3 Disruptive servers</i> section of
 * of the Raft dissertation.
 *
 * @author mdogan
 * @author metanet
 */
public class TriggerLeaderElectionRequest
        implements RaftMsg {

    private static final long serialVersionUID = 1344969819113057168L;

    private RaftEndpoint leader;
    private int term;
    private int lastLogTerm;
    private long lastLogIndex;

    public TriggerLeaderElectionRequest() {
    }

    public TriggerLeaderElectionRequest(RaftEndpoint leader, int term, int lastLogTerm, long lastLogIndex) {
        this.leader = leader;
        this.term = term;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public int lastLogTerm() {
        return lastLogTerm;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public String toString() {
        return "TriggerLeaderElectionRequest{" + "leader=" + leader + ", term=" + term + ", lastLogTerm=" + lastLogTerm
                + ", lastLogIndex=" + lastLogIndex + '}';
    }

}
