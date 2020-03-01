package com.hazelcast.raft;

import java.io.Serializable;

/**
 * Implemented by request and response classes of the Raft consensus
 * algorithm RPCs. Raft messages are the objects that go back and forth between
 * Raft nodes.
 *
 * @author mdogan
 * @author metanet
 */
public interface RaftMsg
        extends Serializable {
}
