package com.hazelcast.raft;

import java.io.Serializable;
import java.util.UUID;

/**
 * Represents an endpoint that participates to at least one Raft group.
 * <p>
 * For the Raft algorithm implementation, it is sufficient to differentiate
 * members of a Raft group with a unique id, and that is why we only have
 * a single method in this interface.
 *
 * @author mdogan
 * @author metanet
 */
public interface RaftEndpoint
        extends Serializable {

    /**
     * Returns the unique identifier of the Raft endpoint.
     */
    UUID getUuid();

    /**
     * Returns a short string to identify the Raft endpoint, which can be
     * different from the string returned from {@link Object#toString()}.
     */
    String identifierString();

}
