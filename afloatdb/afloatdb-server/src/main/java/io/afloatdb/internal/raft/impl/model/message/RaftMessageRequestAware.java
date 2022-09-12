package io.afloatdb.internal.raft.impl.model.message;

import io.afloatdb.raft.proto.RaftMessageRequest;

public interface RaftMessageRequestAware {

    void populate(RaftMessageRequest.Builder builder);

}
