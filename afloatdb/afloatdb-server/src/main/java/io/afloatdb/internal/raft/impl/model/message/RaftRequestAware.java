package io.afloatdb.internal.raft.impl.model.message;

import io.afloatdb.raft.proto.RaftRequest;

public interface RaftRequestAware {

    void populate(RaftRequest.Builder builder);

}
