package com.hazelcast.raft.impl.local;

import java.util.function.BiFunction;

/**
 * @author mdogan
 * @author metanet
 */
public class Nop
        implements BiFunction<RaftDummyService, Long, Object> {
    @Override
    public Object apply(RaftDummyService raftDummyService, Long aLong) {
        return null;
    }

    @Override
    public String toString() {
        return "NopEntry{}";
    }
}
