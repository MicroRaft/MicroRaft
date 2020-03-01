package com.hazelcast.raft.impl.local;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * @author mdogan
 * @author metanet
 */
public class RaftDummyService {

    private final Map<Long, Object> values = new ConcurrentHashMap<>();

    RaftDummyService() {
    }

    private Object apply(long commitIndex, Object value) {
        assert !values.containsKey(commitIndex) :
                "Cannot apply " + value + "since commitIndex: " + commitIndex + " already contains: " + values.get(commitIndex);

        values.put(commitIndex, value);
        return value;
    }

    public Object get(long commitIndex) {
        return values.get(commitIndex);
    }

    public int size() {
        return values.size();
    }

    public Set<Object> values() {
        return new HashSet<>(values.values());
    }

    Map<Long, Object> takeSnapshot(long commitIndex) {
        Map<Long, Object> snapshot = new HashMap<>();
        for (Entry<Long, Object> e : values.entrySet()) {
            assert e.getKey() <= commitIndex : "Key: " + e.getKey() + ", commit-index: " + commitIndex;
            snapshot.put(e.getKey(), e.getValue());
        }

        return snapshot;
    }

    void restoreSnapshot(long commitIndex, Map<Long, Object> snapshot) {
        values.clear();
        values.putAll(snapshot);
    }

    public static BiFunction<RaftDummyService, Long, Object> apply(Object val) {
        return new Apply(val);
    }

    public static BiFunction<RaftDummyService, Long, Object> query() {
        return new Query();
    }

    private static class Apply
            implements BiFunction<RaftDummyService, Long, Object> {

        private Object val;

        Apply(Object val) {
            this.val = val;
        }

        @Override
        public Object apply(RaftDummyService service, Long commitIndex) {
            return service.apply(commitIndex, val);
        }

        @Override
        public String toString() {
            return "Apply{" + "val=" + val + '}';
        }
    }

    private static class Query
            implements BiFunction<RaftDummyService, Long, Object> {

        @Override
        public Object apply(RaftDummyService service, Long commitIndex) {
            return service.get(commitIndex);
        }

        @Override
        public String toString() {
            return "Query{}";
        }
    }
}
