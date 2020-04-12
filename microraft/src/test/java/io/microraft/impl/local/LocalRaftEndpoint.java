package io.microraft.impl.local;

import io.microraft.RaftEndpoint;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public final class LocalRaftEndpoint
        implements RaftEndpoint {

    private static final AtomicInteger COUNTER = new AtomicInteger();
    private final String id;

    private LocalRaftEndpoint(String id) {
        this.id = requireNonNull(id);
    }

    /**
     * Returns a new unique Raft endpoint.
     *
     * @return a new unique Raft endpoint
     */
    public static LocalRaftEndpoint newEndpoint() {
        return new LocalRaftEndpoint("node" + COUNTER.incrementAndGet());
    }

    @Nonnull
    @Override
    public Object getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LocalRaftEndpoint that = (LocalRaftEndpoint) o;

        return id.equals(that.id);
    }

    @Override
    public String toString() {
        return "LocalRaftEndpoint{" + "id=" + id + '}';
    }

}
