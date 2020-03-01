package com.hazelcast.raft.impl.local;

import com.hazelcast.raft.RaftEndpoint;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.raft.impl.util.Preconditions.checkNotNull;

/**
 * @author mdogan
 * @author metanet
 */
public class LocalRaftEndpoint
        implements RaftEndpoint {

    private static final AtomicInteger PORT_GENERATOR = new AtomicInteger(6700);

    public static LocalRaftEndpoint newEndpoint() {
        return new LocalRaftEndpoint(UUID.randomUUID(), PORT_GENERATOR.getAndIncrement());
    }

    private final UUID uuid;

    private final int port;

    private LocalRaftEndpoint(UUID uuid, int port) {
        checkNotNull(uuid);
        this.uuid = uuid;
        this.port = port;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public String identifierString() {
        return "[" + uuid.toString() + ", " + port + "]";
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

        if (port != that.port) {
            return false;
        }
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        int result = uuid.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "LocalRaftEndpoint{" + "uuid=" + uuid + ", port=" + port + '}';
    }
}
