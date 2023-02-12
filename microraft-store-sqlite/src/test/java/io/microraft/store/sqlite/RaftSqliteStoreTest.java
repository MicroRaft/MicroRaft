package io.microraft.store.sqlite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.microraft.RaftEndpoint;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.impl.DefaultRaftModelFactory;
import io.microraft.model.impl.log.DefaultLogEntryOrBuilder;
import io.microraft.model.impl.log.DefaultRaftGroupMembersViewOrBuilder;
import io.microraft.model.impl.log.DefaultSnapshotChunkOrBuilder;
import io.microraft.model.impl.persistence.DefaultRaftEndpointPersistentStateOrBuilder;
import io.microraft.model.impl.persistence.DefaultRaftTermPersistentStateOrBuilder;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.persistence.RaftStoreSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

public class RaftSqliteStoreTest {
    private static final RaftModelFactory RAFT_MODEL_FACTORY = new DefaultRaftModelFactory();

    private static final RaftEndpoint ENDPOINT_A = LocalRaftEndpoint.newEndpoint();
    private static final RaftEndpoint ENDPOINT_B = LocalRaftEndpoint.newEndpoint();
    private static final long RAFT_INDEX = 12345;
    private static final int TERM = 235;
    private static final boolean VOTING = true;

    private static final RaftGroupMembersView INITIAL_GROUP_MEMBERS = RAFT_MODEL_FACTORY
            .createRaftGroupMembersViewBuilder().setLogIndex(RAFT_INDEX).setMembers(List.of(ENDPOINT_A, ENDPOINT_B))
            .setVotingMembers(List.of(ENDPOINT_A)).build();

    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    private File sqlite;

    @Before
    public void before() throws IOException {
        sqlite = new File(tempDir.newFolder(), "sqlite.db");
    }

    private void withRaftStore(Consumer<RaftSqliteStore> consumer) {
        RaftSqliteStore store = RaftSqliteStore.create(sqlite, RAFT_MODEL_FACTORY, JacksonModelSerializer.INSTANCE);
        consumer.accept(store);
        store.onRaftNodeTerminate();
    }

    @Test
    public void noRecoveredStateIfNoWrites() {
        withRaftStore(store -> assertThat(store.getRestoredRaftState()).isEmpty());
    }

    @Test
    public void basicRecoveredState() {
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            RestoredRaftState restored = store.getRestoredRaftState().get();
            assertThat(restored.getLocalEndpointPersistentState().getLocalEndpoint()).isEqualTo(ENDPOINT_A);
            assertThat(restored.getLocalEndpointPersistentState().isVoting()).isEqualTo(VOTING);
            assertThat(restored.getInitialGroupMembers()).usingRecursiveComparison().isEqualTo(INITIAL_GROUP_MEMBERS);
            assertThat(restored.getTermPersistentState().getTerm()).isEqualTo(TERM);
            assertThat(restored.getTermPersistentState().getVotedFor()).isEqualTo(ENDPOINT_B);
        });
    }

    @Test
    public void testLogEntryFlushing() {
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            store.persistLogEntry(logEntry(0, 0));
            store.persistLogEntry(logEntry(1, 0));
            store.persistLogEntry(logEntry(2, 0));
        });
        withRaftStore(store -> {
            assertThat(store.getRestoredRaftState().get().getLogEntries()).isEmpty();
            store.persistLogEntry(logEntry(0, 0));
            store.persistLogEntry(logEntry(1, 0));
            store.flush();
            store.persistLogEntry(logEntry(2, 0));
        });
        withRaftStore(store -> {
            assertThat(store.getRestoredRaftState().get().getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(logEntry(0, 0), logEntry(1, 0));
            store.truncateLogEntriesFrom(1);
            store.flush();
            assertThat(store.getRestoredRaftState().get().getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(logEntry(0, 0));
        });
    }

    @Test
    public void testSnapshots() throws IOException {
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            store.persistLogEntry(logEntry(0, 0));
            store.persistLogEntry(logEntry(1, 0));
            store.persistLogEntry(logEntry(2, 0));
            store.flush();
            store.persistSnapshotChunk(snapshotChunk(1, 0, 0, 1));
            store.flush();
            // once a snapshot chunk has been flushed, irrelevant log entries can be deleted
            assertThat(store.getRestoredRaftState().get().getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(logEntry(2, 0));
            assertThat(store.getRestoredRaftState().get().getSnapshotEntry().getOperation()).usingRecursiveComparison()
                    .isEqualTo(List.of(snapshotChunk(1, 0, 0, 1)));
        });
        sqlite = new File(tempDir.newFolder(), "sqlite.db");
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            store.persistLogEntry(logEntry(3, 1));
            store.flush();
            store.persistSnapshotChunk(snapshotChunk(3, 1, 1, 2));
            store.flush();
            assertThat(store.getRestoredRaftState().get().getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                    .contains(logEntry(3, 1));
            // snapshots can be committed out of order
            store.persistSnapshotChunk(snapshotChunk(3, 1, 0, 2));
            store.flush();
            // irrelevant snapshot chunks are deleted
            assertThat(store.getAllSnapshotChunks()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(snapshotChunk(3, 1, 0, 2), snapshotChunk(3, 1, 1, 2));
            assertThat(store.getRestoredRaftState().get().getLogEntries()).isEmpty();
        });
        sqlite = new File(tempDir.newFolder(), "sqlite.db");
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            store.persistSnapshotChunk(snapshotChunk(1, 1, 0, 1));
            store.flush();
            store.persistSnapshotChunk(snapshotChunk(2, 1, 0, 3));
            store.persistSnapshotChunk(snapshotChunk(2, 1, 2, 3));
            store.deleteSnapshotChunks(2, 1);
            store.flush();
            assertThat(store.getAllSnapshotChunks()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(snapshotChunk(1, 1, 0, 1));
        });
        sqlite = new File(tempDir.newFolder(), "sqlite.db");
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            store.persistSnapshotChunk(snapshotChunk(1, 1, 0, 1));
            store.flush();
            store.persistSnapshotChunk(snapshotChunk(2, 1, 0, 3));
            store.persistSnapshotChunk(snapshotChunk(2, 1, 2, 3));
            store.deleteSnapshotChunks(1, 1);
            store.persistSnapshotChunk(snapshotChunk(3, 1, 0, 1));
            store.flush();
            assertThat(store.getAllSnapshotChunks()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(snapshotChunk(3, 1, 0, 1));
        });
        sqlite = new File(tempDir.newFolder(), "sqlite.db");
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            store.persistSnapshotChunk(snapshotChunk(1, 1, 0, 2));
            store.deleteSnapshotChunks(1, 2);
            store.persistSnapshotChunk(snapshotChunk(2, 1, 0, 2));
            store.deleteSnapshotChunks(2, 2);
            store.flush();
            assertThat(store.getAllSnapshotChunks()).isEmpty();
        });
    }

    @Test
    public void testRestoreCleansUpRedundantLogEntriesAndSnapshotChunks() {
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            store.persistLogEntry(logEntry(1, 1));
            store.persistSnapshotChunk(snapshotChunk(2, 1, 0, 2));
            store.persistSnapshotChunk(snapshotChunk(3, 1, 0, 1));
            store.rawFlush();

            assertThat(store.getRestoredRaftState().get().getLogEntries()).isEmpty();
            assertThat(store.getAllSnapshotChunks()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(snapshotChunk(3, 1, 0, 1));
        });
    }

    private static LogEntry logEntry(long index, int term) {
        return RAFT_MODEL_FACTORY.createLogEntryBuilder().setIndex(index).setTerm(term).setOperation(index + " " + term)
                .build();
    }

    private static SnapshotChunk snapshotChunk(long index, int term, int chunkIndex, int numChunks) {
        return RAFT_MODEL_FACTORY.createSnapshotChunkBuilder().setIndex(index).setTerm(term)
                .setSnapshotChunkIndex(chunkIndex).setSnapshotChunkCount(numChunks)
                .setGroupMembersView(INITIAL_GROUP_MEMBERS)
                .setOperation(index + " " + term + " " + chunkIndex + " " + numChunks).build();
    }

    private static void persistInitialState(RaftSqliteStore store) {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(VOTING).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(
                new DefaultRaftTermPersistentStateOrBuilder().setTerm(TERM).setVotedFor(ENDPOINT_B).build());
    }

    private enum JacksonModelSerializer implements RaftStoreSerializer {
        INSTANCE;

        @Override
        public Serializer<RaftGroupMembersView> raftGroupMembersViewSerializer() {
            return new JacksonSerializer<>(DefaultRaftGroupMembersViewOrBuilder.class);
        }

        @Override
        public Serializer<RaftEndpoint> raftEndpointSerializer() {
            return new JacksonSerializer<>(LocalRaftEndpoint.class);
        }

        @Override
        public Serializer<LogEntry> logEntrySerializer() {
            return new JacksonSerializer<>(DefaultLogEntryOrBuilder.class);
        }

        @Override
        public Serializer<SnapshotChunk> snapshotChunkSerializer() {
            return new JacksonSerializer<>(DefaultSnapshotChunkOrBuilder.class);
        }

        @Override
        public Serializer<RaftEndpointPersistentState> raftEndpointPersistentStateSerializer() {
            return new JacksonSerializer<>(DefaultRaftEndpointPersistentStateOrBuilder.class);
        }

        public Serializer<RaftTermPersistentState> raftTermPersistentState() {
            return new JacksonSerializer<>(DefaultRaftTermPersistentStateOrBuilder.class);
        }
    }

    /**
     * Uses the json library Jackson for retrofitting serialization on top of the
     * default model. If the default model ever gets equals methods, then the
     * recursive comparisons in the tests can be removed. If the default model ever
     * has a baked in persistence mechanism, then the Jackson can be removed.
     */
    private static final class JacksonSerializer<T> implements RaftStoreSerializer.Serializer<T> {
        private static final ObjectMapper objectMapper = new ObjectMapper()
                .addMixIn(LocalRaftEndpoint.class, RaftEndpointMixin.class)
                .addMixIn(DefaultRaftGroupMembersViewOrBuilder.class, RaftGroupMembersViewMixin.class)
                .addMixIn(DefaultLogEntryOrBuilder.class, LogEntryMixin.class)
                .addMixIn(DefaultSnapshotChunkOrBuilder.class, SnapshotChunkMixin.class);

        private final Class<? extends T> clazz;

        private JacksonSerializer(Class<? extends T> clazz) {
            this.clazz = clazz;
        }

        @Nonnull
        @Override
        public byte[] serialize(@Nonnull T element) {
            try {
                return objectMapper.writeValueAsBytes(element);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Nonnull
        @Override
        public T deserialize(@Nonnull byte[] element) {
            try {
                return objectMapper.readValue(element, clazz);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class RaftEndpointMixin {
        @JsonValue
        private String id;

        @JsonCreator
        private RaftEndpointMixin(String id) {
        }
    }

    @JsonDeserialize(builder = DefaultRaftGroupMembersViewOrBuilder.class)
    private static final class RaftGroupMembersViewMixin {

        @JsonDeserialize(contentAs = LocalRaftEndpoint.class)
        private Collection<RaftEndpoint> members;

        @JsonDeserialize(contentAs = LocalRaftEndpoint.class)
        private Collection<RaftEndpoint> votingMembers;
    }

    @JsonDeserialize(builder = DefaultLogEntryOrBuilder.class)
    private static final class LogEntryMixin {
    }

    @JsonDeserialize(builder = DefaultSnapshotChunkOrBuilder.class)
    private static final class SnapshotChunkMixin {
        @JsonDeserialize(as = DefaultRaftGroupMembersViewOrBuilder.class)
        private RaftGroupMembersView groupMembersView;
    }

}
