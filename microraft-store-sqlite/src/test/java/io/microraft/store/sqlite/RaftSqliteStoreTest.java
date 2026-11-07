package io.microraft.store.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.jooq.CloseableDSLContext;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.microraft.RaftEndpoint;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.log.RaftLog;
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
import io.microraft.persistence.RaftStoreSerializer;
import io.microraft.persistence.RestoredRaftState;

public class RaftSqliteStoreTest {
    private static final RaftModelFactory RAFT_MODEL_FACTORY = new DefaultRaftModelFactory();
    private static final RaftEndpoint ENDPOINT_A = LocalRaftEndpoint.newEndpoint();
    private static final RaftEndpoint ENDPOINT_B = LocalRaftEndpoint.newEndpoint();
    private static final long RAFT_INDEX = 12345;
    private static final int TERM = 235;
    private static final boolean VOTING = true;
    private static final int SNAPSHOT_INDEX = 20;
    private static final int SNAPSHOT_CHUNK_SIZE = SNAPSHOT_INDEX / 2;
    private static final int PARTIAL_CHUNK_SIZE = SNAPSHOT_CHUNK_SIZE / 2;

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
        try {
            consumer.accept(store);
        } finally {
            store.onRaftNodeTerminate();
        }
    }

    @Test
    public void noRecoveredStateIfNoWrites() {
        withRaftStore(store -> assertThat(store.getRestoredRaftState(false)).isEmpty());
    }

    @Test
    public void basicRecoveredState() {
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            RestoredRaftState restored = store.getRestoredRaftState(false).get();
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
            store.persistLogEntries(List.of(logEntry(1, 1), logEntry(2, 1), logEntry(3, 1)));
            store.flush();
        });
        withRaftStore(store -> {
            assertThat(store.getRestoredRaftState(false).get().getLogEntries())
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(logEntry(1, 1), logEntry(2, 1), logEntry(3, 1));
            store.persistLogEntries(List.of(logEntry(4, 1), logEntry(5, 1)));
            store.flush();
        });
        withRaftStore(store -> {
            assertThat(store.getRestoredRaftState(false).get().getLogEntries())
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(logEntry(1, 1), logEntry(2, 1), logEntry(3, 1), logEntry(4, 1), logEntry(5, 1));
            store.truncateLogEntriesFrom(2);
            store.flush();
        });
        withRaftStore(store -> {
            assertThat(store.getRestoredRaftState(false).get().getLogEntries())
                    .usingRecursiveFieldByFieldElementComparator().containsExactly(logEntry(1, 1));
        });
    }

    @Test
    public void recoversTermAndLogWhenNoVoteWasCast() {
        withRaftStore(store -> {
            persistInitialState(store);
            store.persistLogEntries(List.of(logEntry(1, TERM)));
            store.persistAndFlushTerm(RAFT_MODEL_FACTORY.createRaftTermPersistentStateBuilder()
                    .setTerm(TERM + 1).setVotedFor(null).build());
        });
        withRaftStore(store -> {
            RestoredRaftState restored = store.getRestoredRaftState(false).orElseThrow();
            assertThat(restored.getTermPersistentState().getTerm()).isEqualTo(TERM + 1);
            assertThat(restored.getTermPersistentState().getVotedFor()).isNull();
            assertThat(restored.getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(logEntry(1, TERM));
        });
    }

    @Test
    public void testSnapshots() throws IOException {
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            store.persistLogEntries(List.of(logEntry(1, 1), logEntry(2, 1), logEntry(3, 1), logEntry(4, 1)));
            store.flush();
            store.persistSnapshotChunk(snapshotChunk(2, 1, 0, 1));
            store.flush();
            // once a snapshot chunk has been flushed, irrelevant log entries can be deleted
            store.truncateLogEntriesUntil(2);
            store.flush();
            store.persistSnapshotChunk(snapshotChunk(3, 1, 0, 1));
            store.flush();
            // once a snapshot chunk has been flushed, irrelevant log entries can be deleted
            store.truncateLogEntriesUntil(3);
            store.flush();
            assertThat(store.getAllSnapshotChunks()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(snapshotChunk(3, 1, 0, 1));
            RestoredRaftState restoredRaftState = store.getRestoredRaftState(false).get();
            assertThat(restoredRaftState.getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(logEntry(4, 1));
            assertThat(restoredRaftState.getSnapshotEntry().getOperation()).usingRecursiveComparison()
                    .isEqualTo(List.of(snapshotChunk(3, 1, 0, 1)));
        });
        sqlite = new File(tempDir.newFolder(), "sqlite.db");
        withRaftStore(RaftSqliteStoreTest::persistInitialState);
        withRaftStore(store -> {
            store.persistSnapshotChunk(snapshotChunk(1, 1, 0, 1));
            store.flush();
            store.persistSnapshotChunk(snapshotChunk(2, 1, 0, 3));
            store.persistSnapshotChunk(snapshotChunk(2, 1, 2, 3));
            store.deleteSnapshotChunks(2, 3);
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
            store.deleteSnapshotChunks(2, 3);
            store.persistSnapshotChunk(snapshotChunk(3, 1, 0, 1));
            store.flush();
            assertThat(store.getAllSnapshotChunks()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(snapshotChunk(1, 1, 0, 1), snapshotChunk(3, 1, 0, 1));
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
            store.persistLogEntries(List.of(logEntry(1, 1)));
            store.persistSnapshotChunk(snapshotChunk(2, 1, 0, 2));
            store.persistSnapshotChunk(snapshotChunk(3, 1, 0, 1));
            store.flush();

            assertThat(store.getRestoredRaftState(true).get().getLogEntries()).isEmpty();
            assertThat(store.getAllSnapshotChunks()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(snapshotChunk(3, 1, 0, 1));
        });
    }

    @Test
    public void identicalSnapshotChunkCanBeRetriedAfterReopening() {
        SnapshotChunk chunk = snapshotChunk(SNAPSHOT_INDEX, TERM, 0, 1);
        withRaftStore(store -> {
            persistInitialState(store);
            store.persistSnapshotChunk(chunk);
            store.flush();
        });
        withRaftStore(store -> {
            store.persistSnapshotChunk(snapshotChunk(SNAPSHOT_INDEX, TERM, 0, 1));
            store.flush();
        });
        withRaftStore(store -> assertThat(store.getRestoredRaftState(false).get().getSnapshotEntry().getOperation())
                .usingRecursiveComparison().isEqualTo(List.of(chunk)));
    }

    @Test
    public void conflictingSnapshotChunkIsRejectedWithoutCorruptingRecovery() {
        SnapshotChunk previousSnapshot = snapshotChunk(1, TERM, 0, 1);
        SnapshotChunk partialChunk = snapshotChunkWithValues(0, 2, 1, PARTIAL_CHUNK_SIZE);
        withRaftStore(store -> {
            persistInitialState(store);
            store.persistSnapshotChunk(previousSnapshot);
            store.persistSnapshotChunk(partialChunk);
            store.flush();
        });
        withRaftStore(store -> {
            assertThatThrownBy(() -> store.persistSnapshotChunk(snapshotChunkWithValues(0, 2, 1, SNAPSHOT_CHUNK_SIZE)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Conflicting snapshot chunk");
        });
        withRaftStore(store -> {
            assertThat(store.getAllSnapshotChunks()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(previousSnapshot, partialChunk);
            assertThat(store.getRestoredRaftState(false).get().getSnapshotEntry().getIndex()).isEqualTo(1);
            assertThat(store.getAllSnapshotChunks()).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(previousSnapshot);
        });
    }

    @Test
    public void inconsistentSnapshotChunkCountIsRejected() {
        withRaftStore(store -> {
            store.persistSnapshotChunk(snapshotChunk(SNAPSHOT_INDEX, TERM, 0, 2));
            // A distinct key must not complete a snapshot using chunks from a different layout.
            assertThatThrownBy(() -> store.persistSnapshotChunk(snapshotChunk(SNAPSHOT_INDEX, TERM, 1, 3)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Conflicting snapshot chunk count");
            assertThat(store.getMaxCommittedSnapshotIndex()).isEmpty();
        });
    }

    @Test
    public void deletedSnapshotCanBeReplacedWithDifferentChunks() {
        withRaftStore(store -> {
            persistInitialState(store);
            store.persistSnapshotChunk(snapshotChunkWithValues(0, 3, 1, PARTIAL_CHUNK_SIZE));
            store.persistSnapshotChunk(snapshotChunkWithValues(2, 3, SNAPSHOT_INDEX - PARTIAL_CHUNK_SIZE + 1,
                    SNAPSHOT_INDEX));
            store.flush();
        });
        withRaftStore(store -> {
            store.deleteSnapshotChunks(SNAPSHOT_INDEX, 3);
            store.persistSnapshotChunk(snapshotChunkWithValues(0, 2, 1, SNAPSHOT_CHUNK_SIZE));
            store.persistSnapshotChunk(snapshotChunkWithValues(1, 2, SNAPSHOT_CHUNK_SIZE + 1, SNAPSHOT_INDEX));
            store.flush();
        });
        withRaftStore(store -> {
            assertThat(store.getRestoredRaftState(false).get().getSnapshotEntry().getIndex()).isEqualTo(SNAPSHOT_INDEX);
            assertThat(store.getAllSnapshotChunks()).hasSize(2);
            List<Object> values = new ArrayList<>();
            for (SnapshotChunk chunk : store.getAllSnapshotChunks()) {
                values.addAll((List<?>) chunk.getOperation());
            }
            assertThat(values).containsExactlyElementsOf(snapshotValues(1, SNAPSHOT_INDEX));
        });
    }

    @Test
    public void recoveryDiscardsIncompleteSnapshotWithCleanupEnabled() {
        assertIncompleteSnapshotIsDiscarded(true, true);
    }

    @Test
    public void recoveryDiscardsIncompleteSnapshotWithCleanupDisabled() {
        assertIncompleteSnapshotIsDiscarded(false, true);
    }

    @Test
    public void recoveryDiscardsIncompleteFirstSnapshotWithCleanupEnabled() {
        assertIncompleteSnapshotIsDiscarded(true, false);
    }

    @Test
    public void recoveryDiscardsIncompleteFirstSnapshotWithCleanupDisabled() {
        assertIncompleteSnapshotIsDiscarded(false, false);
    }

    private void assertIncompleteSnapshotIsDiscarded(boolean truncateStaleData, boolean hasPreviousSnapshot) {
        withRaftStore(store -> {
            persistInitialState(store);
            if (hasPreviousSnapshot) {
                store.persistSnapshotChunk(snapshotChunk(1, TERM, 0, 1));
            }
            store.persistSnapshotChunk(snapshotChunkWithValues(0, 2, 1, PARTIAL_CHUNK_SIZE));
            store.flush();
        });
        withRaftStore(store -> {
            store.getRestoredRaftState(truncateStaleData).orElseThrow();
            // This transfer has different boundaries and has received only chunk 1.
            store.persistSnapshotChunk(snapshotChunkWithValues(1, 2, SNAPSHOT_CHUNK_SIZE + 1, SNAPSHOT_INDEX));
            store.flush();
        });
        withRaftStore(store -> {
            RestoredRaftState restored = store.getRestoredRaftState(truncateStaleData).orElseThrow();
            if (hasPreviousSnapshot) {
                assertThat(restored.getSnapshotEntry().getIndex()).isEqualTo(1);
            } else {
                assertThat(restored.getSnapshotEntry()).isNull();
            }
            // A fresh, complete transfer must still be recoverable.
            store.persistSnapshotChunk(snapshotChunkWithValues(0, 2, 1, SNAPSHOT_CHUNK_SIZE));
            store.persistSnapshotChunk(snapshotChunkWithValues(1, 2, SNAPSHOT_CHUNK_SIZE + 1, SNAPSHOT_INDEX));
            store.flush();
        });
        withRaftStore(store -> assertThat(store.getRestoredRaftState(truncateStaleData).orElseThrow()
                .getSnapshotEntry().getOperation()).usingRecursiveComparison()
                        .isEqualTo(List.of(snapshotChunkWithValues(0, 2, 1, SNAPSHOT_CHUNK_SIZE),
                                snapshotChunkWithValues(1, 2, SNAPSHOT_CHUNK_SIZE + 1, SNAPSHOT_INDEX))));
    }

    @Test
    public void incompleteSnapshotPreservesLogOnRecovery() {
        assertSnapshotCompletionRecovery(false, false, false);
    }

    @Test
    public void unflushedCompletedSnapshotPreservesLogOnRecovery() {
        assertSnapshotCompletionRecovery(true, false, false);
    }

    @Test
    public void completedSnapshotDiscardsConflictingSuffixBeforeInstallation() {
        assertSnapshotCompletionRecovery(true, true, false);
    }

    @Test
    public void discardedSuffixDoesNotReappearBeforeAsyncFlush() {
        assertSnapshotCompletionRecovery(true, true, true);
    }

    private void assertSnapshotCompletionRecovery(boolean complete, boolean flush, boolean install) {
        List<LogEntry> oldLog = List.of(logEntry(1, 1), logEntry(2, 2), logEntry(3, 2));
        List<SnapshotChunk> chunks = List.of(snapshotChunk(2, 3, 0, 2), snapshotChunk(2, 3, 1, 2));
        withRaftStore(store -> {
            persistInitialState(store);
            RaftLog log = RaftLog.create(SNAPSHOT_INDEX, store);
            log.appendEntries(oldLog);
            log.flush();
            store.persistSnapshotChunk(chunks.get(0));
            store.flush();
            if (complete) {
                store.persistSnapshotChunk(chunks.get(1));
                if (flush) {
                    store.flush();
                }
            }
            if (install) {
                log.truncateEntriesFrom(2);
                log.setSnapshot(RAFT_MODEL_FACTORY.createSnapshotEntryBuilder().setIndex(2).setTerm(3)
                        .setSnapshotChunks(chunks).setGroupMembersView(INITIAL_GROUP_MEMBERS).build());
                assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(2);
                assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(3);
            }
            // Closing rolls back buffered changes, as a crash before the next FlushTask would.
        });
        // Inspect disk before startup repair, to verify the completion transaction itself.
        try (CloseableDSLContext db = DSL.using("jdbc:sqlite:" + sqlite)) {
            List<Long> indices = db.select(DSL.field("logIndex", Long.class)).from("logEntries")
                    .orderBy(DSL.field("logIndex")).fetch(0, Long.class);
            if (complete && flush) {
                assertThat(indices).containsExactly(1L);
            } else {
                assertThat(indices).containsExactly(1L, 2L, 3L);
            }
        }
        withRaftStore(store -> {
            RestoredRaftState restored = store.getRestoredRaftState(true).orElseThrow();
            if (complete && flush) {
                assertThat(restored.getSnapshotEntry().getOperation()).usingRecursiveComparison().isEqualTo(chunks);
                assertThat(restored.getLogEntries()).isEmpty();
            } else {
                assertThat(restored.getSnapshotEntry()).isNull();
                assertThat(restored.getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                        .containsExactlyElementsOf(oldLog);
            }
        });
    }

    @Test
    public void completedSnapshotPreservesMatchingSuffix() {
        withRaftStore(store -> {
            persistInitialState(store);
            store.persistLogEntries(List.of(logEntry(1, 1), logEntry(2, 2), logEntry(3, 2)));
            store.flush();
            store.persistSnapshotChunk(snapshotChunk(2, 2, 0, 1));
            store.flush();
        });
        withRaftStore(store -> assertThat(store.getRestoredRaftState(true).orElseThrow().getLogEntries())
                .usingRecursiveFieldByFieldElementComparator().containsExactly(logEntry(3, 2)));
    }

    @Test
    public void retryingCompactedSnapshotPreservesNewerEntries() {
        SnapshotChunk chunk = snapshotChunk(2, 3, 0, 1);
        withRaftStore(store -> {
            persistInitialState(store);
            store.persistSnapshotChunk(chunk);
            store.flush();
            store.truncateLogEntriesUntil(2);
            store.persistLogEntries(List.of(logEntry(3, 3)));
            store.flush();
        });
        withRaftStore(store -> {
            store.persistSnapshotChunk(chunk);
            store.flush();
        });
        withRaftStore(store -> assertThat(store.getRestoredRaftState(true).orElseThrow().getLogEntries())
                .usingRecursiveFieldByFieldElementComparator().containsExactly(logEntry(3, 3)));
    }

    @Test
    public void recoveryRepairsConflictingSnapshotWithEitherCleanupSetting() throws IOException {
        for (boolean truncateStaleData : new boolean[]{true, false}) {
            sqlite = new File(tempDir.newFolder(), "sqlite.db");
            withRaftStore(store -> {
                persistInitialState(store);
                store.persistSnapshotChunk(snapshotChunk(2, 3, 0, 1));
                store.flush();
                // Recreate a database left by the old implementation's snapshot/cleanup crash window.
                store.persistLogEntries(List.of(logEntry(1, 1), logEntry(2, 2), logEntry(3, 2)));
                store.flush();
            });
            withRaftStore(store -> assertThat(store.getRestoredRaftState(truncateStaleData).orElseThrow().getLogEntries())
                    .filteredOn(entry -> entry.getIndex() > 2).isEmpty());
            withRaftStore(store -> {
                // Recovery must commit the repair, so this replacement is not ignored as a duplicate.
                store.persistLogEntries(List.of(logEntry(3, 3)));
                store.flush();
            });
            withRaftStore(store -> assertThat(store.getRestoredRaftState(truncateStaleData).orElseThrow().getLogEntries())
                    .filteredOn(entry -> entry.getIndex() > 2).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(logEntry(3, 3)));
        }
    }

    private static SnapshotChunk snapshotChunkWithValues(int chunkIndex, int chunkCount, int first, int last) {
        return RAFT_MODEL_FACTORY.createSnapshotChunkBuilder().setIndex(SNAPSHOT_INDEX).setTerm(TERM)
                .setSnapshotChunkIndex(chunkIndex).setSnapshotChunkCount(chunkCount)
                .setGroupMembersView(INITIAL_GROUP_MEMBERS).setOperation(snapshotValues(first, last)).build();
    }

    private static List<Integer> snapshotValues(int first, int last) {
        List<Integer> values = new ArrayList<>();
        for (int value = first; value <= last; value++) {
            values.add(value);
        }
        return values;
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
