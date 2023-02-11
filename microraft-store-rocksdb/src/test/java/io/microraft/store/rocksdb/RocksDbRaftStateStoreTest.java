package io.microraft.store.rocksdb;

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
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

public class RocksDbRaftStateStoreTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbRaftStateStoreTest.class);

    private static final RaftModelFactory RAFT_MODEL_FACTORY = new DefaultRaftModelFactory();

    private static final RaftEndpoint ENDPOINT_A = LocalRaftEndpoint.newEndpoint();
    private static final RaftEndpoint ENDPOINT_B = LocalRaftEndpoint.newEndpoint();
    private static final long RAFT_INDEX = 12345;

    private static final RaftGroupMembersView INITIAL_GROUP_MEMBERS = RAFT_MODEL_FACTORY
            .createRaftGroupMembersViewBuilder().setLogIndex(RAFT_INDEX).setMembers(List.of(ENDPOINT_A, ENDPOINT_B))
            .setVotingMembers(List.of(ENDPOINT_A)).build();

    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    private File rocksDbDir;
    private RocksDbRaftStateStore store;

    @Before
    public void init() throws IOException {
        rocksDbDir = tempDir.newFolder();
        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();
    }

    @After
    public void shutdown() {
        if (store != null) {
            store.onRaftNodeTerminate();
        }
    }

    @Test
    public void testPersistInitialState() throws IOException {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(false).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).build());

        store.close(false);

        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();

        Optional<RestoredRaftState> restoredStateOpt = store.getRestoredRaftState(false /* truncateStaleData */);
        assertThat(restoredStateOpt.isPresent()).isTrue();

        RestoredRaftState restoredState = restoredStateOpt.get();

        assertThat(restoredState.getLocalEndpointPersistentState().getLocalEndpoint()).isEqualTo(ENDPOINT_A);
        assertThat(restoredState.getLocalEndpointPersistentState().isVoting()).isEqualTo(false);
        assertInitialGroupMembers(restoredState, INITIAL_GROUP_MEMBERS);
        assertThat(restoredState.getTermPersistentState().getTerm()).isEqualTo(1);
        assertThat(restoredState.getTermPersistentState().getVotedFor()).isNull();
    }

    @Test
    public void testVotingStateChange() throws IOException {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(false).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).build());
        store.persistAndFlushLocalEndpoint(
                new DefaultRaftEndpointPersistentStateOrBuilder().setLocalEndpoint(ENDPOINT_A).setVoting(true).build());

        store.close(false);

        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();

        Optional<RestoredRaftState> restoredStateOpt = store.getRestoredRaftState(false /* truncateStaleData */);
        assertThat(restoredStateOpt.isPresent()).isTrue();

        RestoredRaftState restoredState = restoredStateOpt.get();

        assertThat(restoredState.getLocalEndpointPersistentState().getLocalEndpoint()).isEqualTo(ENDPOINT_A);
        assertThat(restoredState.getLocalEndpointPersistentState().isVoting()).isEqualTo(true);
    }

    @Test
    public void testPersistTermIncrease() throws IOException {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(false).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).build());
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(2).build());

        store.close(false);

        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();

        Optional<RestoredRaftState> restoredStateOpt = store.getRestoredRaftState(false /* truncateStaleData */);
        assertThat(restoredStateOpt.isPresent()).isTrue();

        RestoredRaftState restoredState = restoredStateOpt.get();
        assertThat(restoredState.getTermPersistentState().getTerm()).isEqualTo(2);
        assertThat(restoredState.getTermPersistentState().getVotedFor()).isNull();
    }

    @Test
    public void testPersistVote() throws IOException {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(false).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).build());
        store.persistAndFlushTerm(
                new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).setVotedFor(ENDPOINT_A).build());

        store.close(false);

        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();

        Optional<RestoredRaftState> restoredStateOpt = store.getRestoredRaftState(false /* truncateStaleData */);
        assertThat(restoredStateOpt.isPresent()).isTrue();

        RestoredRaftState restoredState = restoredStateOpt.get();
        assertThat(restoredState.getTermPersistentState().getTerm()).isEqualTo(1);
        assertThat(restoredState.getTermPersistentState().getVotedFor()).isNotNull();
        assertThat(restoredState.getTermPersistentState().getVotedFor().getId()).isEqualTo(ENDPOINT_A.getId());
    }

    @Test
    public void testPersistLogEntries() throws IOException {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(false).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).build());

        store.persistLogEntry(logEntry(1, 1));
        store.persistLogEntry(logEntry(2, 1));
        store.persistLogEntry(logEntry(3, 1));
        store.persistLogEntry(logEntry(4, 2));
        store.flush();

        store.close(false);

        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();

        Optional<RestoredRaftState> restoredStateOpt = store.getRestoredRaftState(false /* truncateStaleData */);
        assertThat(restoredStateOpt.isPresent()).isTrue();
        assertThat(restoredStateOpt.get().getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                .containsExactly(logEntry(1, 1), logEntry(2, 1), logEntry(3, 1), logEntry(4, 2));
    }

    @Test
    public void testTruncatePersistedLogEntries() throws IOException {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(false).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).build());

        store.persistLogEntry(logEntry(1, 1));
        store.persistLogEntry(logEntry(2, 1));
        store.persistLogEntry(logEntry(3, 2));
        store.persistLogEntry(logEntry(4, 2));
        store.persistLogEntry(logEntry(5, 2));
        store.flush();

        store.truncateLogEntriesFrom(4);
        store.flush();

        store.close(false);

        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();

        Optional<RestoredRaftState> restoredStateOpt = store.getRestoredRaftState(false /* truncateStaleData */);
        assertThat(restoredStateOpt.isPresent()).isTrue();
        assertThat(restoredStateOpt.get().getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                .containsExactly(logEntry(1, 1), logEntry(2, 1), logEntry(3, 2));
    }

    @Test
    public void testPartiallyPersistedSnapshotNotRestored() throws IOException {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(false).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).build());

        store.persistLogEntry(logEntry(1, 1));
        store.persistLogEntry(logEntry(2, 1));
        store.persistLogEntry(logEntry(3, 1));
        store.persistLogEntry(logEntry(4, 1));
        store.persistLogEntry(logEntry(5, 1));
        store.persistSnapshotChunk(snapshotChunk(5, 1, 0, 2));

        store.doFlush();
        store.close(false);

        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();

        Optional<RestoredRaftState> restoredStateOpt = store.getRestoredRaftState(false /* truncateStaleData */);
        assertThat(restoredStateOpt.isPresent()).isTrue();
        assertThat(restoredStateOpt.get().getSnapshotEntry()).isNull();
        assertThat(restoredStateOpt.get().getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                .containsExactly(logEntry(1, 1), logEntry(2, 1), logEntry(3, 1), logEntry(4, 1), logEntry(5, 1));
    }

    @Test
    public void testFullyPersistedSnapshotRestored() throws IOException {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(false).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).build());

        store.persistLogEntry(logEntry(1, 1));
        store.persistLogEntry(logEntry(2, 1));
        store.persistLogEntry(logEntry(3, 1));
        store.persistLogEntry(logEntry(4, 1));
        store.persistLogEntry(logEntry(5, 1));
        store.persistLogEntry(logEntry(6, 1));
        store.persistSnapshotChunk(snapshotChunk(5, 1, 0, 2));
        store.persistSnapshotChunk(snapshotChunk(5, 1, 1, 2));

        store.doFlush();
        store.close(false);

        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();

        Optional<RestoredRaftState> restoredStateOpt = store.getRestoredRaftState(false /* truncateStaleData */);
        assertThat(restoredStateOpt.isPresent()).isTrue();
        assertThat(restoredStateOpt.get().getSnapshotEntry()).isNotNull();
        assertThat(restoredStateOpt.get().getSnapshotEntry().getIndex()).isEqualTo(5);
        assertThat(restoredStateOpt.get().getSnapshotEntry().getTerm()).isEqualTo(1);
        assertThat(restoredStateOpt.get().getSnapshotEntry().getSnapshotChunkCount()).isEqualTo(2);
        assertThat(restoredStateOpt.get().getLogEntries()).usingRecursiveFieldByFieldElementComparator()
                .containsExactly(logEntry(6, 1));
    }

    @Test
    public void testDeleteSnapshotChunks() throws IOException {
        store.persistAndFlushLocalEndpoint(new DefaultRaftEndpointPersistentStateOrBuilder()
                .setLocalEndpoint(ENDPOINT_A).setVoting(false).build());
        store.persistAndFlushInitialGroupMembers(INITIAL_GROUP_MEMBERS);
        store.persistAndFlushTerm(new DefaultRaftTermPersistentStateOrBuilder().setTerm(1).build());

        store.persistSnapshotChunk(snapshotChunk(5, 1, 0, 2));
        store.persistSnapshotChunk(snapshotChunk(5, 1, 1, 2));
        store.deleteSnapshotChunks(5, 2);
        store.persistSnapshotChunk(snapshotChunk(6, 1, 0, 2));
        store.persistSnapshotChunk(snapshotChunk(6, 1, 1, 2));
        store.deleteSnapshotChunks(6, 2);

        store.doFlush();
        store.close(false);

        store = new RocksDbRaftStateStore(rocksDbDir.getAbsolutePath(), JacksonModelSerializer.INSTANCE,
                RAFT_MODEL_FACTORY);
        store.onRaftNodeStart();

        Optional<RestoredRaftState> restoredStateOpt = store.getRestoredRaftState(false /* truncateStaleData */);
        assertThat(restoredStateOpt.isPresent()).isTrue();
        assertThat(restoredStateOpt.get().getSnapshotEntry()).isNull();
    }

    @Test
    public void testStressWrites() throws IOException {
        int entryCount = 50000;
        for (int i = 0; i < entryCount; i++) {
            if (i % 100 == 0) {
                LOGGER.info("i=" + i);
            }
            store.persistLogEntry(logEntry(i, 1));
            // if (i % 50 == 0) {
            store.flush();
            // }
        }
    }

    private void assertInitialGroupMembers(RestoredRaftState state, RaftGroupMembersView expected) {
        assertThat(state.getInitialGroupMembers()).isNotNull();
        assertThat(state.getInitialGroupMembers().getLogIndex()).isEqualTo(expected.getLogIndex());
        List<Object> restoredMemberIds = state.getInitialGroupMembers().getMembers().stream().map(RaftEndpoint::getId)
                .collect(Collectors.toList());
        List<Object> expectedMemberIds = expected.getMembers().stream().map(RaftEndpoint::getId)
                .collect(Collectors.toList());
        assertThat(restoredMemberIds).isEqualTo(expectedMemberIds);
        List<Object> restoredVotingMemberIds = state.getInitialGroupMembers().getVotingMembers().stream()
                .map(RaftEndpoint::getId).collect(Collectors.toList());
        List<Object> expectedVotingMemberIds = expected.getVotingMembers().stream().map(RaftEndpoint::getId)
                .collect(Collectors.toList());
        assertThat(restoredVotingMemberIds).isEqualTo(expectedVotingMemberIds);
    }

    private static LogEntry logEntry(long index, int term) {
        return RAFT_MODEL_FACTORY.createLogEntryBuilder().setIndex(index).setTerm(term).setOperation(index + " " + term)
                .build();
    }

    private static SnapshotChunk snapshotChunk(long index, int term, int snapshotChunkIndex, int snapshotChunkCount) {
        return RAFT_MODEL_FACTORY.createSnapshotChunkBuilder().setIndex(index).setTerm(term)
                .setSnapshotChunkIndex(snapshotChunkIndex).setSnapshotChunkCount(snapshotChunkCount)
                .setOperation(index + "-" + term + "-" + snapshotChunkIndex + "-" + snapshotChunkCount)
                .setGroupMembersView(INITIAL_GROUP_MEMBERS).build();
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

        @Override
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
                .addMixIn(DefaultSnapshotChunkOrBuilder.class, SnapshotChunkMixin.class)
                .addMixIn(DefaultRaftEndpointPersistentStateOrBuilder.class, RaftEndpointPersistentStateMixin.class)
                .addMixIn(DefaultRaftTermPersistentStateOrBuilder.class, RaftTermPersistentStateMixin.class);

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

    @JsonDeserialize(builder = DefaultRaftEndpointPersistentStateOrBuilder.class)
    private static final class RaftEndpointPersistentStateMixin {
        @JsonDeserialize(as = LocalRaftEndpoint.class)
        private RaftEndpoint localEndpoint;
    }

    @JsonDeserialize(builder = DefaultRaftTermPersistentStateOrBuilder.class)
    private static final class RaftTermPersistentStateMixin {
        @JsonDeserialize(as = LocalRaftEndpoint.class)
        private RaftEndpoint votedFor;
    }

}
