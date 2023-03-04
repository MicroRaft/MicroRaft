package io.microraft.store.sqlite;

import java.io.File;
import java.sql.Connection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.CloseableDSLContext;
import org.jooq.Converter;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.LockingMode;
import org.sqlite.SQLiteConfig.Pragma;
import org.sqlite.SQLiteConfig.SynchronousMode;

import io.microraft.RaftEndpoint;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RaftStoreSerializer;
import io.microraft.persistence.RestoredRaftState;

/**
 * An implementation of a RaftStore which uses SQLite for persistence. A user of
 * this class is advised to construct this class, and then use
 * {@link RaftSqliteStore#getRestoredRaftState(boolean)} to acquire any
 * previously persisted state.
 * <p>
 * At time of writing, this store prioritizes:
 * <ul>
 * <li>Being reasonably low overhead on individual writes. It should not be a
 * major source of overhead when replicating messages.</li>
 * <li>Being as straightforward as possible with all other operations.</li>
 * </ul>
 * There are three tables which exist in this store.
 * <ol>
 * <li>logEntries stores the log entries</li>
 * <li>snapshots and snapshotChunks store which snapshots exist and pointers to
 * their chunks</li>
 * <li>kv stores the remaining metadata in a single-row, one-column-per-value
 * 'key-value store'.</li>
 * </ol>
 */
public final class RaftSqliteStore implements RaftStore, RaftNodeLifecycleAware {

    private static final Table<Record> KV = DSL.table("kv");

    private static final Field<String> KEY = DSL.field("key", SQLDataType.VARCHAR);
    private static final String PK = "pk";
    private static final Field<Boolean> LOCAL_ENDPOINT_VOTING = DSL.field("localEndpointVoting", SQLDataType.BOOLEAN);
    private static final Field<Integer> TERM = DSL.field("term", SQLDataType.INTEGER);
    private static final Table<Record> LOG_ENTRIES = DSL.table("logEntries");
    private static final Field<Long> INDEX = DSL.field("logIndex", SQLDataType.BIGINT);
    private static final Table<Record> SNAPSHOT_CHUNKS = DSL.table("snapshotChunks");
    private static final Field<Integer> CHUNK_INDEX = DSL.field("chunkIndex", SQLDataType.INTEGER);
    private static final Field<Integer> CHUNK_COUNT = DSL.field("chunkCount", SQLDataType.INTEGER);
    private static final Name COUNT = DSL.name("count");

    private final Field<RaftGroupMembersView> initialGroupMembersField;
    private final Field<RaftEndpoint> localEndpointField;
    private final Field<RaftEndpoint> votedForField;
    private final Field<LogEntry> logEntryField;
    private final Field<SnapshotChunk> chunkField;
    private final CloseableDSLContext dsl;
    private final RaftModelFactory raftModelFactory;

    public RaftSqliteStore(CloseableDSLContext dsl, RaftStoreSerializer modelSerializer,
            RaftModelFactory raftModelFactory) {
        this.dsl = dsl;
        this.raftModelFactory = raftModelFactory;
        initialGroupMembersField = DSL.field("initialGroupMembers",
                SQLDataType.BINARY.asConvertedDataType(new JooqConverterAdapter<>(
                        modelSerializer.raftGroupMembersViewSerializer(), RaftGroupMembersView.class)));
        logEntryField = DSL.field("logEntry", SQLDataType.BINARY
                .asConvertedDataType(new JooqConverterAdapter<>(modelSerializer.logEntrySerializer(), LogEntry.class)));
        localEndpointField = DSL.field("localEndpoint", SQLDataType.BINARY.asConvertedDataType(
                new JooqConverterAdapter<>(modelSerializer.raftEndpointSerializer(), RaftEndpoint.class)));
        votedForField = DSL.field("votedFor", SQLDataType.BINARY.asConvertedDataType(
                new JooqConverterAdapter<>(modelSerializer.raftEndpointSerializer(), RaftEndpoint.class)));
        chunkField = DSL.field("chunk", SQLDataType.BINARY.asConvertedDataType(
                new JooqConverterAdapter<>(modelSerializer.snapshotChunkSerializer(), SnapshotChunk.class)));
    }

    private void createTablesIfNotExists() {
        dsl.createTableIfNotExists(KV).column(KEY).column(localEndpointField).column(LOCAL_ENDPOINT_VOTING)
                .column(initialGroupMembersField).column(TERM).column(votedForField).primaryKey(KEY).execute();
        dsl.insertInto(KV).columns(KEY).values(PK).onConflictDoNothing().execute();

        dsl.createTableIfNotExists(LOG_ENTRIES).column(INDEX).column(logEntryField).primaryKey(INDEX).execute();

        dsl.createTableIfNotExists(SNAPSHOT_CHUNKS).columns(INDEX, CHUNK_INDEX).column(CHUNK_COUNT).column(chunkField)
                .primaryKey(INDEX, CHUNK_INDEX).execute();

        dsl.connection(Connection::commit);
    }

    /**
     * Creates and initializes the SQLite based RaftStore implementation.
     */
    public static RaftSqliteStore create(File sqliteDb, RaftModelFactory raftModelFactory,
            RaftStoreSerializer modelSerializer) {
        SQLiteConfig config = new SQLiteConfig();
        // https://www.sqlite.org/pragma.html#pragma_journal_mode
        config.setPragma(Pragma.JOURNAL_MODE, JournalMode.WAL.getValue());
        // https://www.sqlite.org/pragma.html#pragma_locking_mode
        // database file will be owned by the connection for the whole life-time of the
        // connection.
        config.setPragma(Pragma.LOCKING_MODE, LockingMode.EXCLUSIVE.getValue());
        // https://www.sqlite.org/pragma.html#pragma_synchronous
        config.setPragma(Pragma.SYNCHRONOUS, "EXTRA");

        CloseableDSLContext dsl = DSL.using(jdbcUrl(sqliteDb), config.toProperties());
        dsl.connection(conn -> conn.setAutoCommit(false));

        RaftSqliteStore store = new RaftSqliteStore(dsl, modelSerializer, raftModelFactory);
        store.createTablesIfNotExists();
        return store;
    }

    private static String jdbcUrl(File file) {
        return "jdbc:sqlite:" + file;
    }

    @Override
    public void onRaftNodeTerminate() {
        dsl.connection(Connection::rollback);
        dsl.close();
    }

    @Override
    public void persistAndFlushLocalEndpoint(@Nonnull RaftEndpointPersistentState localEndpointPersistentState) {
        dsl.update(KV).set(localEndpointField, localEndpointPersistentState.getLocalEndpoint())
                .set(LOCAL_ENDPOINT_VOTING, localEndpointPersistentState.isVoting()).execute();
        dsl.connection(Connection::commit);
    }

    @Override
    public void persistAndFlushInitialGroupMembers(@Nonnull RaftGroupMembersView initialGroupMembers) {
        dsl.update(KV).set(initialGroupMembersField, initialGroupMembers).execute();
        dsl.connection(Connection::commit);
    }

    @Override
    public void persistAndFlushTerm(@Nonnull RaftTermPersistentState termPersistentState) {
        dsl.update(KV).set(TERM, termPersistentState.getTerm()).set(votedForField, termPersistentState.getVotedFor())
                .execute();
        dsl.connection(Connection::commit);
    }

    @Override
    @SuppressWarnings("VarUsage")
    public void persistLogEntries(@Nonnull List<LogEntry> logEntries) {
        var statement = dsl.insertInto(LOG_ENTRIES, INDEX, logEntryField);
        for (LogEntry entry : logEntries) {
            statement.values(entry.getIndex(), entry);
        }
        statement.onDuplicateKeyIgnore().execute();
    }

    @Override
    public void persistSnapshotChunk(@Nonnull SnapshotChunk snapshotChunk) {
        dsl.insertInto(SNAPSHOT_CHUNKS, INDEX, CHUNK_INDEX, CHUNK_COUNT, chunkField).values(snapshotChunk.getIndex(),
                snapshotChunk.getSnapshotChunkIndex(), snapshotChunk.getSnapshotChunkCount(), snapshotChunk)
                .onDuplicateKeyIgnore().execute();
    }

    // Visible for testing
    Optional<Long> getMaxCommittedSnapshotIndex() {
        return Optional.ofNullable(dsl.select(DSL.max(INDEX).as(INDEX)).from(completedSnapshots()).fetchOne(INDEX));
    }

    private static <T> Field<T> qualify(Table<?> table, Field<T> field) {
        return DSL.field(DSL.name(table.$name(), field.$name()), field.getType());
    }

    @Override
    public void truncateLogEntriesFrom(@Nonnegative long logIndexInclusive) {
        dsl.deleteFrom(LOG_ENTRIES).where(INDEX.greaterOrEqual(logIndexInclusive)).execute();
    }

    @Override
    public void truncateLogEntriesUntil(@Nonnegative long logIndexInclusive) {
        dsl.deleteFrom(LOG_ENTRIES).where(INDEX.lessOrEqual(logIndexInclusive)).execute();
    }

    /**
     * This returns the completed snapshot offers by returning those snapshots for
     * which the number of persisted chunks is equal to the total number of chunks.
     */
    @SuppressWarnings("VarUsage")
    private Select<? extends Record1<Long>> completedSnapshots() {
        Table<?> tempTable = DSL.table("t");
        var blocks = dsl.select(qualify(SNAPSHOT_CHUNKS, INDEX), DSL.count(qualify(SNAPSHOT_CHUNKS, INDEX)).as(COUNT))
                .from(SNAPSHOT_CHUNKS).groupBy(qualify(SNAPSHOT_CHUNKS, INDEX));
        return dsl.select(qualify(tempTable, INDEX)).from(blocks.asTable(tempTable), SNAPSHOT_CHUNKS)
                .where(qualify(tempTable, INDEX).eq(qualify(SNAPSHOT_CHUNKS, INDEX)))
                .and(DSL.field(COUNT).eq(CHUNK_COUNT));
    }

    @Override
    public void deleteSnapshotChunks(@Nonnegative long logIndex, @Nonnegative int snapshotChunkCount) {
        dsl.deleteFrom(SNAPSHOT_CHUNKS).where(qualify(SNAPSHOT_CHUNKS, INDEX).eq(logIndex)).execute();
    }

    @Override
    public void flush() {
        dsl.connection(Connection::commit);
    }

    public Optional<RestoredRaftState> getRestoredRaftState(boolean truncateStaleData) {
        var record = dsl
                .select(localEndpointField, LOCAL_ENDPOINT_VOTING, initialGroupMembersField, TERM, votedForField)
                .from(KV).fetchOne();

        if (record == null) {
            // This case isn't expected to happen since the row is created in the
            // initialization loop. However, a failure at the wrong time on startup could
            // cause it to occur since SQLite doesn't have transactional DDL.
            return Optional.empty();
        }

        if (record.get(localEndpointField) == null || record.get(LOCAL_ENDPOINT_VOTING) == null
                || record.get(initialGroupMembersField) == null || record.get(TERM) == null
                || record.get(votedForField) == null) {
            checkState((record.get(localEndpointField) == null) == (record.get(LOCAL_ENDPOINT_VOTING) == null),
                    "expected localEndpoint and localEndpointVoting to be both unset, or neither unset");
            checkState((record.get(TERM) != null) || (record.get(votedForField) == null),
                    "expected since voted for is set, term should be unset");
            checkState(
                    record.get(initialGroupMembersField) != null
                            || (record.get(TERM) == null && record.get(localEndpointField) == null),
                    "expected initial group members and local endpoint fields to be set "
                            + "before this node can vote");
            return Optional.empty();
        }

        Optional<SnapshotEntry> snapshot = getMaxCommittedSnapshotIndex().map(lastSnapshotted -> {
            List<SnapshotChunk> snapshotChunks = dsl.select(chunkField).from(SNAPSHOT_CHUNKS)
                    .where(INDEX.eq(lastSnapshotted)).orderBy(CHUNK_INDEX).fetch(chunkField);
            snapshotChunks.sort(Comparator.comparingInt(SnapshotChunk::getSnapshotChunkIndex));

            return raftModelFactory.createSnapshotEntryBuilder().setSnapshotChunks(snapshotChunks)
                    .setIndex(lastSnapshotted).setTerm(snapshotChunks.get(0).getTerm())
                    .setGroupMembersView(snapshotChunks.get(0).getGroupMembersView()).build();
        });

        if (truncateStaleData) {
            snapshot.map(SnapshotEntry::getIndex).ifPresent(this::truncateUntil);
        }

        RaftEndpointPersistentState localEndpointPersistentState = raftModelFactory
                .createRaftEndpointPersistentStateBuilder().setLocalEndpoint(record.get(localEndpointField))
                .setVoting(record.get(LOCAL_ENDPOINT_VOTING)).build();
        RaftTermPersistentState termPersistentState = raftModelFactory.createRaftTermPersistentStateBuilder()
                .setTerm(record.get(TERM)).setVotedFor(record.get(votedForField)).build();

        return Optional.of(new RestoredRaftState(localEndpointPersistentState, record.get(initialGroupMembersField),
                termPersistentState, snapshot.orElse(null),
                dsl.select(logEntryField).from(LOG_ENTRIES).orderBy(INDEX).fetch(logEntryField)));
    }

    // Visible for testing
    List<SnapshotChunk> getAllSnapshotChunks() {
        return dsl.select(chunkField).from(SNAPSHOT_CHUNKS).orderBy(INDEX, CHUNK_INDEX).fetch(chunkField);
    }

    // Visible for testing
    void rawFlush() {
        dsl.connection(Connection::commit);
    }

    private static void checkState(boolean condition, String errorMessage) {
        if (!condition) {
            throw new IllegalStateException(errorMessage);
        }
    }

    private void truncateUntil(long snapshotIndex) {
        // we know there is a snapshot persisted successfully at the given index,
        // so we can delete everything before it.
        truncateLogEntriesUntil(snapshotIndex);
        dsl.deleteFrom(SNAPSHOT_CHUNKS).where(INDEX.lessThan(snapshotIndex)).execute();
        dsl.connection(Connection::commit);
    }

    private static final class JooqConverterAdapter<T> implements Converter<byte[], T> {
        private final RaftStoreSerializer.Serializer<T> serializer;
        private final Class<T> clazz;

        private JooqConverterAdapter(RaftStoreSerializer.Serializer<T> serializer, Class<T> clazz) {
            this.serializer = serializer;
            this.clazz = clazz;
        }

        @Override
        @Nullable
        public T from(@Nullable byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            return serializer.deserialize(bytes);
        }

        @Override
        public byte[] to(T element) {
            if (element == null) {
                return null;
            }
            return serializer.serialize(element);
        }

        @Override
        public Class<byte[]> fromType() {
            return byte[].class;
        }

        @Override
        public Class<T> toType() {
            return clazz;
        }
    }
}
