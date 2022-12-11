package io.microraft.store.sqlite;

import io.microraft.RaftEndpoint;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;

import java.io.File;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;
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

/**
 * An implementation of a RaftStore which uses SQLite for persistence. A user of
 * this class is advised to construct this class, and then use
 * {@link RaftSqliteStore#getRestoredRaftState()} to acquire any previously
 * persisted state.
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
    private boolean tryToCleanUpOldSnapshots = false;

    public RaftSqliteStore(CloseableDSLContext dsl, StoreModelSerializer modelSerializer,
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

    public static RaftSqliteStore create(File sqliteDb, RaftModelFactory raftModelFactory,
            StoreModelSerializer modelSerializer) {
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
    public void persistAndFlushLocalEndpoint(RaftEndpoint localEndpoint, boolean localEndpointVoting) {
        dsl.update(KV).set(localEndpointField, localEndpoint).set(LOCAL_ENDPOINT_VOTING, localEndpointVoting).execute();
        dsl.connection(Connection::commit);
    }

    @Override
    public void persistAndFlushInitialGroupMembers(@Nonnull RaftGroupMembersView initialGroupMembers) {
        dsl.update(KV).set(initialGroupMembersField, initialGroupMembers).execute();
        dsl.connection(Connection::commit);
    }

    @Override
    public void persistAndFlushTerm(int term, @Nullable RaftEndpoint votedFor) {
        dsl.update(KV).set(TERM, term).set(votedForField, votedFor).execute();
        dsl.connection(Connection::commit);
    }

    @Override
    public void persistLogEntry(@Nonnull LogEntry logEntry) {
        dsl.insertInto(LOG_ENTRIES, INDEX, logEntryField).values(logEntry.getIndex(), logEntry).onDuplicateKeyIgnore()
                .execute();
    }

    @Override
    public void persistSnapshotChunk(@Nonnull SnapshotChunk snapshotChunk) {
        dsl.insertInto(SNAPSHOT_CHUNKS, INDEX, CHUNK_INDEX, CHUNK_COUNT, chunkField).values(snapshotChunk.getIndex(),
                snapshotChunk.getSnapshotChunkIndex(), snapshotChunk.getSnapshotChunkCount(), snapshotChunk)
                .onDuplicateKeyIgnore().execute();
        tryToCleanUpOldSnapshots = true;
    }

    // Visible for testing
    Optional<Long> getMaxCommittedSnapshotIndex() {
        return Optional.ofNullable(dsl.select(DSL.max(INDEX).as(INDEX)).from(completedSnapshots()).fetchOne(INDEX));
    }

    private static <T> Field<T> qualify(Table<?> table, Field<T> field) {
        return DSL.field(DSL.name(table.$name(), field.$name()), field.getType());
    }

    @Override
    public void truncateLogEntriesFrom(long logIndexInclusive) {
        dsl.deleteFrom(LOG_ENTRIES).where(INDEX.greaterOrEqual(logIndexInclusive)).execute();
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
    public void truncateSnapshotChunksUntil(long logIndexInclusive) {
        dsl.deleteFrom(SNAPSHOT_CHUNKS).where(qualify(SNAPSHOT_CHUNKS, INDEX).le(logIndexInclusive))
                .and(qualify(SNAPSHOT_CHUNKS, INDEX).notIn(completedSnapshots())).execute();
    }

    // Visible for testing
    List<SnapshotChunk> getAllSnapshotChunks() {
        return dsl.select(chunkField).from(SNAPSHOT_CHUNKS).orderBy(INDEX, CHUNK_INDEX).fetch(chunkField);
    }

    @Override
    public void flush() {
        dsl.connection(Connection::commit);
        if (tryToCleanUpOldSnapshots) {
            tryToCleanUpOldSnapshots = false;
            Optional<Long> maybeSnapshotIndex = getMaxCommittedSnapshotIndex();
            maybeSnapshotIndex.ifPresent(snapshotIndex -> {
                dsl.deleteFrom(LOG_ENTRIES).where(INDEX.lessOrEqual(snapshotIndex)).execute();
                dsl.deleteFrom(SNAPSHOT_CHUNKS).where(INDEX.lessThan(snapshotIndex)).execute();
                dsl.connection(Connection::commit);
            });
        }
    }

    public Optional<RestoredRaftState> getRestoredRaftState() {
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

            return raftModelFactory.createSnapshotEntryBuilder().setSnapshotChunks(snapshotChunks)
                    .setIndex(lastSnapshotted).setTerm(snapshotChunks.get(0).getTerm())
                    .setGroupMembersView(snapshotChunks.get(0).getGroupMembersView()).build();
        });

        return Optional.of(new RestoredRaftState(record.get(localEndpointField), record.get(LOCAL_ENDPOINT_VOTING),
                record.get(initialGroupMembersField), record.get(TERM), record.get(votedForField),
                snapshot.orElse(null),
                dsl.select(logEntryField).from(LOG_ENTRIES).orderBy(INDEX).fetch(logEntryField)));
    }

    private static void checkState(boolean condition, String errorMessage) {
        if (!condition) {
            throw new IllegalStateException(errorMessage);
        }
    }

    private static final class JooqConverterAdapter<T> implements Converter<byte[], T> {
        private final StoreModelSerializer.Serializer<T> serializer;
        private final Class<T> clazz;

        private JooqConverterAdapter(StoreModelSerializer.Serializer<T> serializer, Class<T> clazz) {
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
