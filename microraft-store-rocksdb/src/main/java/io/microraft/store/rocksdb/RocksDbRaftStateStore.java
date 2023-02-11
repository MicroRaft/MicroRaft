package io.microraft.store.rocksdb;

import io.microraft.exception.RaftException;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.persistence.RaftStoreSerializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.WriteOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.Optional;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDbRaftStateStore implements RaftStore, RaftNodeLifecycleAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbRaftStateStore.class);

    static {
        RocksDB.loadLibrary();
        LOGGER.info("RocksDB is loaded.");
    }

    private static final Charset KEY_CHARSET = StandardCharsets.UTF_8;
    private static final byte[] LOCAL_ENDPOINT_STATE_KEY = "a_lcl".getBytes(KEY_CHARSET);
    private static final byte[] INITIAL_GROUP_MEMBERS_KEY = "a_mbs".getBytes(KEY_CHARSET);
    private static final byte[] TERM_STATE_KEY = "a_trm".getBytes(KEY_CHARSET);
    private static final String LOG_ENTRY_PREFIX = "e_";
    private static final String SNAPSHOT_PREFIX = "s_";
    private static final String SNAPSHOT_KEY_SEPERATOR = "_";
    // We do not need WAL since MicroRaft explicitly calls flush()
    // to ensure safety of persisted data. See
    // https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log
    // https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes
    private static final WriteOptions ASYNC_WRITE = new WriteOptions().setSync(false).setDisableWAL(true);
    // private static final WriteOptions ASYNC_WRITE = new
    // WriteOptions().setSync(false);
    // Sync write ensures the data is fdatasync()'ed to the disk.
    private static final WriteOptions SYNC_WRITE = new WriteOptions().setSync(true);

    private static class SnapshotPersistenceState {
        final long logIndex;
        final int snapshotChunkCount;
        final Set<Integer> pendingSnapshotChunkIndices = new HashSet<>();

        SnapshotPersistenceState(long logIndex, int snapshotChunkCount) {
            this.logIndex = logIndex;
            this.snapshotChunkCount = snapshotChunkCount;
            for (int snapshotChunkIndex = 0; snapshotChunkIndex < snapshotChunkCount; snapshotChunkIndex++) {
                pendingSnapshotChunkIndices.add(snapshotChunkIndex);
            }
        }

        void snapshotChunkPersisted(int snapshotIndex) {
            pendingSnapshotChunkIndices.remove(snapshotIndex);
        }

        boolean isSnapshotCompleted() {
            return pendingSnapshotChunkIndices.isEmpty();
        }

        @Override
        public String toString() {
            return "SnapshotPersistenceState{logIndex=" + logIndex + ", snapshotChunkCount=" + snapshotChunkCount
                    + ", pendingSnapshotChunkIndices=" + pendingSnapshotChunkIndices + "}";
        }
    }

    static final byte[] getLogEntryKey(long index) {
        String key = LOG_ENTRY_PREFIX + index;
        return key.getBytes(KEY_CHARSET);
    }

    static final byte[] getSnapshotKey(long index, int snapshotChunkIndex) {
        String key = SNAPSHOT_PREFIX + index + SNAPSHOT_KEY_SEPERATOR + snapshotChunkIndex;
        return key.getBytes(KEY_CHARSET);
    }

    private final String dbDirPath;
    private final RaftStoreSerializer serializer;
    private final RaftModelFactory modelFactory;
    private final Options dbOptions = new Options();
    private final boolean skipInitOnRaftNodeStart;
    private RocksDB db;
    private SnapshotPersistenceState ongoingSnapshot;

    public static RocksDbRaftStateStore create(String dbDirPath, RaftStoreSerializer serializer,
            RaftModelFactory modelFactory) {
        try {
            Files.createDirectories(Paths.get(dbDirPath));
            File dbDir = new File(dbDirPath);
            LOGGER.info("DIR: " + dbDir);
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
            RocksDbRaftStateStore store = new RocksDbRaftStateStore(dbDirPath, serializer, modelFactory, true);
            store.init();
            return store;
        } catch (IOException e) {
            throw new RaftException("Could not create RocksDBRaftStateStore at: " + dbDirPath, null, e);
        }
    }

    public RocksDbRaftStateStore(String dbDirPath, RaftStoreSerializer serializer, RaftModelFactory modelFactory) {
        this(dbDirPath, serializer, modelFactory, false);
    }

    private RocksDbRaftStateStore(String dbDirPath, RaftStoreSerializer serializer, RaftModelFactory modelFactory,
            boolean skipInitOnRaftNodeStart) {
        this.dbDirPath = dbDirPath;
        this.serializer = serializer;
        this.modelFactory = modelFactory;
        this.dbOptions.setCreateIfMissing(true);
        this.dbOptions.setWriteBufferSize(4 * 1024 * 1024);
        this.dbOptions.setDbWriteBufferSize(16 * 1024 * 1024);
        this.dbOptions.setTargetFileSizeBase(64 * 1024 * 1024);
        this.dbOptions.setMaxBytesForLevelBase(512 * 1024 * 1024);
        // this.dbOptions.optimizeLevelStyleCompaction();
        // this.dbOptions.optimizeForSmallDb();
        this.skipInitOnRaftNodeStart = skipInitOnRaftNodeStart;
    }

    @Override
    public void onRaftNodeStart() {
        if (skipInitOnRaftNodeStart) {
            return;
        }
        init();
    }

    void init() {
        try {
            db = RocksDB.open(dbOptions, dbDirPath);
            LOGGER.info("RocksDB initialized for dir: {}", dbDirPath);
        } catch (RocksDBException e) {
            LOGGER.error("Error initializng RocksDB at: {}. Exception: '{}', message: '{}'", dbDirPath, e.getCause(),
                    e.getMessage(), e);
            throw new RaftException(e);
        }
    }

    @Override
    public void onRaftNodeTerminate() {
        close(true);
    }

    public void close(boolean flush) {
        if (flush) {
            try {
                flush();
                LOGGER.info("Flushed dir: {} before close.", dbDirPath);
            } catch (IOException e) {
                throw new RaftException(e);
            }
        }

        try {
            db.closeE();
            LOGGER.info("Closed db at dir: {}.", dbDirPath);
        } catch (RocksDBException e) {
            LOGGER.error("Error closing db at dir: {}", dbDirPath, e);
        }
    }

    @Override
    public void persistAndFlushLocalEndpoint(@Nonnull RaftEndpointPersistentState localEndpointPersistentState)
            throws IOException {
        persistAndFlush(LOCAL_ENDPOINT_STATE_KEY,
                serializer.raftEndpointPersistentStateSerializer().serialize(localEndpointPersistentState),
                localEndpointPersistentState);
    }

    @Override
    public void persistAndFlushInitialGroupMembers(@Nonnull RaftGroupMembersView initialGroupMembers)
            throws IOException {
        persistAndFlush(INITIAL_GROUP_MEMBERS_KEY,
                serializer.raftGroupMembersViewSerializer().serialize(initialGroupMembers), initialGroupMembers);
    }

    @Override
    public void persistAndFlushTerm(@Nonnull RaftTermPersistentState termPersistentState) throws IOException {
        persistAndFlush(TERM_STATE_KEY, serializer.raftTermPersistentState().serialize(termPersistentState),
                termPersistentState);
    }

    @Override
    public void persistLogEntry(@Nonnull LogEntry entry) throws IOException {
        try {
            db.put(ASYNC_WRITE, getLogEntryKey(entry.getIndex()), serializer.logEntrySerializer().serialize(entry));
        } catch (RocksDBException e) {
            LOGGER.error("Persistance of log entry at index/term: " + entry.getIndex() + "/" + entry.getTerm()
                    + " to dir: " + dbDirPath + " has failed!", e);
            throw new IOException(e);
        }
    }

    @Override
    public void persistSnapshotChunk(@Nonnull SnapshotChunk chunk) throws IOException {
        if (ongoingSnapshot == null || ongoingSnapshot.logIndex < chunk.getIndex()) {
            ongoingSnapshot = new SnapshotPersistenceState(chunk.getIndex(), chunk.getSnapshotChunkCount());
        }

        try {
            db.put(ASYNC_WRITE, getSnapshotKey(chunk.getIndex(), chunk.getSnapshotChunkIndex()),
                    serializer.snapshotChunkSerializer().serialize(chunk));
            ongoingSnapshot.snapshotChunkPersisted(chunk.getSnapshotChunkIndex());
            LOGGER.info("Persisted snapshot chunk at index/term: {}/{} snapshot chunk index/count: {}/{} to dir: {}.",
                    chunk.getIndex(), chunk.getTerm(), chunk.getSnapshotChunkIndex(), chunk.getSnapshotChunkCount(),
                    dbDirPath);
        } catch (RocksDBException e) {
            LOGGER.error("Persistance of snapshot chunk at index/term: " + chunk.getIndex() + "/" + chunk.getTerm()
                    + " snapshot chunk index/count: " + chunk.getSnapshotChunkIndex() + "/"
                    + chunk.getSnapshotChunkCount() + " to dir: " + dbDirPath + " has failed!", e);
            throw new IOException(e);
        }
    }

    @Override
    public void truncateLogEntriesFrom(long logIndexInclusive) throws IOException {
        try {
            truncateLogEntries(logIndexInclusive, Long.MAX_VALUE);
            LOGGER.info("Log entries from index: {} at dir: {} are truncated.", logIndexInclusive, dbDirPath);
        } catch (RocksDBException e) {
            LOGGER.error("Truncation of log entries from index: " + logIndexInclusive + " at dir: " + dbDirPath
                    + "has failed!", e);
            throw new IOException(e);
        }
    }

    @Override
    public void deleteSnapshotChunks(long logIndex, int snapshotChunkCount) throws IOException {
        truncateSnapshotChunks(logIndex);
        if (ongoingSnapshot != null && ongoingSnapshot.logIndex == logIndex) {
            LOGGER.info("Snapshot chunks at index: {} is deleted.");
            ongoingSnapshot = null;
        }
    }

    private void persistAndFlush(@Nonnull byte[] key, @Nonnull byte[] value, @Nonnull Object persistedObject)
            throws IOException {
        try {
            db.put(SYNC_WRITE, key, value);
            LOGGER.info("{} is persisted to dir: {}", persistedObject, dbDirPath);
        } catch (RocksDBException e) {
            LOGGER.error("Failed to persist " + persistedObject + " at dir: " + dbDirPath, e);
            throw new IOException(e);
        }
        doFlush();
    }

    @Override
    public void flush() throws IOException {
        try {
            doFlush();
            if (ongoingSnapshot != null) {
                if (!ongoingSnapshot.isSnapshotCompleted()) {
                    String message = "Flushed the changes but there is uncompleted snapshot: " + ongoingSnapshot;
                    LOGGER.error(message);
                    throw new IllegalStateException(message);
                }

                truncateUntil(ongoingSnapshot.logIndex);
            }
        } catch (RocksDBException e) {
            throw new RaftException(e);
        }
    }

    /**
     * Visible for testing
     *
     * @throws IOException
     *             if the flush operation fails
     */
    void doFlush() throws IOException {
        FlushOptions options = new FlushOptions();
        options.setWaitForFlush(true);
        try {
            db.flush(options);
        } catch (RocksDBException e) {
            LOGGER.error("Flush failed at dir: " + dbDirPath, e);
            throw new RaftException(e);
        }
    }

    private void truncateSnapshotChunks(long logIndex) throws IOException {
        try {
            long endIndexExclusive = logIndex < Long.MAX_VALUE ? logIndex + 1 : logIndex;
            int endSnapshotChunkExclusive = logIndex < Long.MAX_VALUE ? 0 : Integer.MAX_VALUE;
            db.deleteRange(ASYNC_WRITE, getSnapshotKey(logIndex, 0),
                    getSnapshotKey(endIndexExclusive, endSnapshotChunkExclusive));
            if (ongoingSnapshot != null && ongoingSnapshot.logIndex == logIndex) {
                ongoingSnapshot = null;
            }
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    private void truncateLogEntries(long startIndexInclusive, long endIndexExclusive) throws RocksDBException {
        db.deleteRange(ASYNC_WRITE, getLogEntryKey(startIndexInclusive), getLogEntryKey(endIndexExclusive));
    }

    private void truncateUntil(long indexExclusive) throws RocksDBException {
        db.deleteRange(ASYNC_WRITE, getSnapshotKey(1, 0), getSnapshotKey(indexExclusive, 0));
        truncateLogEntries(1, indexExclusive);
    }

    public Optional<RestoredRaftState> getRestoredRaftState(boolean truncateStaleData) {
        try {
            ReadOptions options = new ReadOptions();
            options.setVerifyChecksums(true);

            RaftEndpointPersistentState localEndpointPersistentState = null;
            byte[] localEndpointPersistentStateBytes = db.get(options, LOCAL_ENDPOINT_STATE_KEY);
            if (localEndpointPersistentStateBytes != null) {
                localEndpointPersistentState = serializer.raftEndpointPersistentStateSerializer()
                        .deserialize(localEndpointPersistentStateBytes);
            }

            RaftGroupMembersView initialGroupMembers = null;
            byte[] initialGroupMembersBytes = db.get(options, INITIAL_GROUP_MEMBERS_KEY);
            if (initialGroupMembersBytes != null) {
                initialGroupMembers = serializer.raftGroupMembersViewSerializer().deserialize(initialGroupMembersBytes);
            }

            RaftTermPersistentState termPersistentState = null;
            byte[] termPersistentStateBytes = db.get(options, TERM_STATE_KEY);
            if (termPersistentStateBytes != null) {
                termPersistentState = serializer.raftTermPersistentState().deserialize(termPersistentStateBytes);
            }

            if (localEndpointPersistentState == null) {
                if (initialGroupMembers != null) {
                    throw new IllegalStateException("Cannot have initial group members " + initialGroupMembers
                            + " when local endpoint is unknown!");
                } else if (termPersistentState != null) {
                    throw new IllegalStateException(
                            "Cannot have term state: " + termPersistentState + " when local endpoint is unknown!");
                }

                return Optional.empty();
            }

            if (initialGroupMembers == null) {
                throw new IllegalStateException("Cannot restore Raft state since initial group members is unknown!");
            } else if (termPersistentState == null) {
                throw new IllegalStateException("Cannot restore Raft state since term is unknown!");
            }

            SnapshotEntry snapshotEntry = restoreSnapshotEntry(options);
            // if snapshotEntry is null, it means we are not able to load a complete
            // snapshot. if there was a partially persisted snapshot, we won't delete it
            // here.
            if (snapshotEntry != null && truncateStaleData) {
                truncateUntil(snapshotEntry.getIndex());
            }

            List<LogEntry> logEntries = restoreLogEntries(options,
                    snapshotEntry != null ? snapshotEntry.getIndex() + 1 : 1);

            LOGGER.info(
                    "Restored endpoint: {}, initial group members: {}, term: {}, snapshot at index/term: {}/{}, smallest log index/term: {}/{}, greatest log index/term: {}/{} from dir: {}.",
                    localEndpointPersistentState, initialGroupMembers, termPersistentState,
                    snapshotEntry != null ? snapshotEntry.getIndex() : "-",
                    snapshotEntry != null ? snapshotEntry.getTerm() : " -",
                    logEntries.size() > 0 ? logEntries.get(0).getIndex() : "-",
                    logEntries.size() > 0 ? logEntries.get(0).getTerm() : "-",
                    logEntries.size() > 0 ? logEntries.get(logEntries.size() - 1).getIndex() : "-",
                    logEntries.size() > 0 ? logEntries.get(logEntries.size() - 1).getTerm() : "-", dbDirPath);

            return Optional.of(new RestoredRaftState(localEndpointPersistentState, initialGroupMembers,
                    termPersistentState, snapshotEntry, logEntries));
        } catch (RocksDBException e) {
            throw new RaftException(e);
        }
    }

    @Nullable
    private SnapshotEntry restoreSnapshotEntry(ReadOptions options) {
        byte[] smallestSnapshotIndexKey = getSnapshotKey(1, 0);
        RocksIterator it = db.newIterator(options);
        it.seekForPrev(getSnapshotKey(Long.MAX_VALUE, Integer.MAX_VALUE));
        List<SnapshotChunk> chunks = new ArrayList<>();
        for (; it.isValid(); it.prev()) {
            byte[] key = it.key();
            if (Arrays.compare(key, smallestSnapshotIndexKey) < 0) {
                break;
            }
            SnapshotChunk chunk = serializer.snapshotChunkSerializer().deserialize(it.value());
            if (!chunks.isEmpty() && chunks.get(0).getIndex() > chunk.getIndex()) {
                chunks.clear();
            }

            chunks.add(chunk);
            if (chunks.size() == chunk.getSnapshotChunkCount()) {
                break;
            }
        }

        try {
            it.status();
        } catch (RocksDBException e) {
            throw new RaftException(e);
        }

        if (chunks.isEmpty() || chunks.size() < chunks.get(0).getSnapshotChunkCount()) {
            return null;
        }

        chunks.sort(Comparator.comparingInt(SnapshotChunk::getSnapshotChunkIndex));

        return modelFactory.createSnapshotEntryBuilder().setIndex(chunks.get(0).getIndex())
                .setTerm(chunks.get(0).getTerm()).setGroupMembersView(chunks.get(0).getGroupMembersView())
                .setSnapshotChunks(chunks).build();
    }

    @Nonnull
    private List<LogEntry> restoreLogEntries(ReadOptions options, long startIndexInclusive) {
        List<LogEntry> entries = new ArrayList<>();
        byte[] highestLogIndexKey = getLogEntryKey(Long.MAX_VALUE);
        RocksIterator it = db.newIterator(options);
        it.seek(getLogEntryKey(startIndexInclusive));
        for (; it.isValid(); it.next()) {
            byte[] key = it.key();
            if (Arrays.compare(key, highestLogIndexKey) > 0) {
                break;
            }
            LogEntry entry = serializer.logEntrySerializer().deserialize(it.value());
            entries.add(entry);
        }

        try {
            it.status();
        } catch (RocksDBException e) {
            throw new RaftException(e);
        }

        return entries;
    }

}
