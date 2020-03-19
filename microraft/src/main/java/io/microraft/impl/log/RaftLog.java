/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.microraft.impl.log;

import io.microraft.exception.RaftException;
import io.microraft.impl.model.log.DefaultSnapshotEntry.DefaultSnapshotEntryBuilder;
import io.microraft.impl.util.ArrayRingbuffer;
import io.microraft.model.log.BaseLogEntry;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.persistence.NopRaftStore;
import io.microraft.persistence.RaftStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.microraft.model.log.SnapshotEntry.isNonInitial;
import static java.util.Objects.requireNonNull;

/**
 * {@code RaftLog} keeps and maintains Raft log entries and snapshot.
 * <p>
 * The entries appended in the Raft group leader's log are replicated to all
 * followers in the same log order and all Raft nodes in a Raft group
 * eventually keep the exact same copy of the Raft log.
 * <p>
 * Raft maintains the following properties, which together constitute
 * the LogMatching Property:
 * <ul>
 * <li>If two entries in different logs have the same index and term, then
 * they store the same command.</li>
 * <li>If two entries in different logs have the same index and term, then
 * the logs are identical in all preceding entries.</li>
 * </ul>
 *
 * @author mdogan
 * @author metanet
 * @see LogEntry
 * @see SnapshotEntry
 */
public class RaftLog {

    /**
     * Array of log entries stored in the Raft log.
     * <p>
     * Important: Log entry indices start from 1, not 0.
     */
    private final ArrayRingbuffer<LogEntry> log;
    /**
     * Used for reflecting log changes to persistent storage.
     */
    private final RaftStore store;
    /**
     * Latest snapshot entry
     */
    private SnapshotEntry snapshot = new DefaultSnapshotEntryBuilder().build();
    /**
     * Indicates if there is a change after the last {@link #flush()} call.
     */
    private boolean dirty;

    private RaftLog(int capacity, RaftStore store) {
        requireNonNull(store);
        this.log = new ArrayRingbuffer<>(capacity);
        this.store = store;
    }

    private RaftLog(int capacity, SnapshotEntry snapshot, List<LogEntry> entries, RaftStore store) {
        requireNonNull(store);
        this.log = new ArrayRingbuffer<>(capacity);
        long snapshotIndex;
        if (isNonInitial(snapshot)) {
            this.snapshot = snapshot;
            log.setHeadSequence(toSequence(snapshot.getIndex()) + 1);
            log.setTailSequence(log.headSequence() - 1);
            snapshotIndex = snapshot.getIndex();
        } else {
            snapshotIndex = 0;
        }

        for (LogEntry entry : entries) {
            if (entry.getIndex() > snapshotIndex) {
                log.add(entry);
            }
        }

        this.store = store;
    }

    public static RaftLog create(int capacity) {
        return create(capacity, NopRaftStore.INSTANCE);
    }

    public static RaftLog create(int capacity, RaftStore store) {
        return new RaftLog(capacity, store);
    }

    public static RaftLog restore(int capacity, SnapshotEntry snapshot, List<LogEntry> entries) {
        return restore(capacity, snapshot, entries, NopRaftStore.INSTANCE);
    }

    public static RaftLog restore(int capacity, SnapshotEntry snapshot, List<LogEntry> entries, RaftStore store) {
        return new RaftLog(capacity, snapshot, entries, store);
    }

    private long toSequence(long entryIndex) {
        return entryIndex - 1;
    }

    /**
     * Returns the log entry stored at {@code entryIndex}. Entry is retrieved
     * only from the current log, not from the snapshot entry.
     * <p>
     * If no entry available at this index, then {@code null} is returned.
     * <p>
     * Important: Log entry indices start from 1, not 0.
     */
    public LogEntry getLogEntry(long entryIndex) {
        if (entryIndex < 1) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ". Index starts from 1.");
        } else if (!containsLogEntry(entryIndex)) {
            return null;
        }

        LogEntry logEntry = log.read(toSequence(entryIndex));
        assert logEntry.getIndex() == entryIndex : "Expected: " + entryIndex + ", Entry: " + logEntry;
        return logEntry;
    }

    /**
     * Returns true if the log contains an entry at {@code entryIndex},
     * false otherwise.
     * <p>
     * Important: Log entry indices start from 1, not 0.
     */
    public boolean containsLogEntry(long entryIndex) {
        long sequence = toSequence(entryIndex);
        return sequence >= log.headSequence() && sequence <= log.tailSequence();
    }

    /**
     * Truncates log entries with indexes {@code >= entryIndex}.
     *
     * @return truncated log entries
     * @throws IllegalArgumentException If no entries are available to
     *                                  truncate, if {@code entryIndex} is
     *                                  greater than last log index or smaller
     *                                  than snapshot index.
     */
    public List<LogEntry> truncateEntriesFrom(long entryIndex) {
        if (entryIndex <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", snapshot index: " + snapshotIndex());
        } else if (entryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", last log index: " + lastLogOrSnapshotIndex());
        }

        long startSequence = toSequence(entryIndex);
        assert startSequence >= log.headSequence() : "Entry index: " + entryIndex + ", Head Seq: " + log.headSequence();

        List<LogEntry> truncated = new ArrayList<>();
        for (long ix = startSequence; ix <= log.tailSequence(); ix++) {
            truncated.add(log.read(ix));
        }
        log.setTailSequence(startSequence - 1);

        if (truncated.size() > 0) {
            dirty = true;
            try {
                store.truncateLogEntriesFrom(entryIndex);
            } catch (IOException e) {
                throw new RaftException(e);
            }
        }

        return truncated;
    }

    /**
     * Returns the last entry index in the Raft log,
     * either from the last log entry or from the last snapshot
     */
    public long lastLogOrSnapshotIndex() {
        return lastLogOrSnapshotEntry().getIndex();
    }

    /**
     * Returns the last entry in the Raft log,
     * either from the last log entry or from the last snapshot
     * if no logs are available.
     */
    public BaseLogEntry lastLogOrSnapshotEntry() {
        return !log.isEmpty() ? log.read(log.tailSequence()) : snapshot;
    }

    /**
     * Returns snapshot entry index.
     */
    public long snapshotIndex() {
        return snapshot.getIndex();
    }

    /**
     * Appends new entries to the Raft log.
     *
     * @throws IllegalArgumentException If an entry is appended with a lower
     *                                  term than the last term in the log or
     *                                  if an entry is appended with an index
     *                                  not equal to
     *                                  {@code index == lastIndex + 1}.
     */
    public void appendEntries(List<LogEntry> entries) {
        int lastTerm = lastLogOrSnapshotTerm();
        long lastIndex = lastLogOrSnapshotIndex();

        if (!checkAvailableCapacity(entries.size())) {
            throw new IllegalStateException(
                    "Not enough capacity! Capacity: " + log.getCapacity() + ", Size: " + log.size() + ", New entries:" + " "
                            + entries.size());
        }

        for (LogEntry entry : entries) {
            if (entry.getTerm() < lastTerm) {
                throw new IllegalArgumentException(
                        "Cannot append " + entry + " since its term is lower than last log term: " + lastTerm);
            } else if (entry.getIndex() != lastIndex + 1) {
                throw new IllegalArgumentException(
                        "Cannot append " + entry + " since its index is bigger than (lastLogIndex + 1): " + (lastIndex + 1));
            }

            log.add(entry);
            try {
                store.persistLogEntry(entry);
            } catch (IOException e) {
                throw new RaftException(e);
            }
            lastIndex++;
            lastTerm = Math.max(lastTerm, entry.getTerm());
        }

        dirty |= entries.size() > 0;
    }

    /**
     * Returns the last term in the Raft log,
     * either from the last log entry or from the last snapshot
     */
    public int lastLogOrSnapshotTerm() {
        return lastLogOrSnapshotEntry().getTerm();
    }

    /**
     * Returns true if the Raft log contains empty indices for the requested amount
     */
    public boolean checkAvailableCapacity(int requestedCapacity) {
        return availableCapacity() >= requestedCapacity;
    }

    /**
     * Returns the number of empty indices in the Raft log
     */
    public int availableCapacity() {
        return (int) (log.getCapacity() - log.size());
    }

    public void appendEntry(LogEntry entry) {
        if (entry == null) {
            return;
        }

        int lastTerm = lastLogOrSnapshotTerm();
        long lastIndex = lastLogOrSnapshotIndex();

        if (!checkAvailableCapacity(1)) {
            throw new IllegalStateException(
                    "Not enough capacity! Capacity: " + log.getCapacity() + ", Size: " + log.size() + ", 1 new entry!");
        } else if (entry.getTerm() < lastTerm) {
            throw new IllegalArgumentException(
                    "Cannot append " + entry + " since its term is lower than last log term: " + lastTerm);
        } else if (entry.getIndex() != lastIndex + 1) {
            throw new IllegalArgumentException(
                    "Cannot append " + entry + " since its index is bigger than (lastLogIndex + 1): " + (lastIndex + 1));
        }

        log.add(entry);
        try {
            store.persistLogEntry(entry);
        } catch (IOException e) {
            throw new RaftException(e);
        }

        dirty = true;
    }

    /**
     * Returns log entries between {@code fromEntryIndex} and {@code toEntryIndex}, both inclusive.
     *
     * @throws IllegalArgumentException If {@code fromEntryIndex} is greater
     *                                  than {@code toEntryIndex}, or
     *                                  if {@code fromEntryIndex} is equal to /
     *                                  smaller than {@code snapshotIndex},
     *                                  or if {@code fromEntryIndex} is greater
     *                                  than last log index,
     *                                  or if {@code toEntryIndex} is greater
     *                                  than last log index.
     */
    public List<LogEntry> getLogEntriesBetween(long fromEntryIndex, long toEntryIndex) {
        if (fromEntryIndex > toEntryIndex) {
            throw new IllegalArgumentException(
                    "Illegal from entry index: " + fromEntryIndex + ", to entry index: " + toEntryIndex);
        } else if (!containsLogEntry(fromEntryIndex)) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex);
        }

        long lastLogIndex = lastLogOrSnapshotIndex();
        if (fromEntryIndex > lastLogIndex) {
            throw new IllegalArgumentException(
                    "Illegal from entry index: " + fromEntryIndex + ", last log index: " + lastLogIndex);
        } else if (toEntryIndex > lastLogIndex) {
            throw new IllegalArgumentException("Illegal to entry index: " + toEntryIndex + ", last log index: " + lastLogIndex);
        }

        assert ((int) (toEntryIndex - fromEntryIndex)) >= 0 : "Int overflow! From: " + fromEntryIndex + ", to: " + toEntryIndex;
        long offset = toSequence(fromEntryIndex);
        int entryCount = (int) (toEntryIndex - fromEntryIndex + 1);
        List<LogEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            entries.add(log.read(offset + i));
        }

        return entries;
    }

    /**
     * Installs the snapshot entry and truncates log entries those are included
     * in snapshot (entries whose indexes are smaller than
     * the snapshot's index).
     *
     * @return truncated log entries after snapshot is installed
     * @throws IllegalArgumentException if the snapshot's index is smaller than
     *                                  or equal to current snapshot index
     */
    public int setSnapshot(SnapshotEntry snapshot) {
        return setSnapshot(snapshot, snapshot.getIndex());
    }

    public int setSnapshot(SnapshotEntry snapshot, long truncateUpToIndex) {
        if (snapshot.getIndex() <= snapshotIndex()) {
            throw new IllegalArgumentException(
                    "Illegal index: " + snapshot.getIndex() + ", current snapshot index: " + snapshotIndex());
        }

        long newHeadSeq = toSequence(truncateUpToIndex) + 1;
        long newTailSeq = Math.max(log.tailSequence(), newHeadSeq - 1);

        long prevSize = log.size();
        // Set truncated slots to null to reduce memory usage.
        // Otherwise this has no effect on correctness.
        for (long seq = log.headSequence(); seq < newHeadSeq; seq++) {
            log.set(seq, null);
        }

        log.setHeadSequence(newHeadSeq);
        log.setTailSequence(newTailSeq);

        this.snapshot = snapshot;
        // snapshot chunks are already persisted to disk before
        // setSnapshot() is called.
        dirty = true;

        return (int) (prevSize - log.size());
    }

    /**
     * Flushes changes to persistent storage.
     */
    public void flush() {
        if (dirty) {
            try {
                store.flush();
                dirty = false;
            } catch (IOException e) {
                throw new RaftException(e);
            }
        }
    }

    /**
     * Returns snapshot entry.
     */
    public SnapshotEntry snapshotEntry() {
        return snapshot;
    }

}
