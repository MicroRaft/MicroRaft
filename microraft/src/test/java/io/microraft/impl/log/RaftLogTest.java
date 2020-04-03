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

import io.microraft.impl.model.log.DefaultLogEntryOrBuilder;
import io.microraft.impl.model.log.DefaultSnapshotChunkOrBuilder;
import io.microraft.impl.model.log.DefaultSnapshotEntry.DefaultSnapshotEntryBuilder;
import io.microraft.model.log.BaseLogEntry;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author mdogan
 * @author metanet
 */
public class RaftLogTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private RaftLog log;

    @Before
    public void setUp() {
        log = RaftLog.create(100);
    }

    @Test
    public void test_initialState() {
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(0);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(0);
    }

    @Test
    public void test_appendEntries_withSameTerm() {
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build());
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build());
        LogEntry last = new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build();
        log.appendEntry(last);

        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(last.getTerm());
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(last.getIndex());
    }

    @Test
    public void test_appendEntries_withHigherTerms() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);
        LogEntry last = new DefaultLogEntryOrBuilder().setTerm(2).setIndex(4).build();
        log.appendEntry(last);

        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(last.getTerm());
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(last.getIndex());

        BaseLogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.getTerm()).isEqualTo(last.getTerm());
        assertThat(lastLogEntry.getIndex()).isEqualTo(last.getIndex());
    }

    @Test
    public void test_appendEntries_withLowerTerm() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(4).build());
    }

    @Test
    public void test_appendEntries_withLowerIndex() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build());
    }

    @Test
    public void test_appendEntries_withEqualIndex() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
    }

    @Test
    public void test_appendEntries_withGreaterIndex() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(5).build());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withSameTerm() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(3).setGroupMembers(emptySet()).build());

        LogEntry last = new DefaultLogEntryOrBuilder().setTerm(1).setIndex(4).build();
        log.appendEntry(last);

        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(last.getTerm());
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(last.getIndex());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withHigherTerm() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(3).setGroupMembers(emptySet()).build());

        LogEntry last = new DefaultLogEntryOrBuilder().setTerm(2).setIndex(4).build();
        log.appendEntry(last);

        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(last.getTerm());
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(last.getIndex());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withLowerTerm() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(2).setIndex(3).setGroupMembers(emptySet()).build());

        exception.expect(IllegalArgumentException.class);
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(4).build());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withLowerIndex() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(2).setIndex(3).setGroupMembers(emptySet()).build());

        exception.expect(IllegalArgumentException.class);
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withEqualIndex() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(2).setIndex(3).setGroupMembers(emptySet()).build());

        exception.expect(IllegalArgumentException.class);
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withGreaterIndex() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(2).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(2).setIndex(3).setGroupMembers(emptySet()).build());

        exception.expect(IllegalArgumentException.class);
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(2).setIndex(5).build());
    }

    @Test
    public void getEntry() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);

        for (int i = 1; i <= log.lastLogOrSnapshotIndex(); i++) {
            LogEntry entry = log.getLogEntry(i);
            assertThat(entry.getTerm()).isEqualTo(1);
            assertThat(entry.getIndex()).isEqualTo(i);
        }
    }

    @Test
    public void getEntryAfterSnapshot() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(3).setGroupMembers(emptySet()).build());

        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(4).build());
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(5).build());

        for (int i = 1; i <= 3; i++) {
            assertThat(log.getLogEntry(i)).isNull();
        }

        for (int i = 4; i <= log.lastLogOrSnapshotIndex(); i++) {
            LogEntry entry = log.getLogEntry(i);
            assertThat(entry.getTerm()).isEqualTo(1);
            assertThat(entry.getIndex()).isEqualTo(i);
        }
    }

    @Test
    public void getEntry_withUnknownIndex() {
        assertThat(log.getLogEntry(1)).isNull();
    }

    @Test
    public void getEntry_withZeroIndex() {
        exception.expect(IllegalArgumentException.class);
        log.getLogEntry(0);
    }

    @Test
    public void getEntriesBetween() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);

        List<LogEntry> result = log.getLogEntriesBetween(1, 3);
        assertThat(result).isEqualTo(entries);

        result = log.getLogEntriesBetween(1, 2);
        assertThat(result).isEqualTo(entries.subList(0, 2));

        result = log.getLogEntriesBetween(2, 3);
        assertThat(result).isEqualTo(entries.subList(1, 3));
    }

    @Test
    public void getEntriesBetweenAfterSnapshot() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(2).setGroupMembers(emptySet()).build());

        List<LogEntry> result = log.getLogEntriesBetween(3, 3);
        assertThat(result).isEqualTo(entries.subList(2, 3));
    }

    @Test
    public void getEntriesBetweenBeforeSnapshotIndex() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(2).setGroupMembers(emptySet()).build());

        exception.expect(IllegalArgumentException.class);
        log.getLogEntriesBetween(2, 3);
    }

    @Test
    public void truncateEntriesFrom() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(4).build());
        log.appendEntries(entries);

        List<LogEntry> truncated = log.truncateEntriesFrom(3);
        assertThat(truncated.size()).isEqualTo(2);
        assertThat(truncated).isEqualTo(entries.subList(2, 4));

        for (int i = 1; i <= 2; i++) {
            assertThat(log.getLogEntry(i)).isEqualTo(entries.get(i - 1));
        }

        assertThat(log.getLogEntry(3)).isNull();
    }

    @Test
    public void truncateEntriesFrom_afterSnapshot() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(4).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(2).setGroupMembers(emptySet()).build());

        List<LogEntry> truncated = log.truncateEntriesFrom(3);
        assertThat(truncated.size()).isEqualTo(2);
        assertThat(truncated).isEqualTo(entries.subList(2, 4));
        assertThat(log.getLogEntry(3)).isNull();
    }

    @Test
    public void truncateEntriesFrom_outOfRange() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.truncateEntriesFrom(4);
    }

    @Test
    public void truncateEntriesFrom_beforeSnapshotIndex() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build());
        log.appendEntries(entries);
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(2).setGroupMembers(emptySet()).build());

        exception.expect(IllegalArgumentException.class);
        log.truncateEntriesFrom(1);
    }

    @Test
    public void setSnapshotAtLastLogIndex_forSingleEntryLog() {
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build());
        Object chunkOperation = new Object();
        SnapshotChunk snapshotChunk = new DefaultSnapshotChunkOrBuilder().setTerm(1).setIndex(1).setOperation(chunkOperation)
                                                                         .setSnapshotChunkIndex(0).setSnapshotChunkCount(1)
                                                                         .setGroupMembers(emptySet()).build();
        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(1).setSnapshotChunks(singletonList(snapshotChunk))
                                                         .setGroupMembers(emptySet()).build());

        BaseLogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.getTerm()).isEqualTo(1);
        assertThat(lastLogEntry.getIndex()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(1);
        assertThat(log.snapshotIndex()).isEqualTo(1);

        SnapshotEntry snapshotEntry = log.snapshotEntry();
        assertThat(snapshotEntry.getTerm()).isEqualTo(1);
        assertThat(snapshotEntry.getIndex()).isEqualTo(1);
        assertThat(snapshotEntry.getOperation()).isEqualTo(singletonList(snapshotChunk));
    }

    @Test
    public void setSnapshotAtLastLogIndex_forMultiEntryLog() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(4).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(5).build());
        log.appendEntries(entries);

        log.setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(5).setGroupMembers(emptySet()).build());

        BaseLogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.getTerm()).isEqualTo(1);
        assertThat(lastLogEntry.getIndex()).isEqualTo(5);
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(5);
        assertThat(log.snapshotIndex()).isEqualTo(5);

        SnapshotEntry snapshot = log.snapshotEntry();
        assertThat(snapshot.getTerm()).isEqualTo(1);
        assertThat(snapshot.getIndex()).isEqualTo(5);
    }

    @Test
    public void setSnapshot() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(4).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(5).build());
        log.appendEntries(entries);

        int truncated = log
                .setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(3).setGroupMembers(emptySet()).build());
        assertThat(truncated).isEqualTo(3);

        for (int i = 1; i <= 3; i++) {
            assertThat(log.containsLogEntry(i)).isFalse();
            assertThat(log.getLogEntry(i)).isNull();
        }
        for (int i = 4; i <= 5; i++) {
            assertThat(log.containsLogEntry(i)).isTrue();
            assertThat(log.getLogEntry(i)).isNotNull();
        }

        BaseLogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.getTerm()).isEqualTo(1);
        assertThat(lastLogEntry.getIndex()).isEqualTo(5);
        assertThat(log.getLogEntry(lastLogEntry.getIndex())).isSameAs(lastLogEntry);
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(5);
        assertThat(log.snapshotIndex()).isEqualTo(3);

        SnapshotEntry snapshot = log.snapshotEntry();
        assertThat(snapshot.getTerm()).isEqualTo(1);
        assertThat(snapshot.getIndex()).isEqualTo(3);
    }

    @Test
    public void setSnapshot_multipleTimes() {
        List<LogEntry> entries = asList(new DefaultLogEntryOrBuilder().setTerm(1).setIndex(1).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(2).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(3).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(4).build(),
                                        new DefaultLogEntryOrBuilder().setTerm(1).setIndex(5).build());
        log.appendEntries(entries);

        int truncated = log
                .setSnapshot(new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(2).setGroupMembers(emptySet()).build());
        assertThat(truncated).isEqualTo(2);

        for (int i = 1; i <= 2; i++) {
            assertThat(log.containsLogEntry(i)).isFalse();
            assertThat(log.getLogEntry(i)).isNull();
        }
        for (int i = 3; i <= 5; i++) {
            assertThat(log.containsLogEntry(i)).isTrue();
            assertThat(log.getLogEntry(i)).isNotNull();
        }

        Object chunkOperation = new Object();
        SnapshotChunk snapshotChunk = new DefaultSnapshotChunkOrBuilder().setTerm(1).setIndex(4).setOperation(chunkOperation)
                                                                         .setSnapshotChunkIndex(0).setSnapshotChunkCount(1)
                                                                         .setGroupMembers(emptySet()).build();
        truncated = log.setSnapshot(
                new DefaultSnapshotEntryBuilder().setTerm(1).setIndex(4).setSnapshotChunks(singletonList(snapshotChunk))
                                                 .setGroupMembers(emptySet()).build());
        assertThat(truncated).isEqualTo(2);

        for (int i = 1; i <= 4; i++) {
            assertThat(log.containsLogEntry(i)).isFalse();
            assertThat(log.getLogEntry(i)).isNull();
        }

        assertThat(log.containsLogEntry(5)).isTrue();
        assertThat(log.getLogEntry(5)).isNotNull();

        BaseLogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.getTerm()).isEqualTo(1);
        assertThat(lastLogEntry.getIndex()).isEqualTo(5);
        assertThat(log.getLogEntry(lastLogEntry.getIndex())).isSameAs(lastLogEntry);
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(5);
        assertThat(log.snapshotIndex()).isEqualTo(4);

        SnapshotEntry snapshotEntry = log.snapshotEntry();
        assertThat(snapshotEntry.getTerm()).isEqualTo(1);
        assertThat(snapshotEntry.getIndex()).isEqualTo(4);
        assertThat(snapshotEntry.getOperation()).isEqualTo(singletonList(snapshotChunk));
    }

}
