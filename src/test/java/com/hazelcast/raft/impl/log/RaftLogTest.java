package com.hazelcast.raft.impl.log;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        log = new RaftLog(100);
    }

    @Test
    public void test_initialState() {
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(0);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(0);
    }

    @Test
    public void test_appendEntries_withSameTerm() {
        log.appendEntries(new LogEntry(1, 1, null));
        log.appendEntries(new LogEntry(1, 2, null));
        LogEntry last = new LogEntry(1, 3, null);
        log.appendEntries(last);

        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(last.term());
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(last.index());
    }

    @Test
    public void test_appendEntries_withHigherTerms() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null)};
        log.appendEntries(entries);
        LogEntry last = new LogEntry(2, 4, null);
        log.appendEntries(last);

        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(last.term());
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(last.index());

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.term()).isEqualTo(last.term());
        assertThat(lastLogEntry.index()).isEqualTo(last.index());
    }

    @Test
    public void test_appendEntries_withLowerTerm() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(2, 1, null), new LogEntry(2, 2, null), new LogEntry(2, 3, null)};
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(1, 4, null));
    }

    @Test
    public void test_appendEntries_withLowerIndex() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(2, 1, null), new LogEntry(2, 2, null), new LogEntry(2, 3, null)};
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 2, null));
    }

    @Test
    public void test_appendEntries_withEqualIndex() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(2, 1, null), new LogEntry(2, 2, null), new LogEntry(2, 3, null)};
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 3, null));
    }

    @Test
    public void test_appendEntries_withGreaterIndex() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(2, 1, null), new LogEntry(2, 2, null), new LogEntry(2, 3, null)};
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 5, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withSameTerm() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 3, null, 0, Collections.emptySet()));

        LogEntry last = new LogEntry(1, 4, null);
        log.appendEntries(last);

        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(last.term());
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(last.index());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withHigherTerm() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 3, null, 0, Collections.emptySet()));

        LogEntry last = new LogEntry(2, 4, null);
        log.appendEntries(last);

        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(last.term());
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(last.index());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withLowerTerm() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(2, 1, null), new LogEntry(2, 2, null), new LogEntry(2, 3, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(2, 3, null, 0, Collections.emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(1, 4, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withLowerIndex() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(2, 1, null), new LogEntry(2, 2, null), new LogEntry(2, 3, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(2, 3, null, 0, Collections.emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 2, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withEqualIndex() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(2, 1, null), new LogEntry(2, 2, null), new LogEntry(2, 3, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(2, 3, null, 0, Collections.emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 3, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withGreaterIndex() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(2, 1, null), new LogEntry(2, 2, null), new LogEntry(2, 3, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(2, 3, null, 0, Collections.emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 5, null));
    }

    @Test
    public void getEntry() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null)};
        log.appendEntries(entries);

        for (int i = 1; i <= log.lastLogOrSnapshotIndex(); i++) {
            LogEntry entry = log.getLogEntry(i);
            assertThat(entry.term()).isEqualTo(1);
            assertThat(entry.index()).isEqualTo(i);
        }
    }

    @Test
    public void getEntryAfterSnapshot() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 3, null, 0, Collections.emptySet()));

        log.appendEntries(new LogEntry(1, 4, null));
        log.appendEntries(new LogEntry(1, 5, null));

        for (int i = 1; i <= 3; i++) {
            assertThat(log.getLogEntry(i)).isNull();
        }

        for (int i = 4; i <= log.lastLogOrSnapshotIndex(); i++) {
            LogEntry entry = log.getLogEntry(i);
            assertThat(entry.term()).isEqualTo(1);
            assertThat(entry.index()).isEqualTo(i);
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
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null)};
        log.appendEntries(entries);

        LogEntry[] result = log.getEntriesBetween(1, 3);
        assertThat(result).isEqualTo(entries);

        result = log.getEntriesBetween(1, 2);
        assertThat(result).isEqualTo(Arrays.copyOfRange(entries, 0, 2));

        result = log.getEntriesBetween(2, 3);
        assertThat(result).isEqualTo(Arrays.copyOfRange(entries, 1, 3));
    }

    @Test
    public void getEntriesBetweenAfterSnapshot() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.emptySet()));

        LogEntry[] result = log.getEntriesBetween(3, 3);
        assertThat(result).isEqualTo(Arrays.copyOfRange(entries, 2, 3));
    }

    @Test
    public void getEntriesBetweenBeforeSnapshotIndex() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.getEntriesBetween(2, 3);
    }

    @Test
    public void truncateEntriesFrom() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3,
                null), new LogEntry(1, 4, null)};
        log.appendEntries(entries);

        List<LogEntry> truncated = log.truncateEntriesFrom(3);
        assertThat(truncated.size()).isEqualTo(2);
        assertThat(truncated.toArray()).isEqualTo(Arrays.copyOfRange(entries, 2, 4));

        for (int i = 1; i <= 2; i++) {
            assertThat(log.getLogEntry(i)).isEqualTo(entries[i - 1]);
        }

        assertThat(log.getLogEntry(3)).isNull();
    }

    @Test
    public void truncateEntriesFrom_afterSnapshot() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3,
                null), new LogEntry(1, 4, null)};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.emptySet()));

        List<LogEntry> truncated = log.truncateEntriesFrom(3);
        assertThat(truncated.size()).isEqualTo(2);
        assertThat(truncated.toArray()).isEqualTo(Arrays.copyOfRange(entries, 2, 4));
        assertThat(log.getLogEntry(3)).isNull();
    }

    @Test
    public void truncateEntriesFrom_outOfRange() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null),};
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.truncateEntriesFrom(4);
    }

    @Test
    public void truncateEntriesFrom_beforeSnapshotIndex() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3, null),};
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.truncateEntriesFrom(1);
    }

    @Test
    public void setSnapshotAtLastLogIndex_forSingleEntryLog() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null)};
        log.appendEntries(entries);
        Object snapshot = new Object();
        log.setSnapshot(new SnapshotEntry(1, 1, snapshot, 0, Collections.emptySet()));

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.term()).isEqualTo(1);
        assertThat(lastLogEntry.index()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(1);
        assertThat(log.snapshotIndex()).isEqualTo(1);

        LogEntry snapshotEntry = log.snapshot();
        assertThat(snapshotEntry.term()).isEqualTo(1);
        assertThat(snapshotEntry.index()).isEqualTo(1);
        assertThat(snapshotEntry.operation()).isEqualTo(snapshot);
    }

    @Test
    public void setSnapshotAtLastLogIndex_forMultiEntryLog() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3,
                null), new LogEntry(1, 4, null), new LogEntry(1, 5, null),};
        log.appendEntries(entries);

        log.setSnapshot(new SnapshotEntry(1, 5, null, 0, Collections.emptySet()));

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.term()).isEqualTo(1);
        assertThat(lastLogEntry.index()).isEqualTo(5);
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(5);
        assertThat(log.snapshotIndex()).isEqualTo(5);

        LogEntry snapshot = log.snapshot();
        assertThat(snapshot.term()).isEqualTo(1);
        assertThat(snapshot.index()).isEqualTo(5);
    }

    @Test
    public void setSnapshot() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3,
                null), new LogEntry(1, 4, null), new LogEntry(1, 5, null),};
        log.appendEntries(entries);

        int truncated = log.setSnapshot(new SnapshotEntry(1, 3, null, 0, Collections.emptySet()));
        assertThat(truncated).isEqualTo(3);

        for (int i = 1; i <= 3; i++) {
            assertThat(log.containsLogEntry(i)).isFalse();
            assertThat(log.getLogEntry(i)).isNull();
        }
        for (int i = 4; i <= 5; i++) {
            assertThat(log.containsLogEntry(i)).isTrue();
            assertThat(log.getLogEntry(i)).isNotNull();
        }

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.term()).isEqualTo(1);
        assertThat(lastLogEntry.index()).isEqualTo(5);
        assertThat(log.getLogEntry(lastLogEntry.index())).isSameAs(lastLogEntry);
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(5);
        assertThat(log.snapshotIndex()).isEqualTo(3);

        LogEntry snapshot = log.snapshot();
        assertThat(snapshot.term()).isEqualTo(1);
        assertThat(snapshot.index()).isEqualTo(3);
    }

    @Test
    public void setSnapshot_multipleTimes() {
        LogEntry[] entries = new LogEntry[]{new LogEntry(1, 1, null), new LogEntry(1, 2, null), new LogEntry(1, 3,
                null), new LogEntry(1, 4, null), new LogEntry(1, 5, null),};
        log.appendEntries(entries);

        int truncated = log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.emptySet()));
        assertThat(truncated).isEqualTo(2);

        for (int i = 1; i <= 2; i++) {
            assertThat(log.containsLogEntry(i)).isFalse();
            assertThat(log.getLogEntry(i)).isNull();
        }
        for (int i = 3; i <= 5; i++) {
            assertThat(log.containsLogEntry(i)).isTrue();
            assertThat(log.getLogEntry(i)).isNotNull();
        }

        Object snapshot = new Object();
        truncated = log.setSnapshot(new SnapshotEntry(1, 4, snapshot, 0, Collections.emptySet()));
        assertThat(truncated).isEqualTo(2);

        for (int i = 1; i <= 4; i++) {
            assertThat(log.containsLogEntry(i)).isFalse();
            assertThat(log.getLogEntry(i)).isNull();
        }

        assertThat(log.containsLogEntry(5)).isTrue();
        assertThat(log.getLogEntry(5)).isNotNull();

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertThat(lastLogEntry.term()).isEqualTo(1);
        assertThat(lastLogEntry.index()).isEqualTo(5);
        assertThat(log.getLogEntry(lastLogEntry.index())).isSameAs(lastLogEntry);
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(1);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(5);
        assertThat(log.snapshotIndex()).isEqualTo(4);

        LogEntry snapshotEntry = log.snapshot();
        assertThat(snapshotEntry.term()).isEqualTo(1);
        assertThat(snapshotEntry.index()).isEqualTo(4);
        assertThat(snapshotEntry.operation()).isEqualTo(snapshot);
    }

}
