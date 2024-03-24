package io.microraft.impl;

import io.microraft.RaftNode;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.report.RaftNodeReport;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import java.util.List;

import static io.microraft.test.util.AssertionUtils.eventually;
import static org.assertj.core.api.Assertions.assertThat;

public class AppendEntriesTest extends BaseTest {
    private LocalRaftGroup group;

    @After
    public void tearDown() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void testAppendFailureAndRecovery() {
        group = LocalRaftGroup.start(3);
        RaftNode leader = group.waitUntilLeaderElected();

        List<RaftNode> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNode follower0 = followers.get(0);
        RaftNode follower1 = followers.get(1);

        // drop leader to follower0 AppendEntriesRequest
        group.dropMessagesTo(leader.getLocalEndpoint(), follower0.getLocalEndpoint(), AppendEntriesRequest.class);

        int operationCount = 10;
        // apply some operation
        for (int i = 0; i < operationCount; i++) {
            String value = "value";
            leader.replicate(SimpleStateMachine.applyValue(value)).join();
        }

        RaftNodeReport leaderReport = leader.getReport().join().getResult();
        long leaderLastLogIndex = leaderReport.getLog().getLastLogOrSnapshotIndex();
        assertThat(leaderLastLogIndex).isEqualTo(operationCount);

        // follower1 sync with leader
        RaftNodeReport follower1Report = follower1.getReport().join().getResult();
        long follower1LastLogIndex = follower1Report.getLog().getLastLogOrSnapshotIndex();
        assertThat(follower1LastLogIndex).isEqualTo(operationCount);

        // follower0 lagging leader
        RaftNodeReport follower0Report = follower0.getReport().join().getResult();
        long follower0LastLogIndex = follower0Report.getLog().getLastLogOrSnapshotIndex();
        assertThat(follower0LastLogIndex).isEqualTo(0);

        leader.transferLeadership(follower1.getLocalEndpoint()).join();
        RaftNode newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader.getLocalEndpoint()).isEqualTo(follower1.getLocalEndpoint());

        eventually(() -> {

            // follower0 finally catch up leader
            RaftNodeReport report = follower0.getReport().join().getResult();
            long lastLogOrSnapshotIndex = report.getLog().getLastLogOrSnapshotIndex();
            assertThat(lastLogOrSnapshotIndex).isEqualTo(operationCount);

        });
    }

}
