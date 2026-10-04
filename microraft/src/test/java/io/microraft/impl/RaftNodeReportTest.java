/*
 * Copyright (c) 2026, MicroRaft.
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

package io.microraft.impl;

import static io.microraft.MembershipChangeMode.REMOVE_MEMBER;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftNodeStatus.TERMINATED;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.report.RaftNodeReport.RaftNodeReportReason.STATUS_CHANGE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.RaftEndpoint;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.report.RaftLogStatsImpl;
import io.microraft.impl.report.RaftNodeReportImpl;
import io.microraft.report.RaftNodeReport;
import io.microraft.test.util.BaseTest;

public class RaftNodeReportTest extends BaseTest {

    private static final long TIMEOUT_SECONDS = 10;

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void whenReportIsRequestedAfterTerminationThenCachedTerminalReportIsReturned() {
        group = LocalRaftGroup.start(1);
        RaftNodeImpl node = group.waitUntilLeaderElected();

        node.terminate().join();

        List<RaftNodeReport> terminalReports = group.getRaftNodeReports(node.getLocalEndpoint()).stream()
                .filter(report -> report.getStatus() == TERMINATED).collect(toList());
        assertThat(terminalReports).hasSize(1);

        Ordered<RaftNodeReport> first = node.getReport().join();
        Ordered<RaftNodeReport> second = node.getReport().join();
        RaftNodeReport terminalReport = terminalReports.get(0);

        assertThat(first.getCommitIndex()).isEqualTo(terminalReport.getLog().getCommitIndex());
        assertThat(first.getResult()).isSameAs(terminalReport);
        assertThat(second.getResult()).isSameAs(terminalReport);
        assertThat(terminalReport.getReason()).isEqualTo(STATUS_CHANGE);
    }

    @Test(timeout = 300_000)
    public void whenReportIsQueuedBehindTerminationThenCachedTerminalReportIsReturned() throws Exception {
        group = LocalRaftGroup.start(1);
        RaftNodeImpl node = group.waitUntilLeaderElected();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        node.getExecutor().execute(() -> {
            taskStarted.countDown();
            try {
                releaseTask.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(taskStarted.await(TIMEOUT_SECONDS, SECONDS)).isTrue();

        CompletableFuture<Ordered<Object>> terminationFuture = node.terminate();
        CompletableFuture<Ordered<RaftNodeReport>> reportFuture = node.getReport();
        releaseTask.countDown();

        terminationFuture.get(TIMEOUT_SECONDS, SECONDS);
        RaftNodeReport report = reportFuture.get(TIMEOUT_SECONDS, SECONDS).getResult();
        RaftNodeReport terminalReport = group.getRaftNodeReports(node.getLocalEndpoint()).stream()
                .filter(candidate -> candidate.getStatus() == TERMINATED).findFirst().orElseThrow();

        assertThat(report).isSameAs(terminalReport);
        assertThat(report.getReason()).isEqualTo(STATUS_CHANGE);
    }

    @Test(timeout = 300_000)
    public void whenLeaderRemovesItselfThenOneFinalTerminalReportIsRetained() {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value")).join();

        leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0).join();

        List<RaftNodeReport> terminalReports = terminalReports(leader);
        assertThat(terminalReports).hasSize(1);
        RaftNodeReport terminalReport = terminalReports.get(0);
        assertThat(terminalReport.getRole()).isEqualTo(FOLLOWER);
        assertThat(terminalReport.getReason()).isEqualTo(STATUS_CHANGE);
        assertThat(leader.getReport().join().getResult()).isSameAs(terminalReport);
    }

    @Test(timeout = 300_000)
    public void whenReportIsCreatedThenItsCollectionSnapshotsAreImmutable() {
        group = LocalRaftGroup.start(1);
        RaftNodeImpl node = group.waitUntilLeaderElected();
        RaftNodeReport current = node.getReport().join().getResult();
        RaftEndpoint remoteEndpoint = LocalRaftEndpoint.newEndpoint();
        Map<RaftEndpoint, Long> heartbeatTimestamps = new HashMap<>();
        Map<RaftEndpoint, Long> followerMatchIndices = new HashMap<>();
        heartbeatTimestamps.put(remoteEndpoint, 1L);
        followerMatchIndices.put(remoteEndpoint, 1L);
        RaftLogStatsImpl log = new RaftLogStatsImpl(node.state().commitIndex(),
                node.state().log().lastLogOrSnapshotEntry(), node.state().log().snapshotEntry(), 0, 0,
                followerMatchIndices);
        RaftNodeReport report = new RaftNodeReportImpl(current.getReason(), current.getGroupId(),
                current.getEndpoint(), current.getInitialMembers(), current.getCommittedMembers(),
                current.getEffectiveMembers(), current.getRole(), current.getStatus(), current.getTerm(), log,
                heartbeatTimestamps, current.getQuorumHeartbeatTimestamp(), current.getLeaderHeartbeatTimestamp());

        heartbeatTimestamps.put(remoteEndpoint, 2L);
        followerMatchIndices.put(remoteEndpoint, 2L);

        assertThat(report.getHeartbeatTimestamps()).containsEntry(remoteEndpoint, 1L);
        assertThat(report.getLog().getFollowerMatchIndices()).containsEntry(remoteEndpoint, 1L);
        assertThatThrownBy(() -> report.getHeartbeatTimestamps().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> report.getLog().getFollowerMatchIndices().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private List<RaftNodeReport> terminalReports(RaftNodeImpl node) {
        return group.getRaftNodeReports(node.getLocalEndpoint()).stream()
                .filter(report -> report.getStatus() == TERMINATED).collect(toList());
    }

}
