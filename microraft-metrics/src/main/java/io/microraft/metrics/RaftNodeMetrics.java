/*
 * Copyright (c) 2020, MicroRaft.
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

package io.microraft.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.RaftNode.RaftNodeBuilder;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReport.RaftNodeReportReason;
import io.microraft.report.RaftNodeReportListener;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Collect metrics reported by Raft nodes and publishes them to Metric registries.
 * <p>
 * A {@link RaftNodeMetrics} object can be registered to a single Raft node via
 * {@link RaftNodeBuilder#setRaftNodeReportListener(RaftNodeReportListener)}. Then, it will publish metrics from
 * published {@link RaftNodeReport} objects.
 * <p>
 * The list of metrics are as follows:
 * <ul>
 * <li>"raft.node.role": The role of the Raft node (0: leader, 1: candidate, 2: follower, 3: learner). Please see
 * {@link RaftRole}.</li>
 *
 * <li>"raft.node.status": The status of the Raft node (0: initial, 1: active, 2: updating Raft group member list, 3:
 * terminated). Please see {@link RaftNodeStatus}.</li>
 *
 * <li>"raft.node.report.reason": The reason of the last published Raft report (0: periodic, 1: Raft node status change,
 * 2: Raft role change, 3: Raft group member list change, 4: snapshot taken, 5: snapshot installed). Please see
 * {@link RaftNodeReportReason}</li>
 *
 * <li>"raft.node.term": The current term of the Raft node</li>
 *
 * <li>"raft.committed.group.members.commit.index": The Raft log commit index of the last committed Raft group
 * members.</li>
 *
 * <li>"raft.committed.group.members.size": The number of Raft nodes in the last committed Raft group member list.</li>
 *
 * <li>"raft.committed.group.members.majority": The majority number of Raft nodes in the last committed Raft group
 * member list.</li>
 *
 * <li>"raft.effective.group.members.commit.index": The Raft log commit index of the currently effective (maybe not-yet
 * committed) Raft group members.</li>
 *
 * <li>"raft.effective.group.members.size": The number of Raft nodes in the currently effective (maybe not-yet
 * committed) Raft group member list.</li>
 *
 * <li>"raft.effective.group.members.majority": The majority number of Raft nodes in the currently effective (maybe
 * not-yet committed) Raft group member list.</li>
 *
 * <li>"raft.node.commit.index": The committed Raft log index of the Raft node.</li>
 *
 * <li>"raft.node.last.log.term": The term of the last appended Raft log entry.</li>
 *
 * <li>"raft.node.last.log.index": The index of the last appended Raft log entry.</li>
 *
 * <li>"raft.node.last.snapshot.term": The term of the last snapshot taken or installed by the Raft node.</li>
 *
 * <li>"raft.node.last.snapshot.index": The Raft log index of the last snapshot taken or installed by the Raft
 * node.</li>
 *
 * <li>"raft.node.last.take.snapshot.count": The number of snapshots locally taken by the Raft node.</li>
 *
 * <li>"raft.node.last.install.snapshot.count": The number of snapshots transferred from others and installed by the
 * Raft node.</li>
 * </ul>
 *
 * @see RaftNode
 * @see RaftNodeBuilder
 * @see RaftNodeReport
 * @see RaftNodeReportListener
 */
public final class RaftNodeMetrics implements RaftNodeReportListener, MeterBinder {

    private final List<Tag> tags;
    private volatile RaftNodeReport report;
    private volatile MultiGauge followerMatchIndicesGauge;

    /**
     * Creates the object with the given Raft group id and node id strings. Those strings are attached to the published
     * metrics as "raft.group.id" and "raft.node.id" tags.
     *
     * @param groupIdStr
     *            The Raft group id to be attached as "raft.group.id" tag
     * @param raftNodeIdStr
     *            The Raft node id to be attached as "raft.node.id" rag
     */
    public RaftNodeMetrics(String groupIdStr, String raftNodeIdStr) {
        this(asList(Tag.of("raft.group.id", requireNonNull(groupIdStr)),
                Tag.of("raft.node.id", requireNonNull(raftNodeIdStr))));
    }

    /**
     * Creates the object with the given tag list to be attached to the published metrics.
     *
     * @param tags
     *            the list of tags to be attached to the published metrics.
     */
    public RaftNodeMetrics(List<Tag> tags) {
        this.tags = unmodifiableList(new ArrayList<>(requireNonNull(tags)));
    }

    @Override
    public void accept(@Nonnull RaftNodeReport report) {
        this.report = report;

        Map<RaftEndpoint, Long> followerMatchIndices = report.getLog().getFollowerMatchIndices();
        this.followerMatchIndicesGauge.register(followerMatchIndices.keySet().stream().map(endpoint -> {
            Tags followerTag = Tags.of("follower", endpoint.getId().toString());
            return MultiGauge.Row.of(followerTag, () -> getFollowerMatchIndex(endpoint));
        }).collect(toList()));
    }

    @Override
    public void bindTo(@Nonnull MeterRegistry registry) {
        registerRaftNodeMetrics(registry);
        registerRaftGroupMemberListMetrics(registry);
        registerRaftLogMetrics(registry);
    }

    private void registerRaftNodeMetrics(MeterRegistry registry) {
        Gauge.builder("raft.node.role", this::getRaftRole).tags(tags)
                .description("The role of the Raft node (0: leader, 1: candidate, 2: follower, 3: learner)")
                .register(registry);
        Gauge.builder("raft.node.status", this::getRaftNodeStatus).tags(tags).description(
                "The status of the Raft node (0: initial, 1: active, 2: updating Raft group member list, 3: terminated)")
                .register(registry);
        Gauge.builder("raft.node.report.reason", this::getRaftNodeReportPublishReason).tags(tags).description(
                "The reason of the last published Raft report (0: periodic, 1: Raft node status change, 2: Raft role change, "
                        + "3: Raft group member list change, 4: snapshot taken, 5: snapshot installed)")
                .register(registry);
        Gauge.builder("raft.node.term", this::getTerm).tags(tags).description("The current term of the Raft node")
                .register(registry);
    }

    private void registerRaftGroupMemberListMetrics(MeterRegistry registry) {
        Gauge.builder("raft.committed.group.members.commit.index", this::getCommittedRaftGroupMembersCommitIndex)
                .tags(tags).description("The Raft log commit index of the last committed Raft group members")
                .register(registry);
        Gauge.builder("raft.committed.group.members.size", this::getCommittedRaftGroupMembersSize).tags(tags)
                .description("The number of Raft nodes in the last committed Raft group member list")
                .register(registry);
        Gauge.builder("raft.committed.voting.group.members.size", this::getCommittedVotingRaftGroupMembersSize)
                .tags(tags).description("The number of voting Raft nodes in the last committed Raft group member list")
                .register(registry);
        Gauge.builder("raft.committed.group.members.majority", this::getCommittedRaftGroupMembersMajority).tags(tags)
                .description("The majority number of Raft nodes in the last committed Raft group member list")
                .register(registry);
        Gauge.builder("raft.effective.group.members.commit.index", this::getEffectiveRaftGroupMembersCommitIndex)
                .tags(tags).description("The Raft log commit index of the currently effective Raft group members")
                .register(registry);
        Gauge.builder("raft.effective.group.members.size", this::getEffectiveRaftGroupMembersSize).tags(tags)
                .description("The number of Raft nodes in the currently effective Raft group member list")
                .register(registry);
        Gauge.builder("raft.effective.voting.group.members.size", this::getEffectiveVotingRaftGroupMembersSize)
                .tags(tags)
                .description("The number of voting Raft nodes in the currently effective Raft group member list")
                .register(registry);
        Gauge.builder("raft.effective.group.members.majority", this::getEffectiveRaftGroupMembersMajority).tags(tags)
                .description("The majority number of Raft nodes in the currently effective Raft group member list")
                .register(registry);
    }

    private void registerRaftLogMetrics(MeterRegistry registry) {
        Gauge.builder("raft.log.commit.index", this, RaftNodeMetrics::getCommitIndex).tags(tags)
                .description("The committed Raft log index of the Raft node").register(registry);
        Gauge.builder("raft.log.last.term", this, RaftNodeMetrics::getLastLogOrSnapshotTerm).tags(tags)
                .description("The term of the last appended Raft log entry").register(registry);
        Gauge.builder("raft.log.last.index", this::getLastLogOrSnapshotIndex).tags(tags)
                .description("The index of the last appended Raft log entry").register(registry);
        Gauge.builder("raft.log.last.snapshot.term", this, RaftNodeMetrics::getLastSnapshotTerm).tags(tags)
                .description("The term of the last snapshot taken or installed by the Raft node").register(registry);
        Gauge.builder("raft.log.last.snapshot.index", this, RaftNodeMetrics::getLastSnapshotIndex).tags(tags)
                .description("The Raft log index of the last snapshot taken or installed by the Raft node")
                .register(registry);
        Gauge.builder("raft.log.take.snapshot.count", this, RaftNodeMetrics::getTakeSnapshotCount).tags(tags)
                .description("The number of snapshots locally taken by the Raft node").register(registry);
        Gauge.builder("raft.log.install.snapshot.count", this, RaftNodeMetrics::getInstallSnapshotCount).tags(tags)
                .description("The number of snapshots transferred from others and installed by the Raft node")
                .register(registry);
        followerMatchIndicesGauge = MultiGauge.builder("raft.log.follower.match.index")
                .description("The index of the last known appended Raft log entry on the follower").tags(tags)
                .register(registry);
    }

    private int getRaftRole() {
        return getOrDefaultValue(() -> report.getRole().ordinal(), RaftRole.FOLLOWER.ordinal());
    }

    private int getRaftNodeStatus() {
        return getOrDefaultValue(() -> report.getStatus().ordinal(), 0);
    }

    private int getRaftNodeReportPublishReason() {
        return getOrDefaultValue(() -> report.getReason().ordinal(), RaftNodeReportReason.PERIODIC.ordinal());
    }

    private int getTerm() {
        return getOrDefaultValue(() -> report.getTerm().getTerm(), 0);
    }

    private long getCommittedRaftGroupMembersCommitIndex() {
        return getOrDefaultValue(() -> report.getCommittedMembers().getLogIndex(), 0L);
    }

    private int getCommittedRaftGroupMembersSize() {
        return getOrDefaultValue(() -> report.getCommittedMembers().getMembers().size(), 0);
    }

    private int getCommittedVotingRaftGroupMembersSize() {
        return getOrDefaultValue(() -> report.getCommittedMembers().getVotingMembers().size(), 0);
    }

    private int getCommittedRaftGroupMembersMajority() {
        return getOrDefaultValue(() -> report.getCommittedMembers().getMajorityQuorumSize(), 0);
    }

    private long getEffectiveRaftGroupMembersCommitIndex() {
        return getOrDefaultValue(() -> report.getEffectiveMembers().getLogIndex(), 0L);
    }

    private int getEffectiveRaftGroupMembersSize() {
        return getOrDefaultValue(() -> report.getEffectiveMembers().getMembers().size(), 0);
    }

    private int getEffectiveVotingRaftGroupMembersSize() {
        return getOrDefaultValue(() -> report.getEffectiveMembers().getVotingMembers().size(), 0);
    }

    private int getEffectiveRaftGroupMembersMajority() {
        return getOrDefaultValue(() -> report.getEffectiveMembers().getMajorityQuorumSize(), 0);
    }

    private long getCommitIndex() {
        return getOrDefaultValue(() -> report.getLog().getCommitIndex(), 0L);
    }

    private int getLastLogOrSnapshotTerm() {
        return getOrDefaultValue(() -> report.getLog().getLastLogOrSnapshotTerm(), 0);
    }

    private long getLastLogOrSnapshotIndex() {
        return getOrDefaultValue(() -> report.getLog().getLastLogOrSnapshotIndex(), 0L);
    }

    private int getLastSnapshotTerm() {
        return getOrDefaultValue(() -> report.getLog().getLastSnapshotTerm(), 0);
    }

    private long getLastSnapshotIndex() {
        return getOrDefaultValue(() -> report.getLog().getLastSnapshotIndex(), 0L);
    }

    private int getTakeSnapshotCount() {
        return getOrDefaultValue(() -> report.getLog().getTakeSnapshotCount(), 0);
    }

    private int getInstallSnapshotCount() {
        return getOrDefaultValue(() -> report.getLog().getInstallSnapshotCount(), 0);
    }

    private long getFollowerMatchIndex(RaftEndpoint endpoint) {
        return getOrDefaultValue(() -> report.getLog().getFollowerMatchIndices().getOrDefault(endpoint, 0L), 0L);
    }

    private <T> T getOrDefaultValue(Supplier<T> supplier, T defaultValue) {
        return report != null ? supplier.get() : defaultValue;
    }

}
