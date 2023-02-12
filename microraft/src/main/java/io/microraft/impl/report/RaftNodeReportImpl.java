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

package io.microraft.impl.report;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftLogStats;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftTerm;

/**
 * Contains a snapshot of a Raft node's internal state.
 */
public final class RaftNodeReportImpl implements RaftNodeReport {

    private final RaftNodeReportReason reason;
    private final Object groupId;
    private final RaftEndpoint localEndpoint;
    private final RaftGroupMembers initialMembers;
    private final RaftGroupMembers committedMembers;
    private final RaftGroupMembers effectiveMembers;
    private final RaftRole role;
    private final RaftNodeStatus status;
    private final RaftTerm term;
    private final RaftLogStats log;
    private final Map<RaftEndpoint, Long> heartbeatTimestamps;
    private final Optional<Long> quorumHeartbeatTimestamp;
    private final Optional<Long> leaderHeartbeatTimestamp;

    public RaftNodeReportImpl(RaftNodeReportReason reason, Object groupId, RaftEndpoint localEndpoint,
            RaftGroupMembers initialMembers, RaftGroupMembers committedMembers, RaftGroupMembers effectiveMembers,
            RaftRole role, RaftNodeStatus status, RaftTerm term, RaftLogStats log,
            Map<RaftEndpoint, Long> heartbeatTimestamps, Optional<Long> quorumHeartbeatTimestamp,
            Optional<Long> leaderHeartbeatTimestamp) {
        this.reason = requireNonNull(reason);
        this.groupId = requireNonNull(groupId);
        this.localEndpoint = requireNonNull(localEndpoint);
        this.initialMembers = requireNonNull(initialMembers);
        this.committedMembers = requireNonNull(committedMembers);
        this.effectiveMembers = requireNonNull(effectiveMembers);
        this.role = requireNonNull(role);
        this.status = requireNonNull(status);
        this.term = requireNonNull(term);
        this.log = requireNonNull(log);
        this.heartbeatTimestamps = requireNonNull(heartbeatTimestamps);
        this.quorumHeartbeatTimestamp = requireNonNull(quorumHeartbeatTimestamp);
        this.leaderHeartbeatTimestamp = requireNonNull(leaderHeartbeatTimestamp);
    }

    @Nonnull
    @Override
    public RaftNodeReportReason getReason() {
        return reason;
    }

    @Nonnull
    @Override
    public Object getGroupId() {
        return groupId;
    }

    @Nonnull
    @Override
    public RaftEndpoint getEndpoint() {
        return localEndpoint;
    }

    @Nonnull
    @Override
    public RaftGroupMembers getInitialMembers() {
        return initialMembers;
    }

    @Nonnull
    @Override
    public RaftGroupMembers getCommittedMembers() {
        return committedMembers;
    }

    @Nonnull
    @Override
    public RaftGroupMembers getEffectiveMembers() {
        return effectiveMembers;
    }

    @Nonnull
    @Override
    public RaftRole getRole() {
        return role;
    }

    @Nonnull
    @Override
    public RaftNodeStatus getStatus() {
        return status;
    }

    @Nonnull
    @Override
    public RaftTerm getTerm() {
        return term;
    }

    @Nonnull
    @Override
    public RaftLogStats getLog() {
        return log;
    }

    @Nonnull
    @Override
    public Map<RaftEndpoint, Long> getHeartbeatTimestamps() {
        return heartbeatTimestamps;
    }

    @Nonnull
    @Override
    public Optional<Long> getQuorumHeartbeatTimestamp() {
        return quorumHeartbeatTimestamp;
    }

    @Nonnull
    @Override
    public Optional<Long> getLeaderHeartbeatTimestamp() {
        return leaderHeartbeatTimestamp;
    }

    @Override
    public String toString() {
        return "RaftNodeReport{" + "reason=" + reason + ", groupId=" + groupId + ", localEndpoint=" + localEndpoint
                + ", initialMembers=" + initialMembers + ", committedMembers=" + committedMembers
                + ", effectiveMembers=" + effectiveMembers + ", role=" + role + ", status=" + status + ", term=" + term
                + ", log=" + log + ", heartbeatTimestamps=" + heartbeatTimestamps + ", quorumHeartbeatTimestamp="
                + (quorumHeartbeatTimestamp.isPresent() ? quorumHeartbeatTimestamp.get() : "-")
                + ", leaderHeartbeatTimestamp="
                + (leaderHeartbeatTimestamp.isPresent() ? leaderHeartbeatTimestamp.get() : "-") + '}';
    }

}
