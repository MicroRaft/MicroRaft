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

package io.microraft.report;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.microraft.RaftConfig;
import io.microraft.RaftConfig.RaftConfigBuilder;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.statemachine.StateMachine;

/**
 * Contains information about a Raft node's local state related to the execution
 * of the Raft consensus algorithm.
 * <p>
 * Raft node reports are published either periodically or when there is a change
 * in the local state of a Raft node related to the Raft consensus algorithm.
 * The duration of the periodic report publishing is configured via
 * {@link RaftConfigBuilder#setRaftNodeReportPublishPeriodSecs(int)}.
 *
 * @see RaftNodeReportListener
 * @see RaftConfig
 * @see RaftNodeReportReason
 * @see RaftRole
 * @see RaftNodeStatus
 * @see RaftGroupMembers
 * @see RaftTerm
 * @see RaftLogStats
 */
public interface RaftNodeReport {

    RaftNodeReportReason getReason();

    /**
     * Returns the unique ID of the Raft group that this Raft node belongs to.
     *
     * @return the unique ID of the Raft group that this Raft node belongs to
     */
    @Nonnull
    Object getGroupId();

    /**
     * Returns the local endpoint of the Raft node.
     *
     * @return the local endpoint of the Raft node
     */
    @Nonnull
    RaftEndpoint getEndpoint();

    /**
     * Returns the initial members of the Raft group this Raft node belongs to.
     *
     * @return the initial members of the Raft group this Raft node belongs to
     */
    @Nonnull
    RaftGroupMembers getInitialMembers();

    /**
     * Returns the last committed member list of the Raft group this Raft node
     * belongs to.
     * <p>
     * Please note that the returned member list is read from the local state and
     * can be different from the currently effective applied member list, if there
     * is an ongoing (appended but not-yet committed) membership change in the
     * group. It can be different from the current committed member list of the Raft
     * group, also if a new membership change is committed by other Raft nodes of
     * the group but not learnt by this Raft node yet.
     *
     * @return the last committed member list of the Raft group this Raft node
     *         belongs to
     */
    @Nonnull
    RaftGroupMembers getCommittedMembers();

    /**
     * Returns the currently effective member list of the Raft group this Raft node
     * belongs to.
     * <p>
     * Please note that the returned member list is read from the local state and
     * can be different from the committed member list, if there is an ongoing
     * (appended but not-yet committed) membership change in the Raft group.
     *
     * @return the currently effective member list of the Raft group this Raft node
     *         belongs to
     */
    @Nonnull
    RaftGroupMembers getEffectiveMembers();

    /**
     * Returns the role of the Raft node in the current term. If the returned role
     * is {@link RafRole#LEADER}, it means the local Raft node has received
     * heartbeats from the majority in the last
     * {@link RaftConfig#leaderHeartbeatTimeoutSecs} seconds.
     *
     * @return the role of the Raft node in the current term
     */
    @Nonnull
    RaftRole getRole();

    /**
     * Returns the status of the Raft node.
     *
     * @return the status of the Raft node
     */
    @Nonnull
    RaftNodeStatus getStatus();

    /**
     * Returns the locally known term information.
     * <p>
     * Please note that other nodes may have already switched to a higher term.
     *
     * @return the locally known term information
     */
    @Nonnull
    RaftTerm getTerm();

    /**
     * Returns statistics about a Raft node's local Raft log.
     *
     * @return statistics about a Raft node's local Raft log
     */
    @Nonnull
    RaftLogStats getLog();

    /**
     * Returns timestamps of latest heartbeats sent by the non-leader nodes to the
     * leader Raft node, including both {@link RaftRole#FOLLOWER} and
     * {@link RaftRole#LEARNER} nodes. The leader node's RaftEndpoint is not present
     * in the map. This map is returned non-empty only by the leader Raft node.
     *
     * @return timestamps of latest heartbeats sent by the non-leader nodes
     */
    @Nonnull
    Map<RaftEndpoint, Long> getHeartbeatTimestamps();

    /**
     * Returns earliest heartbeat timestamp of the replication quorum. This method
     * returns a non-empty value only for the leader Raft node. For instance, this
     * method returns 8 for the following heartbeat timestamps of 5 Raft nodes, A
     * (leader) ts = -, B (follower) ts = 10, C (follower) ts = 8, D (follower) ts =
     * 6, E (follower) ts = 4. Please note that {@link RaftRole#LEARNER} nodes and
     * their heartbeats are excluded in quorum calculations.
     *
     * @return earliest heartbeat timestamp of the replication quorum
     */
    @Nonnull
    Optional<Long> getQuorumHeartbeatTimestamp();

    /**
     * Returns timestamp of the latest heartbeat received from the leader Raft node.
     * This method returns a non-empty value only from a non-leader Raft node if it
     * has ever received a heartbeat from the leader.
     *
     * @return timestamp of the latest heartbeat received from the leader Raft node
     */
    @Nonnull
    Optional<Long> getLeaderHeartbeatTimestamp();

    /**
     * Denotes the reason for a given report
     */
    enum RaftNodeReportReason {

        /**
         * The report is created on a periodic reporting tick of the {@link RaftNode}.
         */
        PERIODIC,

        /**
         * The report is created when a {@link RaftNode} changes its
         * {@link RaftNodeStatus}.
         */
        STATUS_CHANGE,

        /**
         * The report is created on when a {@link RaftNode} changes its role or
         * discovers the leader in the current term.
         */
        ROLE_CHANGE,

        /**
         * The report is created when a {@link RaftNode} moves to a new Raft group
         * member list.
         */
        GROUP_MEMBERS_CHANGE,

        /**
         * The report is created when a {@link RaftNode} takes a snapshot of the
         * {@link StateMachine}
         */
        TAKE_SNAPSHOT,

        /**
         * The report is created when a {@link RaftNode} installs a snapshot that is
         * sent by the current {@link RaftRole#LEADER}.
         */
        INSTALL_SNAPSHOT,

        /**
         * The report is created for a {@link RaftNode#getReport()} call.
         */
        API_CALL

    }

}
