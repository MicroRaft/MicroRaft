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

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.integration.StateMachine;

import javax.annotation.Nonnull;

/**
 * Contains information about a Raft node's local state related
 * to the execution of the Raft consensus algorithm.
 *
 * @author metanet
 */
public interface RaftNodeReport {

    RaftNodeReportReason getReason();

    /**
     * Returns the unique ID of the Raft group that this Raft node belongs to.
     */
    @Nonnull
    Object getGroupId();

    /**
     * Returns endpoint of the Raft node
     */
    @Nonnull
    RaftEndpoint getEndpoint();

    /**
     * Returns initial members of the Raft group.
     */
    @Nonnull
    RaftGroupMembers getInitialMembers();

    /**
     * Returns the last committed member list of the Raft group this Raft node
     * belongs to. Please note that the returned member list is read from
     * the local state and can be different from the currently effective
     * applied member list, if there is an ongoing (appended but not-yet
     * committed) membership change in the group. It can be different from
     * the current committed member list of the Raft group, also if a new
     * membership change is committed by other Raft nodes of the group but
     * not learnt by this Raft node yet.
     */
    @Nonnull
    RaftGroupMembers getCommittedMembers();

    /**
     * Returns the currently effective member list of the Raft group this Raft
     * node belongs to. Please note that the returned member list is read from
     * the local state and can be different from the committed member list,
     * if there is an ongoing (appended but not-yet committed) membership
     * change in the Raft group.
     */
    @Nonnull
    RaftGroupMembers getEffectiveMembers();

    /**
     * Returns role of the Raft node in the current term
     */
    @Nonnull
    RaftRole getRole();

    /**
     * Returns status of the Raft node.
     */
    @Nonnull
    RaftNodeStatus getStatus();

    /**
     * Returns the locally known term information.
     * <p>
     * Please note that other nodes may have already switched to a higher term.
     */
    @Nonnull
    RaftGroupTerm getTerm();

    /**
     * Returns statistics about a Raft node's local Raft log.
     */
    @Nonnull
    RaftLogStats getLog();

    /**
     * Denotes the reason for a given report
     */
    enum RaftNodeReportReason {

        /**
         * The report is created when a {@link RaftNode} changes
         * its {@link RaftNodeStatus}.
         */
        STATUS_CHANGE,

        /**
         * The report is created on when a {@link RaftNode} changes its role
         * or discovers the leader in the current term.
         */
        ROLE_CHANGE,

        /**
         * The report is created when a {@link RaftNode} moves to a new Raft
         * group member list.
         */
        GROUP_MEMBERS_CHANGE,

        /**
         * The report is created when a {@link RaftNode} takes a snapshot
         * of the {@link StateMachine}
         */
        TAKE_SNAPSHOT,

        /**
         * The report is created when a {@link RaftNode} installs a snapshot
         * that is sent by the current {@link RaftRole#LEADER}.
         */
        INSTALL_SNAPSHOT,

        /**
         * The report is created on a periodic reporting tick of
         * the {@link RaftNode}.
         */
        PERIODIC,

        /**
         * The report is created for a {@link RaftNode#getReport()} call.
         */
        API_CALL
    }

}
