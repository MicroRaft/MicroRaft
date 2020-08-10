/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright (c) 2020, MicroRaft.
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

package io.microraft;

/**
 * Statuses of a Raft node during its own and its Raft group's lifecycle.
 *
 * @see RaftNode
 */
public enum RaftNodeStatus {

    /**
     * Initial status of a Raft node. It stays in this status until it is
     * started.
     */
    INITIAL,

    /**
     * A Raft node stays in this status when there is no ongoing membership
     * change or a Raft group termination process. Operations are committed
     * in this status.
     */
    ACTIVE,

    /**
     * A Raft node moves to this status when a Raft group membership change
     * operation is appended to its Raft log and stays in this status until
     * the membership change is either committed or reverted. New operations
     * can be replicated while there is an ongoing membership change in
     * the Raft group, but no other membership change, or leadership transfer
     * can be triggered until the ongoing membership change process is
     * completed.
     */
    UPDATING_RAFT_GROUP_MEMBER_LIST,

    /**
     * A Raft node moves to this status either when it is removed from the Raft
     * group member list, or it is being terminated on its own, for instance,
     * because its JVM is shutting down.
     * <p>
     * A Raft node stops running the Raft consensus algorithm in this status.
     */
    TERMINATED;

    /**
     * Returns true if the given Raft node status is a terminal. A Raft node
     * stops running the Raft consensus algorithm in a terminal status.
     *
     * @param status
     *         the status object to check
     *
     * @return true if the given status is terminal, false otherwise
     */
    public static boolean isTerminal(RaftNodeStatus status) {
        return status == TERMINATED;
    }

}
