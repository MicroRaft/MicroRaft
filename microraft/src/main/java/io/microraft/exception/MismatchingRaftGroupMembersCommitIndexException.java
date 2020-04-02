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

package io.microraft.exception;

import io.microraft.RaftEndpoint;

import java.util.Collection;

/**
 * Thrown when a membership change is triggered with an expected group members
 * commit index that doesn't match the current group members commit index
 * in the local state of the Raft group leader. A group members commit index
 * is the Raft log index at which the current Raft group member list is
 * committed.
 *
 * @author mdogan
 * @author metanet
 */
public class MismatchingRaftGroupMembersCommitIndexException
        extends RaftException {
    // TODO [basri] find a shorter name

    private static final long serialVersionUID = -109570074579015635L;

    private final long commitIndex;
    private final Collection<RaftEndpoint> members;

    public MismatchingRaftGroupMembersCommitIndexException(long commitIndex, Collection<RaftEndpoint> members) {
        super("commit index: " + commitIndex + " members: " + members, null);
        this.commitIndex = commitIndex;
        this.members = members;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

}
