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

package io.microraft.model.groupop;

import javax.annotation.Nonnull;

/**
 * Terminates a Raft group eternally.
 * <p>
 * Termination means that once this operation is committed to a Raft group,
 * no new operation can be replicated or no new query can be executed ever.
 *
 * @author mdogan
 * @author metanet
 */
public interface TerminateRaftGroupOp
        extends RaftGroupOp {

    /**
     * The builder interface for {@link TerminateRaftGroupOp}.
     */
    interface TerminateRaftGroupOpBuilder {
        @Nonnull
        TerminateRaftGroupOp build();

    }

}
