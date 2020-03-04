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

package io.microraft;

/**
 * Represents result of an operation that is triggered on a Raft node via one
 * of the methods in the {@link RaftNode} interface, along with at which commit
 * index the given operation is executed / performed.
 *
 * @param <T> type of the actual result object
 * @author metanet
 */
public interface Ordered<T> {

    /**
     * Returns the commit index at which the operation is executed / performed.
     */
    long getCommitIndex();

    /**
     * Returns the actual result of the operation.
     */
    T getResult();

}
