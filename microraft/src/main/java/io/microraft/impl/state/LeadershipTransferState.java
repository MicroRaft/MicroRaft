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

package io.microraft.impl.state;

import io.microraft.RaftEndpoint;
import io.microraft.impl.util.OrderedFuture;

/**
 * State maintained by the Raft group leader during leadership transfer.
 *
 * @author mdogan
 * @author metanet
 */
public final class LeadershipTransferState {

    private int term;
    private RaftEndpoint endpoint;
    private OrderedFuture<Object> future;
    private int tryCount;

    LeadershipTransferState(int term, RaftEndpoint endpoint, OrderedFuture<Object> future) {
        this.term = term;
        this.endpoint = endpoint;
        this.future = future;
    }

    /**
     * Returns the term in which this leadership transfer process is triggered.
     */
    public int term() {
        return term;
    }

    /**
     * Returns the endpoint that is supposed to be the new Raft group leader.
     */
    public RaftEndpoint endpoint() {
        return endpoint;
    }

    /**
     * Returns if we can retry leadership transfer on the target endpoint.
     */
    public int incrementTryCount() {
        return ++tryCount;
    }

    /**
     * Completes the current leadership transfer process with the given result.
     */
    void complete(long commitIndex, Object result) {
        if (result instanceof Throwable) {
            future.fail((Throwable) result);
            return;
        }

        if (!future.isDone()) {
            future.complete(commitIndex, result);
        }
    }

    /**
     * Attaches the given Future object to the current leadership transfer
     * process, if it aims the same target endpoint. Otherwise, the given
     * future object is notified with {@link IllegalStateException}.
     */
    void andThen(RaftEndpoint targetEndpoint, OrderedFuture otherFuture) {
        if (this.endpoint.equals(targetEndpoint)) {
            future.thenApply(ordered -> {
                if (!otherFuture.isDone()) {
                    otherFuture.complete(ordered.getCommitIndex(), ordered.getResult());
                }
                return null;
            }).exceptionally(throwable -> {
                otherFuture.fail(throwable);
                return null;
            });
        } else {
            otherFuture.fail(new IllegalStateException("There is an ongoing leadership transfer process to " + endpoint));
        }
    }

}
