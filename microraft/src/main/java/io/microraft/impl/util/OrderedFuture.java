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

package io.microraft.impl.util;

import io.microraft.Ordered;
import io.microraft.RaftNode;

import java.util.concurrent.CompletableFuture;

/**
 * A utility class to implement {@link CompletableFuture} and {@link Ordered}
 * interfaces together. By this way, the return value and result of a
 * {@link RaftNode} API call is realized with a single object.
 * <p>
 * Please note that completion methods here can be called from a single thread.
 */
public class OrderedFuture<T> extends CompletableFuture<Ordered<T>> implements Ordered<T> {

    private long commitIndex;
    private T result;

    public final void completeNull(long commitIndex) {
        complete(commitIndex, null);
    }

    public final void complete(long commitIndex, T result) {
        assert commitIndex >= 0;

        if (isDone()) {
            throw new IllegalStateException("Cannot complete already completed future! new commit index: " + commitIndex
                    + " result: " + result);
        }

        this.commitIndex = commitIndex;
        this.result = result;

        boolean completed = super.complete(this);
        assert completed;
    }

    public final void fail(Throwable throwable) {
        super.completeExceptionally(throwable);
    }

    @Override
    public long getCommitIndex() {
        return commitIndex;
    }

    @Override
    public T getResult() {
        return result;
    }

    @Override
    public boolean complete(Ordered<T> result) {
        throw new UnsupportedOperationException("This future cannot be completed from outside of RaftNode");
    }

    @Override
    public boolean completeExceptionally(Throwable throwable) {
        throw new UnsupportedOperationException("This future cannot be completed from outside of RaftNode");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("This future cannot be completed from outside of RaftNode");
    }

    @Override
    public void obtrudeValue(Ordered<T> result) {
        throw new UnsupportedOperationException("This future cannot be completed from outside of RaftNode");
    }

    @Override
    public void obtrudeException(Throwable throwable) {
        throw new UnsupportedOperationException("This future cannot be completed from outside of RaftNode");
    }

}
