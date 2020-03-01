package com.hazelcast.raft.impl.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.CompletableFuture;

/**
 * A future which prevents completion by outside caller
 */
public class InternallyCompletableFuture<T>
        extends CompletableFuture<T> {

    @Override
    public boolean completeExceptionally(Throwable ex) {
        throw new UnsupportedOperationException("This future can't be completed from outside");
    }

    @Override
    public boolean complete(T value) {
        throw new UnsupportedOperationException("This future can't be completed from outside");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("This future can't be cancelled from outside");
    }

    @Override
    public void obtrudeException(Throwable ex) {
        throw new UnsupportedOperationException("This future can't be completed from outside");
    }

    @Override
    public void obtrudeValue(T value) {
        throw new UnsupportedOperationException("This future can't be completed from outside");
    }

    public boolean internalComplete(T value) {
        return super.complete(value);
    }

    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    public boolean internalCompleteNull() {
        return super.complete(null);
    }

    public boolean internalCompleteExceptionally(Throwable ex) {
        return super.completeExceptionally(ex);
    }

}
