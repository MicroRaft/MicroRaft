package com.hazelcast.raft.impl.util;

// TODO javadoc

/**
 * An {@link RuntimeException} that is thrown when accessing an item in the {@link ArrayRingbuffer} using a sequence that is
 * smaller than the current head sequence and that the ringbuffer store is disabled. This means that the item isn't available
 * in the ringbuffer and it cannot be loaded from the store either, thus being completely unavailable.
 */
public class StaleSequenceException
        extends RuntimeException {

    private final long headSeq;

    /**
     * Creates a StaleSequenceException with the given message.
     *
     * @param message the message
     * @param headSeq the last known head sequence.
     */
    public StaleSequenceException(String message, long headSeq) {
        super(message);
        this.headSeq = headSeq;
    }

    /**
     * Returns the last known head sequence. Beware that this sequence could already be stale again if you want to use it
     * to do a {@link ArrayRingbuffer#read(long)}.
     *
     * @return last known head sequence.
     */
    public long getHeadSeq() {
        return headSeq;
    }
}
