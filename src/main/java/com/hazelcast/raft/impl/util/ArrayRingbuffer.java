package com.hazelcast.raft.impl.util;

import java.util.Arrays;

/**
 * The ArrayRingbuffer is responsible for storing the actual contents of a
 * ringbuffer.
 * <p>
 * Currently the Ringbuffer is not a partitioned data-structure. So all
 * data of a ringbuffer is stored in a single partition and replicated to
 * the replicas. No thread safety is needed since a partition can only be
 * accessed by a single thread at any given moment.
 *
 * @param <E> the type of the data stored in the ringbuffer
 * @author mdogan
 * @author metanet
 */
public class ArrayRingbuffer<E> {

    private E[] ringItems;
    private long tailSequence = -1;
    private long headSequence = tailSequence + 1;
    private int capacity;

    @SuppressWarnings("unchecked")
    public ArrayRingbuffer(int capacity) {
        this.capacity = capacity;
        this.ringItems = (E[]) new Object[capacity];
    }

    public long tailSequence() {
        return tailSequence;
    }

    public void setTailSequence(long sequence) {
        this.tailSequence = sequence;
    }

    public long headSequence() {
        return headSequence;
    }

    public void setHeadSequence(long sequence) {
        this.headSequence = sequence;
    }

    public long getCapacity() {
        return capacity;
    }

    public long size() {
        return tailSequence - headSequence + 1;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public long add(E item) {
        tailSequence++;

        if (tailSequence - capacity == headSequence) {
            headSequence++;
        }

        int index = toIndex(tailSequence);

        ringItems[index] = item;

        return tailSequence;
    }

    public E read(long sequence) {
        checkReadSequence(sequence);
        return ringItems[toIndex(sequence)];
    }

    // the sequence is usually in the right range since the RingbufferContainer checks it too
    private void checkReadSequence(long sequence) {
        if (sequence > tailSequence) {
            throw new IllegalArgumentException(
                    "sequence:" + sequence + " is too large. The current tailSequence is:" + tailSequence);
        }

        if (sequence < headSequence) {
            throw new StaleSequenceException(
                    "sequence:" + sequence + " is too small. The current headSequence is:" + headSequence + " tailSequence is:"
                            + tailSequence, headSequence);
        }
    }

    private int toIndex(long sequence) {
        return (int) (sequence % ringItems.length);
    }

    public void set(long seq, E data) {
        ringItems[toIndex(seq)] = data;
    }

    public void clear() {
        Arrays.fill(ringItems, null);
        tailSequence = -1;
        headSequence = tailSequence + 1;
    }
}
