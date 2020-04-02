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

package io.microraft.impl.util;

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
 * @param <E>
 *         the type of the data stored in the ringbuffer
 *
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

    public boolean isEmpty() {
        return size() == 0;
    }

    public long size() {
        return tailSequence - headSequence + 1;
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

    private int toIndex(long sequence) {
        return (int) (sequence % ringItems.length);
    }

    public E read(long sequence) {
        checkReadSequence(sequence);
        return ringItems[toIndex(sequence)];
    }

    // the sequence is usually in the right range since the RingbufferContainer checks it too
    private void checkReadSequence(long sequence) {
        if (sequence > tailSequence) {
            throw new IllegalArgumentException(
                    "sequence:" + sequence + " is too large. The current tailSequence: " + tailSequence);
        } else if (sequence < headSequence) {
            throw new IllegalArgumentException(
                    "sequence:" + sequence + " is too small. The current headSequence: " + headSequence + " " + "tailSequence: "
                            + tailSequence);
        }
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
