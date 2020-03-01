package com.hazelcast.raft.impl.util;

import java.util.Random;

// TODO javadoc

/**
 *
 */
public final class RandomPicker {

    private static Random randomNumberGenerator;

    private RandomPicker() {
    }

    /**
     * Returns a pseudorandom, uniformly distributed int value between 0 (inclusive)
     * and the specified value (exclusive), drawn from this random number generator's sequence.
     * Starts the random number generator sequence if it has not been initialized.
     *
     * @param n the specified value
     * @return a value between 0 (inclusive) and the specified value (exclusive).
     */
    public static int getRandomInt(int n) {
        if (randomNumberGenerator == null) {
            randomNumberGenerator = new Random();
        }
        return randomNumberGenerator.nextInt(n);
    }

    /**
     * Return a pseudorandom, uniformly distributed in value between the low value (inclusive) and
     * the high value (exclusive), drawn from this random number generator's sequence.
     * Starts the random number generator sequence if it has not been initialized.
     *
     * @param low  lowest value of the range (inclusive)
     * @param high highest value of the range (exclusive)
     * @return a value between the specified low (inclusive) and high value (exclusive).
     */
    public static int getRandomInt(int low, int high) {
        return getRandomInt(high - low) + low;
    }

}
