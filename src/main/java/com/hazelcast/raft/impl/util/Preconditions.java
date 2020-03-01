package com.hazelcast.raft.impl.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.String.format;

/**
 * A utility class for validating arguments and state.
 */
public final class Preconditions {

    private Preconditions() {
    }

    /**
     * Tests if a string contains text.
     *
     * @throws java.lang.IllegalArgumentException if the string is empty
     */
    public static String checkHasText(String argument, String errorMessage) {
        if (argument == null || argument.isEmpty()) {
            throw new IllegalArgumentException(errorMessage);
        }

        return argument;
    }

    /**
     * Tests if an argument is not null.
     *
     * @throws java.lang.NullPointerException if argument is null
     */
    public static <T> T checkNotNull(T argument, String errorMessage) {
        if (argument == null) {
            throw new NullPointerException(errorMessage);
        }
        return argument;
    }

    /**
     * Tests if the elements inside the argument collection are not null.
     * If collection is null or empty the test is ignored.
     *
     * @throws java.lang.NullPointerException if argument contains a null element inside
     */
    public static <T> Iterable<T> checkNoNullInside(Iterable<T> argument, String errorMessage) {
        if (argument == null) {
            return argument;
        }
        for (T element : argument) {
            checkNotNull(element, errorMessage);
        }
        return argument;
    }

    /**
     * Tests if an argument is not null.
     *
     * @throws java.lang.NullPointerException if argument is null
     */
    public static <T> T checkNotNull(T argument) {
        if (argument == null) {
            throw new NullPointerException();
        }
        return argument;
    }

    /**
     * Tests if a string is not null.
     *
     * @throws java.lang.IllegalArgumentException if the string is null.
     */
    public static <E> E isNotNull(E argument, String argName) {
        if (argument == null) {
            throw new IllegalArgumentException(format("argument '%s' can't be null", argName));
        }

        return argument;
    }

    /**
     * Tests if the {@code value} is &gt;= 0.
     *
     * @throws java.lang.IllegalArgumentException if the value is negative.
     */
    public static long checkNotNegative(long value, String errorMessage) {
        if (value < 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if the {@code value} is &gt;= 0.
     *
     * @throws java.lang.IllegalArgumentException if the value is negative.
     */
    public static int checkNotNegative(int value, String errorMessage) {
        if (value < 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if the {@code value} is &lt; 0.
     *
     * @throws java.lang.IllegalArgumentException if the value is negative.
     */
    public static long checkNegative(long value, String errorMessage) {
        if (value >= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if a {@code value} is positive, that is strictly larger than 0 (value &gt; 0).
     *
     * @throws java.lang.IllegalArgumentException if the value is not positive.
     */
    public static long checkPositive(long value, String errorMessage) {
        if (value <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if a {@code value} is positive, that is strictly larger than 0 (value &gt; 0).
     *
     * @throws java.lang.IllegalArgumentException if the value is not positive.
     */
    public static double checkPositive(double value, String errorMessage) {
        if (value <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if a {@code value} is positive, that is strictly larger than 0 (value &gt; 0).
     *
     * @throws java.lang.IllegalArgumentException if the value is not positive.
     */
    public static int checkPositive(int value, String errorMessage) {
        if (value <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests whether the supplied object is an instance of the supplied class type.
     *
     * @throws java.lang.IllegalArgumentException if the object is not an instance of the expected type.
     */
    public static <E> E checkInstanceOf(Class<E> type, Object object, String errorMessage) {
        isNotNull(type, "type");
        if (!type.isInstance(object)) {
            throw new IllegalArgumentException(errorMessage);
        }
        return (E) object;
    }

    /**
     * Tests whether the supplied object is an instance of the supplied class type.
     *
     * @throws java.lang.IllegalArgumentException if the object is not an instance of the expected type.
     */
    public static <E> E checkInstanceOf(Class<E> type, Object object) {
        isNotNull(type, "type");
        if (!type.isInstance(object)) {
            throw new IllegalArgumentException(object + " should be instanceof " + type.getName());
        }
        return (E) object;
    }

    /**
     * Tests the supplied object to see if it is not a type of the supplied class.
     *
     * @throws java.lang.IllegalArgumentException if the object is an instance of the type that is not of the expected class.
     */
    public static <E> E checkNotInstanceOf(Class type, E object, String errorMessage) {
        isNotNull(type, "type");
        if (type.isInstance(object)) {
            throw new IllegalArgumentException(errorMessage);
        }
        return object;
    }

    /**
     * Tests whether the supplied expression is {@code false}.
     *
     * @throws java.lang.IllegalArgumentException if the supplied expression is {@code true}.
     */
    public static void checkFalse(boolean expression, String errorMessage) {
        if (expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Tests whether the supplied expression is {@code true}.
     *
     * @throws java.lang.IllegalArgumentException if the supplied expression is {@code false}.
     */
    public static void checkTrue(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Check if iterator has next element. If not throw NoSuchElementException
     *
     * @return the iterator itself
     * @throws java.util.NoSuchElementException if iterator.hasNext returns false
     */
    public static <T> Iterator<T> checkHasNext(Iterator<T> iterator, String message)
            throws NoSuchElementException {
        if (!iterator.hasNext()) {
            throw new NoSuchElementException(message);
        }
        return iterator;
    }

    /**
     * Check the state of a condition
     *
     * @throws IllegalStateException if condition if false
     */
    public static void checkState(boolean condition, String message)
            throws IllegalStateException {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}

