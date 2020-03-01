package com.hazelcast.raft.impl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

/**
 * @author mdogan
 * @author metanet
 */
public class AssertUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssertUtil.class);

    private static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    static {
        ASSERT_TRUE_EVENTUALLY_TIMEOUT = getInteger("hazelcast.eventually.timeout", 120);
        LOGGER.debug("ASSERT_TRUE_EVENTUALLY_TIMEOUT = " + ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void eventually(AssertTask task) {
        eventually(null, task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void eventually(AssertTask task, long timeoutSeconds) {
        eventually(null, task, timeoutSeconds);
    }

    public static void eventually(String message, AssertTask task, long timeoutSeconds) {
        AssertionError error = null;
        int sleepMillis = 200;
        long iterations = timeoutSeconds * 5;
        for (int i = 0; i < iterations; i++) {
            try {
                try {
                    task.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return;
            } catch (AssertionError e) {
                error = e;
            }

            sleepMillis(sleepMillis);
        }
        if (error != null) {
            throw error;
        }

        fail("eventually() failed without AssertionError! " + message);
    }

    public static void allTheTime(AssertTask task, long durationSeconds) {
        for (int i = 0; i < durationSeconds; i++) {
            try {
                task.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            sleepSeconds(1);
        }
    }

    public static void sleepMillis(int millis) {
        try {
            MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void sleepSeconds(int seconds) {
        try {
            SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    public interface AssertTask {
        void run()
                throws Exception;
    }
}
