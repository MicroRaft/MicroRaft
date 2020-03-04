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
public final class AssertionUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssertionUtils.class);

    private static final int EVENTUAL_ASSERTION_TIMEOUT_SECS;

    private AssertionUtils() {
    }

    public static void eventually(AssertTask task) {
        eventually(null, task, EVENTUAL_ASSERTION_TIMEOUT_SECS);
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

    public static void sleepMillis(int millis) {
        try {
            MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void eventually(AssertTask task, long timeoutSeconds) {
        eventually(null, task, timeoutSeconds);
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

    static {
        EVENTUAL_ASSERTION_TIMEOUT_SECS = getInteger("eventual.timeout.seconds", 120);
        LOGGER.debug("EVENTUAL ASSERTION TIMEOUT SECONDS = " + EVENTUAL_ASSERTION_TIMEOUT_SECS);
    }

}
