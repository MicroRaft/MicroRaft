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

package io.microraft.test.util;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AssertionUtils {

    public static final int EVENTUAL_ASSERTION_TIMEOUT_SECS;

    private static final Logger LOGGER = LoggerFactory.getLogger(AssertionUtils.class);

    private AssertionUtils() {
    }

    public static void eventually(AssertTask task) {
        eventually(task, EVENTUAL_ASSERTION_TIMEOUT_SECS);
    }

    public static void eventually(AssertTask task, long timeoutSeconds) {
        AssertionError error = null;
        long sleepMillis = 200;
        long iterations = SECONDS.toMillis(timeoutSeconds) / sleepMillis;
        for (int i = 0; i < iterations; i++) {
            try {
                try {
                    task.run();
                    return;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } catch (AssertionError e) {
                error = e;
            }

            sleepMillis(sleepMillis);
        }

        if (error != null) {
            throw error;
        }

        fail("eventually() failed without AssertionError!");
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

    public static void sleepMillis(long millis) {
        if (millis <= 0) {
            return;
        }

        long now = System.currentTimeMillis();
        long deadline = now + millis;
        while (now < deadline) {
            try {
                MILLISECONDS.sleep(deadline - now);
                now = Math.max(now, System.currentTimeMillis());
            } catch (InterruptedException e) {
                LOGGER.info("Interrupted while sleeping {} millis.", millis);
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public static void sleepSeconds(int seconds) {
        sleepMillis(seconds * 1000);
    }

    @FunctionalInterface
    public interface AssertTask {
        void run() throws Exception;
    }

    static {
        EVENTUAL_ASSERTION_TIMEOUT_SECS = getInteger("eventual.timeout.seconds", 120);
        LOGGER.debug("EVENTUAL ASSERTION TIMEOUT SECONDS = " + EVENTUAL_ASSERTION_TIMEOUT_SECS);
    }

}
