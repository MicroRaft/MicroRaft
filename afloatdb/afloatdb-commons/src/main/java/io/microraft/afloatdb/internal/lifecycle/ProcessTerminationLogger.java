/*
 * Copyright (c) 2020, AfloatDB.
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

package io.microraft.afloatdb.internal.lifecycle;

import org.slf4j.Logger;

public interface ProcessTerminationLogger {

    boolean isCurrentProcessTerminating();

    default void logInfo(Logger logger, String message) {
        if (isCurrentProcessTerminating()) {
            System.err.println(message);
        } else {
            logger.info(message);
        }
    }

    default void logWarn(Logger logger, String message) {
        if (isCurrentProcessTerminating()) {
            System.err.println(message);
        } else {
            logger.warn(message);
        }
    }

    default void logError(Logger logger, String message, Throwable t) {
        if (isCurrentProcessTerminating()) {
            System.err.println(message);
            t.printStackTrace(System.err);
        } else {
            logger.error(message, t);
        }
    }

}
