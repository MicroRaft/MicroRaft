/*
 * Copyright (c) 2020, MicroRaft.
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

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTest {

    static final Logger LOGGER = LoggerFactory.getLogger("Test");

    @RegisterExtension
    @SuppressWarnings("checkstyle:anoninnerlength")
    public final TestWatcher testWatcher = new TestWatcher() {
        @Override
        public void testSuccessful(ExtensionContext context) {
            LOGGER.info("+ SUCCEEDED: " + context.getDisplayName());
        }
        @Override
        public void testFailed(ExtensionContext context, Throwable cause) {
            LOGGER.info("- FAILED: " + context.getDisplayName());
        }
    };

}
