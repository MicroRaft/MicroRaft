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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import java.nio.file.Path;
import java.nio.file.Files;

import io.microraft.RaftConfig;

public final class ParserTestUtils {

    private ParserTestUtils() {
    }

    public static void assertSampleConfig(RaftConfig config) {
        assertThat(config.getLeaderElectionTimeoutMillis()).isEqualTo(750L);
        assertThat(config.getLeaderHeartbeatPeriodSecs()).isEqualTo(15L);
        assertThat(config.getLeaderHeartbeatTimeoutSecs()).isEqualTo(45L);
        assertThat(config.getAppendEntriesRequestBatchSize()).isEqualTo(750);
        assertThat(config.getCommitCountToTakeSnapshot()).isEqualTo(7500);
        assertThat(config.getMaxPendingLogEntryCount()).isEqualTo(1500);
        assertThat(config.isTransferSnapshotsFromFollowersEnabled()).isFalse();
        assertThat(config.getRaftNodeReportPublishPeriodSecs()).isEqualTo(20);
    }

    public static void assertLongTimeoutConfig(RaftConfig config) {
        assertThat(config.getLeaderElectionTimeoutMillis()).isEqualTo(5_000_000_000L);
        assertThat(config.getLeaderHeartbeatPeriodSecs()).isEqualTo(15L);
        assertThat(config.getLeaderHeartbeatTimeoutSecs()).isEqualTo(45L);
    }

    public static void assertMaxIntConfigValue(int value) {
        assertThat(value).isEqualTo(Integer.MAX_VALUE);
    }

    public static void assertConfigException(ThrowingCallable callable, Class<? extends Throwable> type, String message) {
        AbstractThrowableAssert<?, ? extends Throwable> assertion = assertThatThrownBy(callable).isInstanceOf(type);
        if (message != null) {
            assertion.hasMessage(message);
        }
    }

    public static File createTempFile(Path folder, String content) throws IOException {
        java.nio.file.Path filePath = Files.createTempFile(folder, "tmp", null);
        File file = filePath.toFile();
        try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
            writer.write(content);
        }

        return file;
    }

}
