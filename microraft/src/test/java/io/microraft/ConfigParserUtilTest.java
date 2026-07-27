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

package io.microraft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.microraft.test.util.BaseTest;

public class ConfigParserUtilTest extends BaseTest {

    @Test
    public void getLong_whenLongValueProvided_returnsLong() {
        assertThat(ConfigParserUtil.getLong(5_000_000_000L, "test-field")).isEqualTo(5_000_000_000L);
    }

    @Test
    public void getLong_whenFractionalValueProvided_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> ConfigParserUtil.getLong(1.5d, "test-field"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Config field 'test-field' must be an integer in the long range: 1.5");
    }

    @Test
    public void getLong_whenNonNumericValueProvided_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> ConfigParserUtil.getLong("value", "test-field"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Config field 'test-field' must be a number: value");
    }

    @Test
    public void getInt_whenMaxIntValueProvided_returnsInt() {
        assertThat(ConfigParserUtil.getInt(2_147_483_647L, "test-field")).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    public void getInt_whenFractionalValueProvided_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> ConfigParserUtil.getInt(1.5d, "test-field"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Config field 'test-field' must be an integer in the int range: 1.5");
    }

    @Test
    public void getInt_whenOutOfRangeValueProvided_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> ConfigParserUtil.getInt(2_147_483_648L, "test-field"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Config field 'test-field' must be an integer in the int range: 2147483648");
    }

}

