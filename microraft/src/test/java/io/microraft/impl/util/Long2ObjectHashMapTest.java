/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2020, Hazelcast, Inc. All Rights Reserved.
 * Modified work Copyright (c) 2020, MicroRaft.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.microraft.test.util.BaseTest;

public class Long2ObjectHashMapTest extends BaseTest {

    @Test
    public void constructor_whenNegativeInitialCapacityProvided_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> new Long2ObjectHashMap<>(-1, Long2ObjectHashMap.DEFAULT_LOAD_FACTOR))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Initial capacity cannot be negative: -1");
    }

    @Test
    public void constructor_whenZeroLoadFactorProvided_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> new Long2ObjectHashMap<>(8, 0.0d)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Load factor must be > 0 and < 1: 0.0");
    }

    @Test
    public void constructor_whenOneLoadFactorProvided_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> new Long2ObjectHashMap<>(8, 1.0d)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Load factor must be > 0 and < 1: 1.0");
    }

    @Test
    public void constructor_whenValidArgumentsProvided_createsMap() {
        Long2ObjectHashMap<String> map = new Long2ObjectHashMap<>(8, 0.6d);

        map.put(1L, "value");

        assertThat(map.get(1L)).isEqualTo("value");
        assertThat(map.size()).isEqualTo(1);
    }

}
