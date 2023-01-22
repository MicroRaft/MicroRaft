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

package io.microraft.afloatdb.client;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.microraft.afloatdb.AfloatDB;
import io.microraft.afloatdb.client.config.AfloatDBClientConfig;
import io.microraft.afloatdb.client.kv.KV;
import io.microraft.afloatdb.client.kv.Ordered;
import io.microraft.afloatdb.internal.utils.Exceptions;
import io.grpc.Status;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.typesafe.config.ConfigFactory.load;
import static io.microraft.afloatdb.utils.AfloatDBTestUtils.CONFIG_1;
import static io.microraft.afloatdb.utils.AfloatDBTestUtils.CONFIG_2;
import static io.microraft.afloatdb.utils.AfloatDBTestUtils.CONFIG_3;
import static io.microraft.afloatdb.utils.AfloatDBTestUtils.waitUntilLeaderElected;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class KVTest extends BaseTest {

    private static final byte[] BYTES_1 = new byte[]{1, 2, 3, 4};
    private static final byte[] BYTES_2 = new byte[]{4, 3, 2, 1};
    private static final long LONG_1 = 19238;
    private static final long LONG_2 = 4693;
    private static final String STRING_1 = "str1";
    private static final String STRING_2 = "str2";
    private static final String KEY = "key";
    private static final List<AfloatDB> servers = new ArrayList<>();

    private final boolean singleConnection;
    private AfloatDBClient client;
    private KV kv;

    public KVTest(boolean singleConnection) {
        this.singleConnection = singleConnection;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @BeforeClass
    public static void initCluster() {
        servers.add(AfloatDB.bootstrap(CONFIG_1));
        servers.add(AfloatDB.bootstrap(CONFIG_2));
        servers.add(AfloatDB.bootstrap(CONFIG_3));

    }

    @AfterClass
    public static void shutDownCluster() {
        servers.forEach(AfloatDB::shutdown);
    }

    @Before
    public void initClient() {
        String serverAddress = waitUntilLeaderElected(servers).getConfig().getLocalEndpointConfig().getAddress();
        Config config = ConfigFactory.parseString("afloatdb.client.server-address: \"" + serverAddress + "\"")
                .withFallback(load("client.conf"));
        AfloatDBClientConfig clientConfig = AfloatDBClientConfig.newBuilder().setConfig(config)
                .setSingleConnection(singleConnection).build();
        client = AfloatDBClient.newInstance(clientConfig);
        kv = client.getKV();
        kv.clear();
    }

    @After
    public void shutDownClient() {
        if (client != null) {
            client.shutdown();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testPutNullKey() {
        kv.put(null, "val1");
    }

    @Test
    public void testPutByteArray() {
        Ordered<byte[]> result1 = kv.put(KEY, BYTES_1);
        Ordered<byte[]> result2 = kv.put(KEY, BYTES_2);

        assertThat(result1.get()).isNull();
        assertThat(result2.get()).isEqualTo(BYTES_1);
        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test
    public void testPutLong() {
        Ordered<Long> result1 = kv.put(KEY, LONG_1);
        Ordered<Long> result2 = kv.put(KEY, LONG_2);

        assertThat(result1.get()).isNull();
        assertThat(result2.get()).isEqualTo(LONG_1);
        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test
    public void testPutString() {
        Ordered<String> result1 = kv.put(KEY, STRING_1);
        Ordered<String> result2 = kv.put(KEY, STRING_2);

        assertThat(result1.get()).isNull();
        assertThat(result2.get()).isEqualTo(STRING_1);
        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentNullKey() {
        kv.putIfAbsent(null, "val1");
    }

    @Test
    public void testPutByteArrayIfAbsent() {
        Ordered<byte[]> result1 = kv.putIfAbsent(KEY, BYTES_1);
        Ordered<byte[]> result2 = kv.putIfAbsent(KEY, BYTES_2);

        assertThat(result1.get()).isNull();
        assertThat(result2.get()).isEqualTo(BYTES_1);
        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test
    public void testPutLongIfAbsent() {
        Ordered<Long> result1 = kv.putIfAbsent(KEY, LONG_1);
        Ordered<Long> result2 = kv.putIfAbsent(KEY, LONG_2);

        assertThat(result1.get()).isNull();
        assertThat(result2.get()).isEqualTo(LONG_1);
        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test
    public void testPutStringIfAbsent() {
        Ordered<String> result1 = kv.putIfAbsent(KEY, STRING_1);
        Ordered<String> result2 = kv.putIfAbsent(KEY, STRING_2);

        assertThat(result1.get()).isNull();
        assertThat(result2.get()).isEqualTo(STRING_1);
        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test(expected = NullPointerException.class)
    public void testSetNullKey() {
        kv.set(null, "val1");
    }

    @Test
    public void testSetByteArray() {
        Ordered<Void> result1 = kv.set(KEY, BYTES_1);
        assertThat(kv.<byte[]>get(KEY).get()).isEqualTo(BYTES_1);

        Ordered<Void> result2 = kv.set(KEY, BYTES_2);
        assertThat(kv.<byte[]>get(KEY).get()).isEqualTo(BYTES_2);

        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test
    public void testSetLong() {
        Ordered<Void> result1 = kv.set(KEY, LONG_1);
        assertThat(kv.<Long>get(KEY).get()).isEqualTo(LONG_1);

        Ordered<Void> result2 = kv.set(KEY, LONG_2);
        assertThat(kv.<Long>get(KEY).get()).isEqualTo(LONG_2);

        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test
    public void testSetString() {
        Ordered<Void> result1 = kv.set(KEY, STRING_1);
        assertThat(kv.<String>get(KEY).get()).isEqualTo(STRING_1);

        Ordered<Void> result2 = kv.set(KEY, STRING_2);
        assertThat(kv.<String>get(KEY).get()).isEqualTo(STRING_2);

        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test
    public void testSet() {
        String key = "key1";
        String val1 = "val1";
        String val2 = "val2";

        String val = kv.<String>get(key).get();
        assertThat(val).isNull();

        boolean contains = kv.containsKey(key).get();
        assertFalse(contains);

        kv.set(key, val1);

        val = kv.<String>get(key).get();
        assertThat(val).isEqualTo(val1);

        kv.set(key, val2);

        val = kv.<String>get(key).get();
        assertThat(val).isEqualTo(val2);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteNullKey() {
        kv.delete(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsNullKey() {
        kv.containsKey(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsNullKeyForKV() {
        kv.contains(null, STRING_1);
    }

    @Test
    public void testContainsByteArray() {
        long commitIndex1 = kv.set(KEY, BYTES_1).getCommitIndex();

        assertThat(kv.containsKey(KEY, commitIndex1).get()).isTrue();
        assertThat(kv.contains(KEY, BYTES_1, commitIndex1).get()).isTrue();
        assertThat(kv.contains(KEY, BYTES_2, commitIndex1).get()).isFalse();

        long commitIndex2 = kv.delete(KEY).getCommitIndex();

        assertThat(kv.containsKey(KEY, commitIndex2).get()).isFalse();
    }

    @Test
    public void testContainsLong() {
        long commitIndex1 = kv.set(KEY, LONG_1).getCommitIndex();

        assertThat(kv.containsKey(KEY, commitIndex1).get()).isTrue();
        assertThat(kv.contains(KEY, LONG_1, commitIndex1).get()).isTrue();
        assertThat(kv.contains(KEY, LONG_2, commitIndex1).get()).isFalse();

        long commitIndex2 = kv.delete(KEY).getCommitIndex();

        assertThat(kv.containsKey(KEY, commitIndex2).get()).isFalse();
    }

    @Test
    public void testContainsString() {
        long commitIndex1 = kv.set(KEY, STRING_1).getCommitIndex();

        assertThat(kv.containsKey(KEY, commitIndex1).get()).isTrue();
        assertThat(kv.contains(KEY, STRING_1, commitIndex1).get()).isTrue();
        assertThat(kv.contains(KEY, STRING_2, commitIndex1).get()).isFalse();

        long commitIndex2 = kv.delete(KEY).getCommitIndex();

        assertThat(kv.containsKey(KEY, commitIndex2).get()).isFalse();
    }

    @Test
    public void testDelete() {
        String key = "key1";
        String val1 = "val1";

        String val = kv.<String>get(key).get();
        assertThat(val).isNull();

        kv.set(key, val1);

        val = kv.<String>get(key).get();
        assertThat(val).isEqualTo(val1);

        Ordered<Boolean> result1 = kv.delete(key);
        assertTrue(result1.get());

        val = kv.<String>get(key).get();
        assertThat(val).isNull();

        Ordered<Boolean> result2 = kv.delete(key);
        assertFalse(result2.get());

        val = kv.<String>get(key).get();
        assertThat(val).isNull();

        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
    }

    @Test
    public void testDeleteNonExistingKey() {
        Ordered<Boolean> result = kv.delete(KEY);
        assertThat(result.get()).isFalse();
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testDeleteByteArray() {
        kv.set(KEY, BYTES_1);

        Ordered<Boolean> result = kv.delete(KEY);
        assertThat(result.get()).isTrue();
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testDeleteLong() {
        kv.set(KEY, LONG_1);

        Ordered<Boolean> result = kv.delete(KEY);
        assertThat(result.get()).isTrue();
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testDeleteString() {
        kv.set(KEY, STRING_1);

        Ordered<Boolean> result = kv.delete(KEY);
        assertThat(result.get()).isTrue();
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveNullKey() {
        kv.remove(null);
    }

    @Test
    public void testRemoveNonExistingKey() {
        Ordered<Object> result = kv.remove(KEY);
        assertThat(result.get()).isNull();
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testRemoveByteArray() {
        kv.set(KEY, BYTES_1);

        Ordered<Boolean> result1 = kv.remove(KEY, BYTES_2);
        Ordered<Boolean> result2 = kv.remove(KEY, BYTES_1);
        assertThat(result1.get()).isFalse();
        assertThat(result2.get()).isTrue();

        kv.set(KEY, BYTES_2);

        Ordered<byte[]> result3 = kv.remove(KEY);
        assertThat(result3.get()).isEqualTo(BYTES_2);
        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
        assertThat(result3.getCommitIndex()).isGreaterThan(result2.getCommitIndex());
    }

    @Test
    public void testRemoveLong() {
        kv.set(KEY, LONG_1);

        Ordered<Boolean> result1 = kv.remove(KEY, LONG_2);
        Ordered<Boolean> result2 = kv.remove(KEY, LONG_1);
        assertThat(result1.get()).isFalse();
        assertThat(result2.get()).isTrue();

        kv.set(KEY, LONG_2);

        Ordered<Long> result3 = kv.remove(KEY);
        assertThat(result3.get()).isEqualTo(LONG_2);
        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
        assertThat(result3.getCommitIndex()).isGreaterThan(result2.getCommitIndex());
    }

    @Test
    public void testRemoveString() {
        kv.set(KEY, STRING_1);

        Ordered<Boolean> result1 = kv.remove(KEY, STRING_2);
        Ordered<Boolean> result2 = kv.remove(KEY, STRING_1);
        assertThat(result1.get()).isFalse();
        assertThat(result2.get()).isTrue();

        kv.set(KEY, STRING_2);

        Ordered<String> result3 = kv.remove(KEY);
        assertThat(result3.get()).isEqualTo(STRING_2);
        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
        assertThat(result3.getCommitIndex()).isGreaterThan(result2.getCommitIndex());
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNullKey() {
        kv.replace(null, STRING_1, STRING_2);
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNullOldValue() {
        kv.replace(KEY, null, STRING_1);
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNullNewValue() {
        kv.replace(KEY, STRING_1, null);
    }

    @Test
    public void testReplaceByteArray() {
        kv.set(KEY, BYTES_1);

        Ordered<Boolean> result1 = kv.replace(KEY, BYTES_2, BYTES_1);
        Ordered<Boolean> result2 = kv.replace(KEY, BYTES_2, STRING_1);
        Ordered<Boolean> result3 = kv.replace(KEY, BYTES_1, BYTES_2);
        Ordered<Boolean> result4 = kv.replace(KEY, BYTES_2, STRING_1);

        assertThat(result1.get()).isFalse();
        assertThat(result2.get()).isFalse();
        assertThat(result3.get()).isTrue();
        assertThat(result4.get()).isTrue();
    }

    @Test
    public void testReplaceLong() {
        kv.set(KEY, LONG_1);

        Ordered<Boolean> result1 = kv.replace(KEY, LONG_2, LONG_1);
        Ordered<Boolean> result2 = kv.replace(KEY, LONG_2, STRING_1);
        Ordered<Boolean> result3 = kv.replace(KEY, LONG_1, LONG_2);
        Ordered<Boolean> result4 = kv.replace(KEY, LONG_2, STRING_1);

        assertThat(result1.get()).isFalse();
        assertThat(result2.get()).isFalse();
        assertThat(result3.get()).isTrue();
        assertThat(result4.get()).isTrue();
    }

    @Test
    public void testReplaceString() {
        kv.set(KEY, STRING_1);

        Ordered<Boolean> result1 = kv.replace(KEY, STRING_2, STRING_1);
        Ordered<Boolean> result2 = kv.replace(KEY, STRING_2, LONG_1);
        Ordered<Boolean> result3 = kv.replace(KEY, STRING_1, STRING_2);
        Ordered<Boolean> result4 = kv.replace(KEY, STRING_2, LONG_1);

        assertThat(result1.get()).isFalse();
        assertThat(result2.get()).isFalse();
        assertThat(result3.get()).isTrue();
        assertThat(result4.get()).isTrue();
    }

    @Test
    public void testSize() {
        int keyCount = 100;
        for (int i = 0; i < keyCount; i++) {
            String key = "key" + i;
            String val = "val" + i;

            long commitIndex = kv.set(key, val).getCommitIndex();

            int expectedSize = i + 1;

            Ordered<Integer> result = kv.size();
            assertThat(result.getCommitIndex()).isGreaterThanOrEqualTo(commitIndex);
            assertThat(result.get()).isEqualTo(expectedSize);
        }

        for (int i = 0; i < keyCount; i++) {
            String key = "key" + i;

            long commitIndex = kv.delete(key).getCommitIndex();

            int expectedSize = 100 - i - 1;

            Ordered<Integer> result = kv.size();
            assertThat(result.getCommitIndex()).isGreaterThanOrEqualTo(commitIndex);
            assertThat(result.get()).isEqualTo(expectedSize);
        }
    }

    @Test
    public void testClear() {
        int keyCount = 100;
        long commitIndex = 0;
        for (int i = 0; i < keyCount; i++) {
            String key = "key" + i;
            String val = "val" + i;

            commitIndex = kv.set(key, val).getCommitIndex();
        }

        long finalCommitIndex = commitIndex;

        Ordered<Boolean> isEmptyResult1 = kv.isEmpty(finalCommitIndex);
        assertThat(isEmptyResult1.get()).isFalse();
        assertThat(isEmptyResult1.getCommitIndex()).isGreaterThanOrEqualTo(finalCommitIndex);

        Ordered<Integer> clearResult = kv.clear();
        assertThat(clearResult.get()).isEqualTo(keyCount);
        assertThat(clearResult.getCommitIndex()).isGreaterThan(commitIndex);

        Ordered<Boolean> isEmptyResult2 = kv.isEmpty(clearResult.getCommitIndex());
        assertThat(isEmptyResult2.get()).isTrue();
        assertThat(isEmptyResult2.getCommitIndex()).isGreaterThanOrEqualTo(clearResult.getCommitIndex());
    }

    @Test
    public void testGetOrderedWithCommitIndex() {
        int keyCount = 100;
        for (int i = 0; i < keyCount; i++) {
            String key = "key" + i;
            String val = "val" + i;

            long commitIndex = kv.set(key, val).getCommitIndex();
            while (true) {
                try {
                    Ordered<String> result = kv.get(key, commitIndex);
                    assertThat(result.getCommitIndex()).isEqualTo(commitIndex);
                    assertThat(result.get()).isEqualTo(val);
                    break;
                } catch (io.grpc.StatusRuntimeException e) {
                    assertThat(e.getStatus()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertTrue(Exceptions.isRaftException(e.getMessage(), LaggingCommitIndexException.class));
                }
            }
        }
    }

    @Test
    public void testGet() {
        int keyCount = 100;
        for (int i = 0; i < keyCount; i++) {
            String key = "key" + i;
            String val = "val" + i;

            long commitIndex = kv.set(key, val).getCommitIndex();

            Ordered<String> result = kv.get(key);
            assertThat(result.getCommitIndex()).isEqualTo(commitIndex);
            assertThat(result.get()).isEqualTo(val);
        }
    }

}
