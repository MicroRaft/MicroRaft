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

package io.microraft.afloatdb;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.microraft.afloatdb.client.config.AfloatDBClientConfig;
import io.microraft.afloatdb.client.kv.KV;
import io.microraft.afloatdb.client.AfloatDBClient;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Fork(1)
public class AfloatDBBenchmark {

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(1)
    @Measurement(iterations = 50)
    @Warmup(iterations = 20)
    public void setTesting(BenchmarkState state) {
        int i = state.random(state.keyCount);
        String keyStr = "key" + i;
        String valueStr = "value" + i;
        state.kv.set(keyStr, valueStr);
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        AfloatDBClient client;
        KV kv;
        int keyCount;

        @Setup(Level.Trial)
        public void setup() {
            File configFile = new File("benchmark-client.conf");
            Config config = ConfigFactory.parseFile(configFile);
            AfloatDBClientConfig clientConfig = AfloatDBClientConfig.newBuilder().setConfig(config).build();
            client = AfloatDBClient.newInstance(clientConfig);
            kv = client.getKV();
            kv.clear();
            keyCount = config.getInt("key-count");
            System.out.println("# Key count: " + keyCount);
        }

        @TearDown(Level.Trial)
        public void tearDown(Blackhole hole) {
            client.shutdown();
        }

        public int random(int limit) {
            return ThreadLocalRandom.current().nextInt(limit);
        }

    }

}
