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

package io.afloatdb;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

public class AfloatDBBenchmarkRunner {

    @Option(name = "--threads")
    private int threads;

    @Option(name = "--warmup")
    private int warmupIterations;

    @Option(name = "--iterations")
    private int measurementIterations;

    @Option(name = "--mode")
    private Mode mode;

    @Option(name = "--timeUnit")
    private TimeUnit timeUnit;

    public void runBenchmark(String[] args) throws RunnerException, CmdLineException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.getProperties().withUsageWidth(80);
        parser.parseArgument(args);

        ChainedOptionsBuilder optionsBuilder = new OptionsBuilder().include(AfloatDBBenchmark.class.getSimpleName());

        if (threads > 0) {
            optionsBuilder.threads(threads);
        }

        if (warmupIterations > 0) {
            optionsBuilder.warmupIterations(warmupIterations);
        }

        if (measurementIterations > 0) {
            optionsBuilder.measurementIterations(measurementIterations);
        }

        if (mode != null) {
            optionsBuilder.mode(mode);
        }

        if (timeUnit != null) {
            optionsBuilder.timeUnit(timeUnit);
        }

        new Runner(optionsBuilder.build()).run();
    }

    public static void main(String[] args) throws RunnerException, CmdLineException {
        new AfloatDBBenchmarkRunner().runBenchmark(args);
    }

}
