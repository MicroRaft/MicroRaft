package io.afloatdb.client;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

public class AfloatDBClientBenchmarkRunner {

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

        ChainedOptionsBuilder optionsBuilder = new OptionsBuilder()
                .include(AfloatDBClientBenchmark.class.getSimpleName());

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
        new AfloatDBClientBenchmarkRunner().runBenchmark(args);
    }

}
