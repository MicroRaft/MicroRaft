package io.afloatdb.client;

import static java.nio.file.Files.readAllLines;
import static java.util.Objects.requireNonNull;

import com.typesafe.config.ConfigFactory;
import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.kv.KV;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.function.Function;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public class AfloatDBClientCliRunner implements Runnable {

    @Option(names = { "-s", "--server" }, required = true)
    public String serverAddress;

    @Option(names = { "-cfg", "--config-file-path" }, required = false)
    public String configFilePath;

    @Option(names = { "-id", "--client-id" }, required = false)
    public String clientId;

    @Option(names = { "-sc", "--single-connection" }, required = false)
    public boolean singleConnection;

    @Option(names = { "-tsec", "--rpc-timeout-secs" }, required = false)
    public Integer rpcTimeoutSecs;

    @Option(names = { "-prcfg", "--print-config" }, required = false)
    public boolean printConfig;

    @Option(names = { "-k", "--key" }, required = false)
    public String key;

    @Option(names = { "-v", "--value" }, required = false)
    public String value;

    @Option(names = { "-nv", "--new-value" }, required = false)
    public String newValue;

    @Option(names = { "-cix", "--min-commit-index" }, required = false)
    public long minCommitIndex = -1;

    private AfloatDBClientConfig initConfig() {
        AfloatDBClientConfig.AfloatDBClientConfigBuilder configBuilder = new AfloatDBClientConfig.AfloatDBClientConfigBuilder();
        if (configFilePath != null) {
            try {
                configBuilder.setConfig(ConfigFactory.parseString(
                        String.join("\n", readAllLines(Paths.get(configFilePath), StandardCharsets.UTF_8))));
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Cannot read config path: " + configFilePath);
                System.exit(-1);
            }
        }
        configBuilder.setServerAddress(serverAddress);
        configBuilder.setSingleConnection(singleConnection);
        if (clientId != null) {
            configBuilder.setClientId(clientId);
        }
        if (rpcTimeoutSecs != null) {
            configBuilder.setRpcTimeoutSecs(rpcTimeoutSecs);
        }

        AfloatDBClientConfig config = configBuilder.build();
        if (printConfig) {
            System.out.println(config);
        }

        org.apache.logging.log4j.core.config.Configurator.setRootLevel(org.apache.logging.log4j.Level.OFF);

        return config;
    }

    private AfloatDBClient initClient() {
        AfloatDBClientConfig config = initConfig();
        return AfloatDBClient.newInstance(config);
    }

    private Long parseValueLong() {
        try {
            return value != null ? Long.parseLong(value) : null;
        } catch (Throwable ignored) {
            return null;
        }
    }

    private Long parseNewValueLong() {
        try {
            return value != null ? Long.parseLong(newValue) : null;
        } catch (Throwable ignored) {
            return null;
        }
    }

    private void failIfKeyMissing() {
        requireNonNull(key, "key is missing");
    }

    private void failIfValueMissing() {
        requireNonNull(value, "value is missing");
    }

    private void failIfNewValueMissing() {
        requireNonNull(newValue, "new value is missing");
    }

    private <T> T call(Function<KV, T> func) {
        try (AfloatDBClient client = initClient()) {
            return func.apply(client.getKV());
        }
    }

    @Command(name = "put")
    public void put() {
        failIfKeyMissing();
        failIfValueMissing();

        Object result = call(kv -> {
            Long longVal = parseValueLong();
            return longVal != null ? kv.put(key, longVal) : kv.put(key, value);
        });

        System.out.println(result);
    }

    @Command(name = "put-if-absent")
    public void putIfAbsent() {
        failIfKeyMissing();
        failIfValueMissing();

        Object result = call(kv -> {
            Long longVal = parseValueLong();
            return longVal != null ? kv.putIfAbsent(key, longVal) : kv.putIfAbsent(key, value);
        });

        System.out.println(result);
    }

    @Command(name = "set")
    public void set() {
        failIfKeyMissing();
        failIfValueMissing();

        Object result = call(kv -> {
            Long longVal = parseValueLong();
            return longVal != null ? kv.set(key, longVal) : kv.set(key, value);
        });

        System.out.println(result);
    }

    @Command(name = "replace")
    public void replace() {
        failIfKeyMissing();
        failIfValueMissing();
        failIfNewValueMissing();

        Object result = call(kv -> {
            Long longVal = parseValueLong();
            Long longNewVal = parseNewValueLong();
            return (longVal != null && longNewVal != null) ? kv.replace(key, longVal, longNewVal)
                    : kv.replace(key, value, newValue);
        });

        System.out.println(result);
    }

    @Command(name = "remove")
    public void remove() {
        failIfKeyMissing();

        Object result = call(kv -> {
            if (value == null) {
                return kv.remove(key);
            }
            Long longVal = parseValueLong();
            return longVal != null ? kv.remove(key, longVal) : kv.remove(key, value);
        });

        System.out.println(result);
    }

    @Command(name = "delete")
    public void delete() {
        failIfKeyMissing();

        Object result = call(kv -> kv.delete(key));

        System.out.println(result);
    }

    @Command(name = "get")
    public void get() {
        failIfKeyMissing();

        Object result = call(kv -> minCommitIndex != -1 ? kv.get(key, minCommitIndex) : kv.get(key));

        System.out.println(result);
    }

    @Command(name = "contains")
    public void contains() {
        failIfKeyMissing();
        failIfValueMissing();

        Object result = call(kv -> {
            Long longVal = parseValueLong();
            if (longVal != null) {
                return minCommitIndex != -1 ? kv.contains(key, longVal, minCommitIndex) : kv.contains(key, longVal);
            } else {
                return minCommitIndex != -1 ? kv.contains(key, value, minCommitIndex) : kv.contains(key, value);
            }
        });

        System.out.println(result);
    }

    @Command(name = "contains-key")
    public void containsKey() {
        failIfKeyMissing();

        Object result = call(kv -> minCommitIndex != -1 ? kv.containsKey(key, minCommitIndex) : kv.containsKey(key));

        System.out.println(result);
    }

    @Command(name = "is-empty")
    public void isEmpty() {
        Object result = call(kv -> minCommitIndex != -1 ? kv.isEmpty(minCommitIndex) : kv.isEmpty());

        System.out.println(result);
    }

    @Command(name = "size")
    public void size() {
        Object result = call(kv -> minCommitIndex != -1 ? kv.size(minCommitIndex) : kv.size());

        System.out.println(result);
    }

    @Command(name = "clear")
    public void clear() {
        Object result = call(kv -> kv.clear());

        System.out.println(result);
    }

    @Override
    public void run() {
        System.out.println("Command not provided. --help for how to use.");
    }

    public static void main(String[] args) {
        CommandLine.run(new AfloatDBClientCliRunner(), args);
    }
}
