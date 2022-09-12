package io.afloatdb.client.internal.channel.impl;

import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.internal.channel.ChannelManager;
import io.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.afloatdb.client.internal.di.AfloatDBClientModule.CONFIG_KEY;
import static java.util.Objects.requireNonNull;

@Singleton
public class ChannelManagerImpl implements ChannelManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelManager.class);

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final AfloatDBClientConfig config;
    private final ProcessTerminationLogger processTerminationLogger;

    @Inject
    public ChannelManagerImpl(@Named(CONFIG_KEY) AfloatDBClientConfig config,
            ProcessTerminationLogger processTerminationLogger) {
        this.config = config;
        this.processTerminationLogger = processTerminationLogger;
    }

    @Nonnull
    @Override
    public ManagedChannel getOrCreateChannel(@Nonnull String address) {
        ManagedChannel channel = channels.get(requireNonNull(address));
        if (channel != null) {
            if (channel.getState(true) != ConnectivityState.SHUTDOWN) {
                return channel;
            }

            silentlyShutdownNow(channel);
            channels.remove(address, channel);
        }

        return channels.computeIfAbsent(address, s -> {
            LOGGER.info("{} created channel to {}.", config.getClientId(), address);
            return ManagedChannelBuilder.forTarget(address).disableRetry().directExecutor().usePlaintext().build();
        });
    }

    @Override
    public void checkChannel(@Nonnull String address, @Nonnull ManagedChannel channel) {
        if (channel.getState(true) == ConnectivityState.SHUTDOWN && channels.remove(address, channel)) {
            silentlyShutdownNow(channel);
            LOGGER.warn("{} removed the shutdown-channel to: {}.", config.getClientId(), address);
        }
    }

    @Override
    public void retainChannels(@Nonnull Collection<String> addresses) {
        if (channels.keySet().retainAll(addresses)) {
            LOGGER.info("{} retained channels to addresses: {}.", config.getClientId(), addresses);
        }

        Iterator<Map.Entry<String, ManagedChannel>> it = channels.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ManagedChannel> e = it.next();
            if (addresses.contains(e.getKey())) {
                continue;
            }

            LOGGER.warn("{} shutting down channel to: {}", config.getClientId(), e.getKey());

            silentlyShutdownNow(e.getValue());
            it.remove();
        }
    }

    @PreDestroy
    public void shutdown() {
        channels.values().forEach(this::silentlyShutdownNow);
        channels.clear();
        processTerminationLogger.logInfo(LOGGER, config.getClientId() + " connection manager is shut down.");
    }

    private void silentlyShutdownNow(ManagedChannel channel) {
        try {
            channel.shutdownNow();
        } catch (Throwable ignored) {
        }
    }

}
