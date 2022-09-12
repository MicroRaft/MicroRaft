package io.afloatdb.client.internal.channel;

import io.grpc.ManagedChannel;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface ChannelManager {

    @Nonnull
    ManagedChannel getOrCreateChannel(@Nonnull String address);

    void checkChannel(@Nonnull String address, @Nonnull ManagedChannel channel);

    void retainChannels(@Nonnull Collection<String> addresses);

}
