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

package io.microraft.afloatdb.client.internal.di;

import static com.google.inject.name.Names.named;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import io.microraft.afloatdb.client.config.AfloatDBClientConfig;
import io.microraft.afloatdb.client.internal.channel.ChannelManager;
import io.microraft.afloatdb.client.internal.channel.impl.ChannelManagerImpl;
import io.microraft.afloatdb.client.internal.kv.impl.KVSupplier;
import io.microraft.afloatdb.client.internal.rpc.InvocationService;
import io.microraft.afloatdb.client.internal.rpc.impl.MultiKVServiceStubManager;
import io.microraft.afloatdb.client.internal.rpc.impl.UniKVServiceStubManager;
import io.microraft.afloatdb.client.kv.KV;
import io.microraft.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.microraft.afloatdb.internal.lifecycle.impl.ProcessTerminationLoggerImpl;
import io.microraft.afloatdb.kv.proto.KVServiceGrpc.KVServiceFutureStub;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class AfloatDBClientModule extends AbstractModule {

    public static final String CLIENT_ID_KEY = "ClientId";
    public static final String CONFIG_KEY = "Config";
    public static final String KV_STUB_KEY = "KVStub";
    public static final String KV_STORE_KEY = "KVStore";

    private final AfloatDBClientConfig config;
    private final AtomicBoolean processTerminationFlag;

    public AfloatDBClientModule(AfloatDBClientConfig config, AtomicBoolean processTerminationFlag) {
        this.config = config;
        this.processTerminationFlag = processTerminationFlag;
    }

    @Override
    protected void configure() {
        bind(String.class).annotatedWith(named(CLIENT_ID_KEY)).toInstance(config.getClientId());
        bind(AfloatDBClientConfig.class).annotatedWith(named(CONFIG_KEY)).toInstance(config);
        bind(AtomicBoolean.class).annotatedWith(named(ProcessTerminationLoggerImpl.PROCESS_TERMINATION_FLAG_KEY))
                .toInstance(processTerminationFlag);
        bind(ProcessTerminationLogger.class).to(ProcessTerminationLoggerImpl.class);
        bind(ChannelManager.class).to(ChannelManagerImpl.class);
        bind(new TypeLiteral<Supplier<KV>>() {
        }).annotatedWith(named(KV_STORE_KEY)).to(KVSupplier.class);
        Class<? extends InvocationService> kvStubSupplierClazz = config.isSingleConnection()
                ? UniKVServiceStubManager.class
                : MultiKVServiceStubManager.class;
        bind(InvocationService.class).to(kvStubSupplierClazz);
    }
}
