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

package io.afloatdb.client.internal.kv.impl;

import io.afloatdb.client.kv.KV;
import io.afloatdb.client.internal.rpc.InvocationService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.function.Supplier;

@Singleton
public class KVSupplier implements Supplier<KV> {

    private final KV kv;

    @Inject
    public KVSupplier(InvocationService invocationService) {
        this.kv = new KVProxy(invocationService);
    }

    @Override
    public KV get() {
        return kv;
    }

}
