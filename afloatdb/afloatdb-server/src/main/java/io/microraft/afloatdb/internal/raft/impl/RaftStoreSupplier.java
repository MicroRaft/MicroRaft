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

package io.microraft.afloatdb.internal.raft.impl;

import java.util.function.Supplier;
import io.microraft.persistence.RaftStore;
import io.microraft.afloatdb.config.AfloatDBConfig;
import io.microraft.model.RaftModelFactory;
import io.microraft.store.sqlite.StoreModelSerializer;
import io.microraft.store.sqlite.RaftSqliteStore;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;

import static io.microraft.afloatdb.internal.di.AfloatDBModule.CONFIG_KEY;

@Singleton
public class RaftStoreSupplier implements Supplier<RaftStore> {

    private RaftSqliteStore sqliteStore;

    @Inject
    public RaftStoreSupplier(@Named(CONFIG_KEY) AfloatDBConfig config, RaftModelFactory modelFactory,
            StoreModelSerializer storeModelSerializer) {
        File sqliteFile = new File(config.getPersistenceConfig().getSqliteFilePath());
        this.sqliteStore = RaftSqliteStore.create(sqliteFile, modelFactory, storeModelSerializer);
    }

    @Override
    public RaftSqliteStore get() {
        return sqliteStore;
    }

}
