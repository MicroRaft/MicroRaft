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

import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.microraft.afloatdb.config.AfloatDBConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static java.nio.file.Files.readAllLines;

public class RunAfloatDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunAfloatDB.class);

    public static void main(String[] args) {
        String configFileName = getConfigFileName(args);
        LOGGER.info("Reading config from " + configFileName);

        AfloatDB server;
        AfloatDBConfig config = readConfigFile(configFileName);
        if (config.getRaftGroupConfig().getJoinTo() == null) {
            server = AfloatDB.bootstrap(config);
        } else {
            server = AfloatDB.join(config, true /* votingMember */);
        }

        server.awaitTermination();
    }

    private static String getConfigFileName(String[] args) {
        String prop = System.getProperty("afloatdb.config");
        if (args.length > 0) {
            return args[0];
        } else if (prop != null) {
            return prop;
        } else {
            LOGGER.error("Config file location must be provided either via program argument or system parameter: "
                    + "\"afloatdb.config\"! If both are present, program argument is used.");
            System.exit(-1);
            return null;
        }
    }

    private static AfloatDBConfig readConfigFile(String configFileName) {
        try {
            String config = String.join("\n", readAllLines(Paths.get(configFileName), StandardCharsets.UTF_8));
            return AfloatDBConfig.from(ConfigFactory.parseString(config));
        } catch (IOException e) {
            LOGGER.error("Cannot read config file: " + configFileName, e);
            System.exit(-1);
            return null;
        }
    }

}
