/*
 * Copyright 2017 Marco Helmich
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

package org.carbon.grid;

import org.cfg4j.provider.ConfigurationProvider;
import org.cfg4j.provider.ConfigurationProviderBuilder;
import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.context.environment.Environment;
import org.cfg4j.source.context.environment.ImmutableEnvironment;
import org.cfg4j.source.files.FilesConfigurationSource;
import org.cfg4j.source.reload.ReloadStrategy;
import org.cfg4j.source.reload.strategy.PeriodicalReloadStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Stand-alone data node entry point.
     * You have to start CarbonGrid by providing a command line like this:
     * java -jar carbon-grid.jar server <path-to-config-file>
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            printUsage();
            return;
        }

        if (!"server".equals(args[0])) {
            printUsage();
            return;
        }

        logger.info("Starting as standalone data node with config file at {}", args[1]);
        new Main().start(args[1]);
    }

    private static void printUsage() {
        logger.error("Didn't provide a valid command line!");
        logger.error("java -jar carbon-grid.jar server <path-to-config-file>");
    }

    private ConfigurationProvider buildConfigProvider(Path configFile) {
        Path absPath = configFile.toAbsolutePath();
        logger.info("Searching for config file here: {}", absPath);
        if (!Files.exists(configFile)) {
            throw new CarbonGridException("Config file " + absPath + " doesn't exist!!");
        }

        ConfigurationSource cs = new FilesConfigurationSource(absPath::getFileName);
        Environment ce = new ImmutableEnvironment(absPath.getParent().toString());
        ReloadStrategy reloadStrategy = new PeriodicalReloadStrategy(1, TimeUnit.MINUTES);
        return new ConfigurationProviderBuilder()
                .withConfigurationSource(cs)
                .withEnvironment(ce)
                .withReloadStrategy(reloadStrategy)
                .build();
    }

    private void start(String pathToConfigFile) {
        Path configFile = Paths.get(pathToConfigFile);
        ConfigurationProvider configProvider = buildConfigProvider(configFile);
        new CarbonGrid(configProvider);
    }
}
