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
import org.cfg4j.source.classpath.ClasspathConfigurationSource;
import org.cfg4j.source.context.environment.Environment;
import org.cfg4j.source.context.environment.ImmutableEnvironment;
import org.cfg4j.source.files.FilesConfigurationSource;
import org.cfg4j.source.reload.ReloadStrategy;
import org.cfg4j.source.reload.strategy.PeriodicalReloadStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public final class CarbonGridFactory {
    private final static Logger logger = LoggerFactory.getLogger(CarbonGridFactory.class);

    public CarbonGrid createCarbonGrid() {
        logger.info("Searching for carbon-grid.yaml on the class path");
        ConfigurationSource cs = new ClasspathConfigurationSource(
                () -> Paths.get("carbon-grid.yaml")
        );

        ConfigurationProvider configProvider = new ConfigurationProviderBuilder()
                .withConfigurationSource(cs)
                .build();

        return new CarbonGrid(configProvider);
    }

    public CarbonGrid createCarbonGrid(Path configFile) {
        Path absPath = configFile.toAbsolutePath();
        logger.info("Searching for config file here: {}", absPath);
        if (!Files.exists(configFile)) {
            throw new CarbonGridException("Config file " + absPath + " doesn't exist!!");
        }

        ConfigurationSource cs = new FilesConfigurationSource(absPath::getFileName);
        Environment ce = new ImmutableEnvironment(absPath.getParent().toString());
        ReloadStrategy reloadStrategy = new PeriodicalReloadStrategy(1, TimeUnit.MINUTES);
        ConfigurationProvider configProvider = new ConfigurationProviderBuilder()
                .withConfigurationSource(cs)
                .withEnvironment(ce)
                .withReloadStrategy(reloadStrategy)
                .build();

        return new CarbonGrid(configProvider);
    }

    public CarbonGrid createCarbonGrid(File configFile) {
        return createCarbonGrid(configFile.toPath());
    }
}
