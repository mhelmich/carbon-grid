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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.carbon.grid.cache.Cache;
import org.carbon.grid.cache.CacheModule;
import org.carbon.grid.cache.InternalCache;
import org.carbon.grid.cluster.Cluster;
import org.carbon.grid.cluster.ClusterModule;
import org.cfg4j.provider.ConfigurationProvider;
import org.cfg4j.provider.ConfigurationProviderBuilder;
import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.classpath.ClasspathConfigurationSource;
import org.cfg4j.source.context.environment.Environment;
import org.cfg4j.source.context.environment.ImmutableEnvironment;
import org.cfg4j.source.files.FilesConfigurationSource;
import org.cfg4j.source.reload.ReloadStrategy;
import org.cfg4j.source.reload.strategy.PeriodicalReloadStrategy;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public final class CarbonGrid implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(CarbonGrid.class);
    // you can have multiple grids inside the same JVM
    // the key into the map is a hash code (currently) on a config source
    private static final NonBlockingHashMap<Integer, CarbonGrid> hashToGrid = new NonBlockingHashMap<>();

    private static void printUsage() {
        logger.error("Didn't provide a valid command line!");
        logger.error("java -jar carbon-grid.jar server <path-to-config-file>");
    }

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
        Path configFile = Paths.get(args[1]);
        start(configFile);
    }

    /**
     * Start carbon grid and leave it to itself to find configurations.
     */
    public static CarbonGrid start() throws CarbonGridException {
        logger.info("Searching for carbon-grid.yaml on the class path");
        ConfigurationSource cs = new ClasspathConfigurationSource(
                () -> Paths.get("carbon-grid.yaml")
        );

        return innerStart(cs);
    }

    /**
     * Start carbon grid and point it to the config file it is supposed to load.
     */
    public static CarbonGrid start(Path configFile) throws CarbonGridException {
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
        return innerStart(configProvider);
    }

    /**
     * Start carbon grid and point it to the config file it is supposed to load.
     */
    public static CarbonGrid start(File configFile) throws CarbonGridException {
        return start(configFile.toPath());
    }

    @VisibleForTesting
    static CarbonGrid innerStart(ConfigurationSource cs) {
        ConfigurationProvider configProvider = new ConfigurationProviderBuilder()
                .withConfigurationSource(cs)
                .build();

        return innerStart(configProvider);
    }

    private static CarbonGrid innerStart(ConfigurationProvider configProvider) {
        CarbonGrid grid = hashToGrid.putIfAbsent(configProvider.hashCode(), new CarbonGrid(configProvider));
        return grid == null ? hashToGrid.get(configProvider.hashCode()) : grid;
    }

    private final ConfigurationProvider configProvider;
    private Injector injector;
    private Cache cache;
    private Cluster cluster;

    private CarbonGrid(ConfigurationProvider configProvider) {
        this.configProvider = configProvider;
        createInjector(configProvider);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdownGracefully();
            } catch (IOException xcp) {
                logger.warn("Error during shutdown", xcp);
            }
        }));
        printBanner();
    }

    private void printBanner() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(CarbonGrid.class.getResourceAsStream("/banner.txt")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Exception xcp) {
            // no op
        }
    }

    private void createInjector(ConfigurationProvider configProvider) {
        try {
            injector = Guice.createInjector(
                    new ConfigModule(configProvider),
                    new ClusterModule(),
                    new CacheModule()
            );

            cluster = injector.getInstance(Cluster.class);
            cache = injector.getInstance(InternalCache.class);
        } catch (Exception xcp) {
            throw new CarbonGridException(xcp);
        }
    }

    public void shutdownGracefully() throws IOException {
        getCluster().close();
        getCache().close();
    }

    public Cache getCache() {
        return cache;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public boolean isUp() {
        return cluster.isUp();
    }

    @Override
    public void close() throws IOException {
        shutdownGracefully();
    }

    /**
     * This module publishes all config objects to guice consumers.
     */
    static class ConfigModule extends AbstractModule {
        private final ConfigurationProvider configProvider;

        ConfigModule(ConfigurationProvider configProvider) {
            this.configProvider = configProvider;
        }

        @Override
        protected void configure() {
            bind(ServerConfig.class).toInstance(configProvider.bind("server", ServerConfig.class));
            bind(ConsulConfig.class).toInstance(configProvider.bind("consul", ConsulConfig.class));
            bind(CacheConfig.class).toInstance(configProvider.bind("cache", CacheConfig.class));
        }
    }

    //////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////
    /////////////////////////////////////////////
    // These interfaces are definitions of the config
    // we expect to see
    public interface ServerConfig {
        Integer port();
        Integer timeout();
    }

    public interface ConsulConfig {
        String host();
        Integer port();
        Integer timeout();
        Integer numCheckinFailuresToShutdown();
        String dataCenterName();
    }

    public interface CacheConfig {
        Long maxAvailableMemory();
        Integer maxCacheLineSize();
        Integer replicationFactor();
    }
}
