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
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class CarbonGrid implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(CarbonGrid.class);
    // you can have multiple grids inside the same JVM
    // the key into the map is a hash code (currently) on a config source
    private static final NonBlockingHashMap<Integer, CarbonGrid> hashToGrid = new NonBlockingHashMap<>();

    private static void printUsage() {
        logger.error("Didn't provide a valid command line!");
        logger.error("java -jar carbon-grid.jar server <path-to-config-file>");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            printUsage();
            return;
        }

        if (!"server".equals(args[0])) {
            printUsage();
            return;
        }

        // TODO -- build stand-alone server start method
        start();
    }

    /**
     * Start carbon grid and leave to itself to find configurations.
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
        logger.info("Searching for config file here: {}", configFile);
        ConfigurationSource cs = new ClasspathConfigurationSource(() -> configFile);
        return innerStart(cs);
    }

    /**
     * Start carbon grid and point it to the config file it is supposed to load.
     */
    public static CarbonGrid start(File configFile) throws CarbonGridException {
        return start(configFile.toPath());
    }

    static CarbonGrid innerStart(ConfigurationSource cs) {
        ConfigurationProvider configProvider = new ConfigurationProviderBuilder()
                .withConfigurationSource(cs)
                .build();

        CarbonGrid grid = hashToGrid.putIfAbsent(cs.hashCode(), new CarbonGrid(configProvider));
        return grid == null ? hashToGrid.get(cs.hashCode()) : grid;
    }

    private final ConfigurationProvider configProvider;
    private Injector injector;
    private Cache cache;
    private Cluster cluster;

    private CarbonGrid(ConfigurationProvider configProvider) {
        this.configProvider = configProvider;
        createInjector(configProvider);
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
    }
}
