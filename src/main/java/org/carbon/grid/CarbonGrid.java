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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;

public final class CarbonGrid implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(CarbonGrid.class);
    private final ConfigurationProvider configProvider;
    private Injector injector;
    private Cache cache;
    private Cluster cluster;

    CarbonGrid(ConfigurationProvider configProvider) {
        this.configProvider = configProvider;
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

    void start() {
        createInjector(configProvider);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down carbon grid node {} ...", cluster == null ? "" : cluster.myNodeId());
            shutdownGracefully();
        }));
        printBanner();
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

    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException xcp) {
                logger.error("", xcp);
            }
        }
    }

    public void shutdownGracefully() {
        injector = null;
        closeQuietly(cluster);
        closeQuietly(cache);
    }

    public Cache getCache() {
        return cache;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public boolean isUp() {
        return cluster != null && cluster.isUp();
    }

    @Override
    public void close() throws IOException {
        shutdownGracefully();
    }

    /**
     * This module publishes all config objects to guice consumers.
     */
    private static class ConfigModule extends AbstractModule {
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
