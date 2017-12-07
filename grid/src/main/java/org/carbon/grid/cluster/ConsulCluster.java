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

package org.carbon.grid.cluster;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.carbon.grid.CarbonGrid;
import org.carbon.grid.cache.PeerChangeConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class takes care of everything that involves global state in the cluster.
 * That includes:
 * - globally unique ids for cache lines
 * - cluster-unique node id allocation
 * - health checks
 * - service registrations
 * - node info and what it looks like
 * - changes in node upness (and the listener and its consumer)
 * - death pills
 * - leader-replica allocation (I still have to define a HA strategy)
 */
@Singleton
class ConsulCluster implements Cluster {
    private final static Logger logger = LoggerFactory.getLogger(ConsulCluster.class);
    private final static String serviceName = ConsulClient.SERVICE_NAME;
    private final static String CACHE_LINE_ID_KEY = serviceName + "/cluster/cache-lines/running-cache-line-id";
    final static String NODE_INFO_KEY_PREFIX = serviceName + "/cluster/node-info/";
    private final static int ID_CHUNK_SIZE = 100;
    final static short MIN_NODE_ID = 500;

    // this executor service will be shared with the consul client for callbacks
    // and it's running the regular health checks and session touches
    private final ScheduledExecutorService executorService;
    // this encapsulates the connection to consul and offers
    // convenience methods to talk to consul
    private final ConsulClient consulClient;
    // this boolean controls whether there is a globally unique id allocator run on the way already
    private final AtomicBoolean idAllocatorInFlight = new AtomicBoolean(false);
    // this holds a list of all ids that have been reserved on this
    private final LinkedBlockingQueue<Long> nextCacheLineIds = new LinkedBlockingQueue<>(ID_CHUNK_SIZE * 2);
    // this long demarcates the highest cache line that has been reserved by this node
    // since the cache line ids end up in the (roughly) ordered this serves as
    // a good approximation when a new allocator run needs to be triggered
    private final AtomicLong highWaterMarkCacheLineId = new AtomicLong(0);
    private final short myNodeId;
    // a boolean caching the up state of this node
    // this is determined by the regular session renewals that each node does
    private final AtomicBoolean isUp = new AtomicBoolean(false);
    // counts how often consul can't be reached during regular checkins
    private final AtomicInteger consulCheckinFailureCounter = new AtomicInteger(0);

    @Inject
    ConsulCluster(CarbonGrid.ServerConfig serverConfig, CarbonGrid.ConsulConfig consulConfig, PeerChangeConsumer peerChangeConsumer) {
        // this executor service that runs all sorts little threads
        // id allocation, health checks
        this.executorService = Executors.newScheduledThreadPool(2, new DefaultThreadFactory("consul-session-group"));
        this.consulClient = new ConsulClient(consulConfig, executorService);
        this.myNodeId = consulClient.myNodeId();
        this.consulClient.addPeerChangeConsumer(peerChangeConsumer);
        finishBootstrappingCluster(serverConfig, consulConfig);
    }

    private void finishBootstrappingCluster(CarbonGrid.ServerConfig serverConfig, CarbonGrid.ConsulConfig consulConfig) {
        // this method internally uses the executor service
        // beware to create the thing before calling into register
        consulClient.registerHealthCheckJobs(serverConfig.port(), (xcp) -> {
            consulCheckinFailureCounter.incrementAndGet();
            logger.error("", xcp);
            // TODO -- build death pill logic here
        });
        setDefaultCacheLineIdSeed();
        triggerGloballyUniqueIdAllocator();
        this.isUp.set(true);
    }

    private void setDefaultCacheLineIdSeed() {
        if (!consulClient.putIfAbsent(CACHE_LINE_ID_KEY, String.valueOf(Long.MIN_VALUE))) {
            logger.warn("Cache line id seed was already present");
        }
    }

    // id allocation happens in bulks
    // each node allocates a range of unique ids (by incrementing a counter in consul)
    // each id in this range of ids is then offered for consumption via the GloballyUniqueIdAllocator interface
    // all other nodes also compete for the same id ranges
    // this has the distinct backdraw that if a node dies, all reserved ids are lost forever
    @VisibleForTesting
    Pair<Long, Long> allocateIds(int numIdsToAllocate) {
        ConsulClient.ConsulValue highestCacheLineId;

        do {
            Optional<ConsulClient.ConsulValue> highestCacheLineIdOpt = consulClient.getValue(CACHE_LINE_ID_KEY);
            if (!highestCacheLineIdOpt.isPresent()) throw new IllegalStateException("Can't allocate new cache line ids");
            highestCacheLineId = highestCacheLineIdOpt.get();
            if (highestCacheLineIdOpt.get().asLong() + numIdsToAllocate > Long.MAX_VALUE) throw new IllegalStateException("Can't allocate cache line id " + (highestCacheLineIdOpt.get().asLong() + numIdsToAllocate));
            logger.info("Trying to allocate chunk {} - {}", highestCacheLineIdOpt.get().asLong(), highestCacheLineIdOpt.get().asLong() + numIdsToAllocate);
        } while (!consulClient.casValue(CACHE_LINE_ID_KEY, highestCacheLineId.asLong() + numIdsToAllocate, highestCacheLineId));
        return Pair.of(highestCacheLineId.asLong() - numIdsToAllocate, highestCacheLineId.asLong());
    }

    // if no other id allocation thread is in-flight, spawn a new one
    // all ids in the acquired range will be fed into the local (synchronized) queue
    // the GloballyUniqueIdAllocator will take ids out of this queue
    private void triggerGloballyUniqueIdAllocator() {
        if (!idAllocatorInFlight.get()) {
            synchronized (idAllocatorInFlight) {
                if (!idAllocatorInFlight.get()) {
                    if (!idAllocatorInFlight.compareAndSet(false, true)) {
                        logger.warn("idAllocatorInFlight was set to {} -- that's weird I'm overriding it to true anyway to proceed", idAllocatorInFlight.get());
                        idAllocatorInFlight.set(true);
                    }

                    executorService.submit(() -> {
                        long before = System.currentTimeMillis();
                        int numIdsToAllocate = Math.min(nextCacheLineIds.remainingCapacity(), ID_CHUNK_SIZE);
                        Pair<Long, Long> idChunk = allocateIds(numIdsToAllocate);
                        highWaterMarkCacheLineId.set(idChunk.getRight());
                        // all of these ids go into the id queue now
                        for (long i = idChunk.getLeft(); i < idChunk.getRight(); i++) {
                            nextCacheLineIds.offer(i);
                        }
                        logger.info("Reserved {} new ids in {} ms", numIdsToAllocate, System.currentTimeMillis() - before);
                        if (!idAllocatorInFlight.compareAndSet(true, false)) {
                            logger.warn("idAllocatorInFlight was set to {} -- that's weird I'm overriding it to false anyway to proceed", idAllocatorInFlight.get());
                            idAllocatorInFlight.set(false);
                        }
                    });
                }
            }
        }
    }

    @Override
    public boolean isUp() {
        return isUp.get();
    }

    @Override
    public short myNodeId() {
        return myNodeId;
    }

    @Override
    public GloballyUniqueIdAllocator getIdAllocator() {
        return () -> {
            Long id;
            try {
                id = nextCacheLineIds.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Can't allocate new cache line ids", e);
            }
            if (highWaterMarkCacheLineId.get() - id < 50) {
                triggerGloballyUniqueIdAllocator();
            }
            return id;
        };
    }

    // this method just returns the current state (as far as nodes and their health goes) from consul
    Map<Short, InetSocketAddress> getHealthyNodes() {
        return consulClient.getHealthyNodes();
    }

    @Override
    public String toString() {
        return "consulClient: " + consulClient + " myNodeId: " + myNodeId;
    }

    @Override
    public void close() throws IOException {
        try {
            consulClient.close();
        } finally {
            executorService.shutdown();
        }
    }
}
