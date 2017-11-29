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
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.Session;
import com.orbitz.consul.option.ImmutablePutOptions;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class ConsulCluster implements Cluster {
    private final static Logger logger = LoggerFactory.getLogger(ConsulCluster.class);
    private final static long CONSUL_TIMEOUT_SECS = 30L;
    private final static String NODE_ID_KEY_PREFIX = "node-ids/node-id-";
    private final static String CACHE_LINE_ID_KEY = "cache-lines/running-cache-line-id";
    private final static String serviceName = "carbon-grid";
    private final static int NUM_RETRIES = 10;
    private final static int ID_CHUNK_SIZE = 100;
    final static int MIN_NODE_ID = 500;

    private final Consul consul;
    private final String consulSessionId;
    private final ScheduledExecutorService executorService;
    private final AtomicBoolean idAllocatorInFlight = new AtomicBoolean(false);
    private final LinkedBlockingQueue<Long> nextCacheLineIds = new LinkedBlockingQueue<>(ID_CHUNK_SIZE * 2);
    private final AtomicLong highWaterMarkCacheLineId = new AtomicLong(0);

    private BiConsumer<EventType, InetSocketAddress> peerHandler;

    final String myNodeId;

    ConsulCluster(int myServicePort) {
        consul = Consul.builder()
                .withHostAndPort(HostAndPort.fromParts("localhost", 8500))
                .build();
        consulSessionId = createConsulSession();
        myNodeId = findMyNodeId();
        executorService = Executors.newScheduledThreadPool(2, new DefaultThreadFactory("consul-session-thread"));
        // this method internally uses the executor service
        // beware to create the thing before calling into register
        registerMyself(myServicePort);
        setDefaultCacheLineIdSeed();
        fireUpIdAllocator();

        // TODO -- add other peers at startup
        // TODO -- register callback in case node availability changes
        //List<ServiceHealth> nodes = consul.healthClient().getHealthyServiceInstances(serviceName).getResponse();
    }

    private String createConsulSession() {
        Session session = ImmutableSession.builder()
                .name("session-" + UUID.randomUUID().toString())
                .behavior("delete")
                .lockDelay("1s")
                .ttl(CONSUL_TIMEOUT_SECS + "s")
                .build();
        return consul.sessionClient().createSession(session).getId();
    }

    // this method tries to uniquely allocate a node id of type short
    // it does so by:
    // - querying all existing node ids
    // - sorting them
    // - finding the first gap in the sorted line of ids and take it
    // --- if there's no gaps, add one at the end
    // - then try to exclusively lock that key with the calculated id
    // - if I could get the lock, take it and be done
    // --- if not, loop all over again and calculate a new node id until I succeed
    protected String findMyNodeId() {
        KeyValueClient kvClient = consul.keyValueClient();
        int counter = 0;
        String myTentativeNodeId;
        do {
            if (counter > 0 && counter < NUM_RETRIES) {
                sleep(500 * counter);
            } else if (counter > NUM_RETRIES) {
                throw new RuntimeException("Can't acquire a new node id");
            }
            counter++;
            List<String> takenKeys = listKeys(kvClient, NODE_ID_KEY_PREFIX);
            myTentativeNodeId = calcNextNodeId(takenKeys);
            logger.info("Trying to acquire node id {}", myTentativeNodeId);
        } while (!kvClient.acquireLock(NODE_ID_KEY_PREFIX + myTentativeNodeId, consulSessionId));
        kvClient.putValue(NODE_ID_KEY_PREFIX + myTentativeNodeId, myTentativeNodeId);
        logger.info("Acquired node id {}", myTentativeNodeId);
        return myTentativeNodeId;
    }

    private List<String> listKeys(KeyValueClient kvClient, String nodePrefix) {
        try {
            return kvClient.getKeys(nodePrefix);
        } catch (ConsulException xcp) {
            if (xcp.getCode() == 404) {
                return Collections.emptyList();
            } else {
                logger.error("", xcp);
                throw xcp;
            }
        }
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException xcp) {
            throw new RuntimeException(xcp);
        }
    }

    private void setDefaultCacheLineIdSeed() {
        if (!consul.keyValueClient().putValue(CACHE_LINE_ID_KEY, String.valueOf(Long.MIN_VALUE), 0L, ImmutablePutOptions.builder().cas(0L).build())) {
            logger.warn("Cache line id seed was present already");
        }
    }

    // this method calculates the next node id in a list of all existing node ids
    protected String calcNextNodeId(List<String> takenKeys) {
        if (takenKeys.isEmpty()) {
            return String.valueOf(MIN_NODE_ID);
        }

        Collections.sort(takenKeys);
        takenKeys = takenKeys.stream().map(key -> key.substring(NODE_ID_KEY_PREFIX.length())).collect(Collectors.toList());

        if (Integer.valueOf(takenKeys.get(0)) != MIN_NODE_ID) {
            return String.valueOf(MIN_NODE_ID);
        }

        if (takenKeys.size() < 2 || Integer.valueOf(takenKeys.get(1)) != MIN_NODE_ID + 1) {
            return String.valueOf(MIN_NODE_ID + 1);
        }

        for (int i = 1; i < takenKeys.size(); i++) {
            int lastId = Integer.valueOf(takenKeys.get(i - 1));
            int currentId = Integer.valueOf(takenKeys.get(i));
            if (lastId + 1 != currentId) {
                return String.valueOf((short) (lastId + 1));
            }
        }

        if (MIN_NODE_ID + takenKeys.size() > Short.MAX_VALUE) {
            throw new RuntimeException("No more node ids to allocate");
        } else {
            return String.valueOf(MIN_NODE_ID + takenKeys.size());
        }
    }

    // register two scheduled jobs that keep refreshing the session and the service health check
    private void registerMyself(int myServicePort) {
        consul.agentClient().register(myServicePort, CONSUL_TIMEOUT_SECS, serviceName, myNodeId);
        executorService.scheduleAtFixedRate(
                () -> {
                    try {
                        consul.agentClient().pass(myNodeId);
                    } catch (NotRegisteredException xcp) {
                        throw new RuntimeException(xcp);
                    }
                },
                0L,
                CONSUL_TIMEOUT_SECS / 2,
                TimeUnit.SECONDS
        );

        executorService.scheduleAtFixedRate(
                () -> consul.sessionClient().renewSession(consulSessionId),
                0L,
                CONSUL_TIMEOUT_SECS / 2,
                TimeUnit.SECONDS
        );
    }

    @VisibleForTesting
    Pair<Long, Long> allocateIds(int numIdsToAllocate) {
        Long currentlyHighestCacheLineId = null;
        Long modifyIndex = 0L;

        do {
            Optional<Value> valueOpt = consul.keyValueClient().getValue(CACHE_LINE_ID_KEY);
            if (valueOpt.isPresent()) {
                modifyIndex = valueOpt.get().getModifyIndex();
                Optional<String> vOpt = valueOpt.get().getValueAsString();
                if (vOpt.isPresent()) {
                    currentlyHighestCacheLineId = Long.valueOf(vOpt.get());
                }
            }
            if (currentlyHighestCacheLineId == null) throw new IllegalStateException("Can't allocate new cache line ids");
            if (currentlyHighestCacheLineId + numIdsToAllocate > Long.MAX_VALUE) throw new IllegalStateException("Can't allocate cache line id " + (currentlyHighestCacheLineId + numIdsToAllocate));
            logger.info("Trying to allocate chunk {} - {}", currentlyHighestCacheLineId, currentlyHighestCacheLineId + numIdsToAllocate);
        } while (!consul.keyValueClient().putValue(CACHE_LINE_ID_KEY, String.valueOf(currentlyHighestCacheLineId + numIdsToAllocate), 0L, ImmutablePutOptions.builder().cas(modifyIndex).build()));
        return Pair.of(currentlyHighestCacheLineId - numIdsToAllocate, currentlyHighestCacheLineId);
    }

    private void fireUpIdAllocator() {
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
    public Supplier<Long> getIdSupplier() {
        return () -> {
            Long id;
            try {
                id = nextCacheLineIds.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Can't allocate new cache line ids", e);
            }
            if (highWaterMarkCacheLineId.get() - id < 50) {
                fireUpIdAllocator();
            }
            return id;
        };
    }

    @Override
    public String toString() {
        return "sessionId: " + consulSessionId + " myNodeId: " + myNodeId;
    }

    @Override
    public void close() throws IOException {
        try {
            consul.agentClient().deregister(myNodeId);
        } finally {
            try {
                consul.sessionClient().destroySession(consulSessionId);
            } finally {
                try {
                    executorService.shutdown();
                } finally {
                    consul.destroy();
                }
            }
        }
    }
}