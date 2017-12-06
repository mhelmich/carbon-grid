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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.Session;
import com.orbitz.consul.option.ImmutablePutOptions;
import io.netty.util.internal.SocketUtils;
import org.carbon.grid.CarbonGrid;
import org.carbon.grid.cache.PeerChangeConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class ConsulClient implements Closeable {

    static class ConsulValue {
        final String s;
        private final long modifyIndex;

        ConsulValue(String s, long modifyIndex) {
            this.s = s;
            this.modifyIndex = modifyIndex;
        }

        long asLong() {
            return Long.valueOf(s);
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(ConsulClient.class);

    final static String SERVICE_NAME = "carbon-grid";
    private final static short MIN_NODE_ID = 500;
    private final static int NUM_RETRIES = 10;

    private final CarbonGrid.ConsulConfig consulConfig;
    private final ScheduledExecutorService executorService;
    private final Consul consul;
    private final String consulSessionName = "session-" + UUID.randomUUID().toString();
    private final short myNodeId;
    private final String myNodeIdStr;
    private final String consulSessionId;
    private final ServiceHealthCache shCache;

    ConsulClient(CarbonGrid.ConsulConfig consulConfig, ScheduledExecutorService executorService) {
        this.consulConfig = consulConfig;
        this.executorService = executorService;
        this.consul = Consul.builder()
                .withHostAndPort(HostAndPort.fromParts(consulConfig.host(), consulConfig.port()))
                .withExecutorService(executorService)
                .build();
        this.consulSessionId = createConsulSession(consulConfig);
        this.shCache = newServiceHealthCache();
        this.myNodeId = reserveMyNodeId();
        this.myNodeIdStr = String.valueOf(this.myNodeId);
    }

    private String createConsulSession(CarbonGrid.ConsulConfig consulConfig) {
        Session session = ImmutableSession.builder()
                .name(consulSessionName)
                .behavior("delete")
                .lockDelay("1s")
                .ttl(consulConfig.timeout() + "s")
                .build();
        return consul.sessionClient().createSession(session).getId();
    }

    private ServiceHealthCache newServiceHealthCache() {
        ServiceHealthCache cache = ServiceHealthCache.newCache(consul.healthClient(), SERVICE_NAME);
        try {
            cache.start();
        } catch (Exception xcp) {
            throw new RuntimeException(xcp);
        }
        return cache;
    }

    // register two scheduled jobs that keep refreshing the session and the service health check
    void registerHealthCheckJobs(int myServicePort, Consumer<Exception> healthCheckFailedCallback) {
        consul.agentClient().register(myServicePort, consulConfig.timeout(), SERVICE_NAME, myNodeIdStr);
        executorService.scheduleAtFixedRate(
                () -> {
                    try {
                        consul.agentClient().pass(myNodeIdStr);
                    } catch (NotRegisteredException xcp) {
                        throw new RuntimeException(xcp);
                    } catch (Exception xcp) {
                        healthCheckFailedCallback.accept(xcp);
                    }
                },
                0L,
                // give this job two chances to reach out to consul to ping healthy
                consulConfig.timeout() / 2,
                TimeUnit.SECONDS
        );

        executorService.scheduleAtFixedRate(
                () -> {
                    try {
                        consul.sessionClient().renewSession(consulSessionId);
                    } catch (Exception xcp) {
                        healthCheckFailedCallback.accept(xcp);
                    }
                },
                0L,
                // give this job two chances to reach out to consul to ping healthy
                consulConfig.timeout() / 2,
                TimeUnit.SECONDS
        );
    }

    Map<Short, InetSocketAddress> getHealthyNodes() {
        List<ServiceHealth> nodes = consul.healthClient().getHealthyServiceInstances(SERVICE_NAME).getResponse();
        Map<Short, InetSocketAddress> nodesToAddr = nodes.stream()
                .map(ServiceHealth::getService)
                .collect(Collectors.toMap(
                        s -> Short.valueOf(s.getId()),
                        s -> SocketUtils.socketAddress(s.getAddress(), s.getPort())
                ));
        return ImmutableMap.copyOf(nodesToAddr);
    }

    private List<String> listKeys(String nodePrefix) {
        try {
            return consul.keyValueClient().getKeys(nodePrefix);
        } catch (ConsulException xcp) {
            if (xcp.getCode() == 404) {
                return Collections.emptyList();
            } else {
                logger.error("", xcp);
                throw xcp;
            }
        }
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
    protected short reserveMyNodeId() {
        int counter = 0;
        String myTentativeNodeId;
        do {
            if (counter > 0 && counter < NUM_RETRIES) {
                sleep(500 * counter);
            } else if (counter > NUM_RETRIES) {
                throw new RuntimeException("Can't acquire a new node id");
            }
            counter++;
            List<String> takenKeys = listKeys(ConsulCluster.NODE_INFO_KEY_PREFIX);
            myTentativeNodeId = calcNextNodeId(takenKeys);
            logger.info("Trying to acquire node id {}", myTentativeNodeId);
        } while (!acquireLock(ConsulCluster.NODE_INFO_KEY_PREFIX + myTentativeNodeId));
        // fill in value to be empty
        // that's where the NodeInfo will go later when we know this nodes role, etc.
        putValue(ConsulCluster.NODE_INFO_KEY_PREFIX + myTentativeNodeId, "");
        logger.info("Acquired node id {}", myTentativeNodeId);
        return Short.valueOf(myTentativeNodeId);
    }

    // this method calculates the next node id in a list of all existing node ids
    protected String calcNextNodeId(List<String> takenKeys) {
        if (takenKeys.isEmpty()) {
            return String.valueOf(MIN_NODE_ID);
        }

        Collections.sort(takenKeys);
        takenKeys = takenKeys.stream().map(key -> key.substring(ConsulCluster.NODE_INFO_KEY_PREFIX .length())).collect(Collectors.toList());

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

    boolean acquireLockWithWait(String key) {
        int count = 0;
        while (!acquireLock(key)) {
            count++;
            if (count >= NUM_RETRIES) {
                return false;
            } else {
                sleep(count * 50);
            }
        }
        return true;
    }

    private boolean acquireLock(String key) {
        return consul.keyValueClient().acquireLock(key, this.consulSessionId);
    }

    boolean releaseLock(String key) {
        return consul.keyValueClient().releaseLock(key, this.consulSessionId);
    }

    boolean putValue(String key, String value) {
        return consul.keyValueClient().putValue(key, value);
    }

    boolean putIfAbsent(String key, String value) {
        return consul.keyValueClient().putValue(key, value, 0L, ImmutablePutOptions.builder().cas(0L).build());
    }

    java.util.Optional<String> getValueAsString(String key) {
        com.google.common.base.Optional<Value> valueOpt = consul.keyValueClient().getValue(key);
        if (valueOpt.isPresent() && valueOpt.get().getValue().isPresent()) {
            return java.util.Optional.of(valueOpt.get().getValue().get());
        } else {
            return java.util.Optional.empty();
        }
    }

    java.util.Optional<ConsulValue> getValue(String key) {
        com.google.common.base.Optional<Value> valueOpt = consul.keyValueClient().getValue(key);
        if (valueOpt.isPresent() && valueOpt.get().getValueAsString().isPresent()) {
            return java.util.Optional.of(
                    new ConsulValue(
                        valueOpt.get().getValueAsString().get(),
                        valueOpt.get().getModifyIndex()
                    )
            );
        } else {
            return java.util.Optional.empty();
        }
    }

    boolean casValue(String key, long value, ConsulValue oldValue) {
        return casValue(key, String.valueOf(value), oldValue);
    }

    private boolean casValue(String key, String value, ConsulValue oldValue) {
        return consul.keyValueClient().putValue(key, value, 0L, ImmutablePutOptions.builder().cas(oldValue.modifyIndex).build());
    }

    // this method registers a callback with consul
    // every time the health of a node changes, consul sends back the entire state (!!!)
    // that means two things:
    // a) the peerChangeConsumer needs to be really fast
    // b) the peerChangeConsumer needs to dedup existing nodes from new nodes (by putting the into a map or something)
    boolean addPeerChangeConsumer(PeerChangeConsumer peerChangeConsumer) {
        return shCache.addListener(
                hostsAndHealth -> {
                    Map<Short, InetSocketAddress> m = hostsAndHealth.keySet().stream()
                            .collect(Collectors.toMap(
                                    key -> Short.valueOf(key.getServiceId()),
                                    key -> SocketUtils.socketAddress(key.getHost(), key.getPort())
                            ));
                    peerChangeConsumer.accept(m);
                }
        );
    }

    short myNodeId() {
        return myNodeId;
    }

    private void sleep(int ms) {
        logger.info("Sleeping {}ms", ms);
        try {
            Thread.sleep(ms);
        } catch (InterruptedException xcp) {
            throw new RuntimeException(xcp);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            shCache.stop();
        } catch (Exception xcp) {
            logger.warn("Error during shutdown ... but at this point I don't really care anymore", xcp);
        } finally {
            try {
                consul.agentClient().deregister(myNodeIdStr);
            } finally {
                try {
                    consul.sessionClient().destroySession(consulSessionId);
                } finally {
                    consul.destroy();
                }
            }
        }
    }

    @Override
    public String toString() {
        return "myNodeId: " + myNodeIdStr + " sessionId: " + consulSessionId + " consulSessionName: " + consulSessionName;
    }
}
