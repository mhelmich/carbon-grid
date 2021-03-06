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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.Session;
import com.orbitz.consul.option.ImmutablePutOptions;
import com.orbitz.consul.option.QueryOptions;
import io.netty.util.internal.SocketUtils;
import org.carbon.grid.CarbonGrid;
import org.carbon.grid.cache.PeerChangeConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class ConsulClient implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(ConsulClient.class);

    final static String SERVICE_NAME = "carbon-grid";
    private final static short MIN_NODE_ID = 500;
    private final static int NUM_RETRIES = 10;

    private final String consulSessionName = "session-" + UUID.randomUUID().toString();
    private final LinkedList<ConsulValueWatcher<?>> watchers = new LinkedList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final CarbonGrid.ConsulConfig consulConfig;
    private final ScheduledExecutorService executorService;
    private final Consul consul;
    private final short myNodeId;
    private final String myNodeIdStr;
    private final String consulSessionId;

    ConsulClient(CarbonGrid.ConsulConfig consulConfig, ScheduledExecutorService executorService) {
        this.consulConfig = consulConfig;
        this.executorService = executorService;
        logger.info("Connecting to consul at {} {}", consulConfig.host(), consulConfig.port());
        this.consul = Consul.builder()
                .withHostAndPort(HostAndPort.fromParts(consulConfig.host(), consulConfig.port()))
                .withExecutorService(executorService)
                .build();
        this.consulSessionId = createConsulSession(consulConfig);
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

    boolean setMyNodeInfo(NodeInfo myNodeInfo) throws IOException {
        return putValue(ConsulCluster.NODE_INFO_KEY_PREFIX + myNodeIdStr, objectMapper.writeValueAsString(myNodeInfo));
    }

    boolean setMyNodeInfo(String dataCenter, List<Short> leaderIds, List<Short> replicaIds) throws IOException {
        return setMyNodeInfo(new NodeInfo(myNodeId, dataCenter, replicaIds, leaderIds));
    }

    void applyTreeToTree(CrushNode applyToMe, CrushNode treeToApply) {
        List<CrushNode> children = applyToMe.getChildren();
        for (CrushNode node : children) {
            CrushNode n = treeToApply.getChildByName(node.getNodeName());
            if (n == null) {
                setDeadRecursively(node, true);
            } else {
                node.setDead(false);
                node.setFull(n.isFull());
                if (!n.isLeaf()) {
                    applyTreeToTree(node, n);
                }
            }
        }

        for (CrushNode nodeToAdd : treeToApply.getChildren()) {
            if (applyToMe.getChildByName(nodeToAdd.getNodeName()) == null) {
                applyToMe.addChild(nodeToAdd.getNodeName(), nodeToAdd);
            }
        }
    }

    private void setDeadRecursively(CrushNode node, boolean isDead) {
        node.setDead(isDead);
        if (!node.isLeaf()) {
            for (CrushNode child : node.getChildren()) {
                setDeadRecursively(child, isDead);
            }
        }
    }

    CrushNode buildCrushNodeHierarchy(List<NodeInfo> nodeInfos) {
        Map<String, List<NodeInfo>> dcToNI = new HashMap<>();

        for (NodeInfo ni : nodeInfos) {
            List<NodeInfo> newList = new LinkedList<>();
            List<NodeInfo> oldList = dcToNI.putIfAbsent(ni.dataCenter, newList);
            if (oldList == null) {
                newList.add(ni);
            } else {
                oldList.add(ni);
            }
        }

        CrushNode root = new CrushNode(CrushHierarchyLevel.ROOT, "root");

        for (Map.Entry<String, List<NodeInfo>> e : dcToNI.entrySet()) {
            CrushNode child = root.getChildByName(e.getKey());
            if (child == null) {
                CrushNode dcNode = new CrushNode(CrushHierarchyLevel.DATA_CENTER, e.getKey());
                root.addChild(e.getKey(), dcNode);
                child = root.getChildByName(e.getKey());
            }
            for (NodeInfo ni : e.getValue()) {
                String nodeName = String.valueOf(ni.nodeId);
                CrushNode node = new CrushNode(CrushHierarchyLevel.NODE, ni.nodeId);
                child.addChild(nodeName, node);
            }
        }

        return root;
    }

    List<NodeInfo> getAllNodeInfos() {
        List<String> nodeInfoKeys = listKeys(ConsulCluster.NODE_INFO_KEY_PREFIX);
        List<NodeInfo> nodeInfos = new LinkedList<>();
        for (String nodeInfoKey : nodeInfoKeys) {
            Optional<String> niOpt = getValueAsString(nodeInfoKey);
            if (niOpt.isPresent()) {
                NodeInfo ni = null;
                try {
                    ni = objectMapper.readValue(niOpt.get(), NodeInfo.class);
                } catch (IOException e) {
                    logger.error("Couldn't deserialize node info", e);
                }
                nodeInfos.add(ni);
            }
        }
        return nodeInfos;
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
                        logger.error("", xcp);
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
                        logger.error("", xcp);
                        healthCheckFailedCallback.accept(xcp);
                    }
                },
                0L,
                // give this job two chances to reach out to consul to ping healthy
                consulConfig.timeout() / 2,
                TimeUnit.SECONDS
        );
    }

    void registerNodeHealthWatcher(PeerChangeConsumer peerChangeConsumer) {
        // this method registers a callback with consul
        // every time the health of a node changes, consul sends back the entire state (!!!)
        // that means two things:
        // a) the peerChangeConsumer needs to be really fast
        // b) the peerChangeConsumer needs to dedup existing nodes from new nodes (by putting the into a map or something)
        ConsulValueWatcher<List<ServiceHealth>> serviceHealthWatcher = new ConsulValueWatcher<>(executorService,
                (index, responseCallback) -> {
                    QueryOptions params = ConsulValueWatcher.generateBlockingQueryOptions(index, 10);
                    consul.healthClient().getHealthyServiceInstances(SERVICE_NAME, params, responseCallback);
                },
                (list) -> {
                    ImmutableMap.Builder<Short, InetSocketAddress> mb = ImmutableMap.builder();
                    for (ServiceHealth sh : list) {
                        Service s = sh.getService();
                        mb.put(Short.valueOf(s.getId()), SocketUtils.socketAddress(s.getAddress(), s.getPort()));
                    }

                    ImmutableMap<Short, InetSocketAddress> m = mb.build();
                    peerChangeConsumer.accept(m);
                }
        );

        watchers.add(serviceHealthWatcher);
    }

    void registerNodeInfoWatcher(String keyPrefix, Consumer<List<NodeInfo>> nodeInfoConsumer) {
        ConsulValueWatcher<List<Value>> nodeInfoWatcher = new ConsulValueWatcher<>(executorService,
                (index, responseCallback) -> {
                    QueryOptions params = ConsulValueWatcher.generateBlockingQueryOptions(index, 10);
                    consul.keyValueClient().getValues(keyPrefix, params, responseCallback);
                },
                valueList -> {
                    List<NodeInfo> nis = new LinkedList<>();
                    for (Value v : valueList) {
                        if (v.getValueAsString().isPresent()) {
                            String nodeInfoStr = v.getValueAsString().get();
                            if (!nodeInfoStr.isEmpty()) {
                                NodeInfo ni = null;
                                try {
                                    ni = objectMapper.readValue(nodeInfoStr, NodeInfo.class);
                                } catch (IOException xcp) {
                                    logger.error("Key {} doesn't have a value", v.getKey(), xcp);
                                }
                                nis.add(ni);
                            } else {
                                logger.warn("Key {} doesn't have a value", v.getKey());
                            }
                        } else {
                            logger.warn("Key {} doesn't have a value", v.getKey());
                        }
                    }

                    nodeInfoConsumer.accept(nis);
                }
        );
        watchers.add(nodeInfoWatcher);
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
        if (valueOpt.isPresent() && valueOpt.get().getValueAsString().isPresent()) {
            return java.util.Optional.of(valueOpt.get().getValueAsString().get());
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
    public void close() {
        try {
            watchers.forEach(ConsulValueWatcher::close);
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
}
