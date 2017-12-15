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

import com.orbitz.consul.Consul;
import org.carbon.grid.BaseTest;
import org.carbon.grid.CarbonGrid;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsulClientTest extends BaseTest {
    @Test
    public void testRegister() {
        ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
        try (ConsulClient client = new ConsulClient(mockConsulConfig(), es)) {
            assertEquals(ConsulCluster.MIN_NODE_ID, client.myNodeId());
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testRegisterConcurrent() {
        Consul consul = createConsul();

        String prefix = UUID.randomUUID().toString();
        consul.keyValueClient().putValue( prefix + "-500", "500");
        consul.keyValueClient().putValue( prefix + "-501", "501");
        consul.keyValueClient().putValue( prefix + "-502", "502");
        // GAP
        consul.keyValueClient().putValue( prefix + "-504", "504");
        consul.keyValueClient().putValue( prefix + "-505", "505");

        CountDownLatch latch = new CountDownLatch(1);

        ScheduledExecutorService es = Executors.newScheduledThreadPool(2);
        try {
            es.submit(
                    () -> {
                        try (ConsulClient client123 = new ConsulClient(mockConsulConfig(), es) {
                            @Override
                            protected String calcNextNodeId(List<String> takenKeys) {
                                String nodeId = super.calcNextNodeId(takenKeys);
                                try {
                                    assertTrue(latch.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                                } catch (InterruptedException e) {
                                    fail();
                                }
                                return nodeId;
                            }
                        }) {
                            assertEquals((short)506, client123.myNodeId());
                        }
                    }
            );

            es.submit(
                    () -> {
                        try (ConsulClient client456 = new ConsulClient(mockConsulConfig(), es) {
                            @Override
                            protected short reserveMyNodeId() {
                                short s = super.reserveMyNodeId();
                                latch.countDown();
                                return s;
                            }
                        }) {
                            assertEquals((short)503, client456.myNodeId());
                        }
                    }
            );
        } finally {
            es.shutdown();
            consul.destroy();
        }
    }

    @Test
    public void testNodeInfoWatcher() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ScheduledExecutorService es = Executors.newScheduledThreadPool(2);
        try (ConsulClient client = new ConsulClient(mockConsulConfig(), es)) {
            assertEquals(ConsulCluster.MIN_NODE_ID, client.myNodeId());
            client.registerNodeInfoWatcher(ConsulCluster.NODE_INFO_KEY_PREFIX, nodeInfos -> {
                latch.countDown();
                assertEquals(0, nodeInfos.size());
            });

            assertTrue(latch.await(TIMEOUT_SECS, TimeUnit.SECONDS));
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testGetNodeInfos() {
        ScheduledExecutorService es = Executors.newScheduledThreadPool(2);
        try (ConsulClient client1 = new ConsulClient(mockConsulConfig(), es)) {
            List<NodeInfo> nodeInfos = client1.getAllNodeInfos();
            assertEquals(0, nodeInfos.size());
            NodeInfo client1NodeInfo = new NodeInfo(client1.myNodeId(), "dc1", Collections.emptySet(), -1);
            assertTrue(client1.setMyNodeInfo(client1NodeInfo));
            nodeInfos = client1.getAllNodeInfos();
            assertEquals(1, nodeInfos.size());
            try (ConsulClient client2 = new ConsulClient(mockConsulConfig(), es)) {
                nodeInfos = client2.getAllNodeInfos();
                assertEquals(1, nodeInfos.size());
                NodeInfo client2NodeInfo = new NodeInfo(client2.myNodeId(), "dc1", Collections.emptySet(), -1);
                client2.setMyNodeInfo(client2NodeInfo);
                nodeInfos = client1.getAllNodeInfos();
                assertEquals(2, nodeInfos.size());
            }
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testBasicConsulIO() {
        ScheduledExecutorService es = Executors.newScheduledThreadPool(2);
        try (ConsulClient client = new ConsulClient(mockConsulConfig(), es)) {
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            assertTrue(client.putValue(key, value));
            Optional<String> strOpt = client.getValueAsString(key);
            assertTrue(strOpt.isPresent());
            assertEquals(value, strOpt.get());
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testBuildCrushNodeHierarchy() {
        String dc1 = "dc1";
        String dc2 = "dc2";
        ScheduledExecutorService es = Executors.newScheduledThreadPool(9);
        try (ConsulClient client1 = new ConsulClient(mockConsulConfig(), es)) {
            client1.setMyNodeInfo(dc1, -1, Collections.emptyList());
            try (ConsulClient client2 = new ConsulClient(mockConsulConfig(), es)) {
                client2.setMyNodeInfo(dc2, -1, Collections.emptyList());
                try (ConsulClient client3 = new ConsulClient(mockConsulConfig(), es)) {
                    client3.setMyNodeInfo(dc2, -1, Collections.emptyList());
                    try (ConsulClient client4 = new ConsulClient(mockConsulConfig(), es)) {
                        client4.setMyNodeInfo(dc1, -1, Collections.emptyList());
                        try (ConsulClient client5 = new ConsulClient(mockConsulConfig(), es)) {
                            client5.setMyNodeInfo(dc2, -1, Collections.emptyList());
                            try (ConsulClient client6 = new ConsulClient(mockConsulConfig(), es)) {
                                client6.setMyNodeInfo(dc1, -1, Collections.emptyList());
                                try (ConsulClient client7 = new ConsulClient(mockConsulConfig(), es)) {
                                    client7.setMyNodeInfo(dc1, -1, Collections.emptyList());

                                    CrushNode cn = client1.buildCrushNodeHierarchy(client1.getAllNodeInfos());
                                    assertNotNull(cn);
                                    assertEquals(2, cn.getChildren().size());
                                    CrushNode dc1Node = cn.getChildByName(dc1);
                                    assertNotNull(dc1Node);
                                    assertEquals(4, dc1Node.getChildren().size());
                                    CrushNode dc2Node = cn.getChildByName(dc2);
                                    assertNotNull(dc2Node);
                                    assertEquals(3, dc2Node.getChildren().size());
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testReplicaPlacement() {
        short myNodeId = 15;
        CarbonGrid.ConsulConfig cc1 = mockConsulConfig("dc1");
        CarbonGrid.ConsulConfig cc2 = mockConsulConfig("dc2");
        CrushMap crushMap = CrushMap.builder()
                .addPlacementRule(CrushHierarchyLevel.DATA_CENTER, 2, i -> true)
                .addPlacementRule(CrushHierarchyLevel.NODE, 1, i -> true)
                .build();
        AtomicReference<List<Short>> myReplicaIds = new AtomicReference<>(Collections.emptyList());

        ScheduledExecutorService es = Executors.newScheduledThreadPool(2);
        try (ConsulClient client1 = new ConsulClient(mockConsulConfig(), es)) {
            client1.setMyNodeInfo(cc1.dataCenterName(), -1, Collections.emptyList());
            ConsulCluster.ReplicaPlacer rp = new ConsulCluster.ReplicaPlacer(myNodeId, cc1, client1, crushMap, myReplicaIds);
            List<NodeInfo> nodeInfos = client1.getAllNodeInfos();
            rp.accept(nodeInfos);
            assertTrue(myReplicaIds.get().isEmpty());
            try (ConsulClient client2 = new ConsulClient(mockConsulConfig(), es)) {
                client2.setMyNodeInfo(cc2.dataCenterName(), -1, Collections.emptyList());
                nodeInfos = client1.getAllNodeInfos();
                rp.accept(nodeInfos);
                assertEquals(2, myReplicaIds.get().size());

                Set<Short> nodeIds = new HashSet<>();
                nodeIds.add(client1.myNodeId());
                nodeIds.add(client2.myNodeId());

                for (Short s : myReplicaIds.get()) {
                    assertTrue(nodeIds.remove(s));
                }
            }
        }
    }
}
