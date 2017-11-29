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

import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsulClusterTest {
    private final static int TIMEOUT_SECS = 555;
    private final BiConsumer<Short, InetSocketAddress> emptyPeerHandler = (id, addr) -> {};

    @Test
    public void testRegister() throws IOException {
        try (ConsulCluster cluster = new ConsulCluster(9999, emptyPeerHandler)) {
            assertEquals(String.valueOf(ConsulCluster.MIN_NODE_ID), cluster.myNodeId);
        }
    }

    @Test
    public void testRegisterMultipleClusters() throws IOException {
        Set<String> nodeIds = new HashSet<String>() {{
            add(String.valueOf(ConsulCluster.MIN_NODE_ID));
            add(String.valueOf(ConsulCluster.MIN_NODE_ID + 1));
            add(String.valueOf(ConsulCluster.MIN_NODE_ID + 2));
        }};
        try (ConsulCluster cluster123 = new ConsulCluster(7777, emptyPeerHandler)) {
            try (ConsulCluster cluster456 = new ConsulCluster(8888, emptyPeerHandler)) {
                try (ConsulCluster cluster789 = new ConsulCluster(9999, emptyPeerHandler)) {
                    assertTrue(nodeIds.remove(cluster123.myNodeId));
                    assertTrue(nodeIds.remove(cluster456.myNodeId));
                    assertTrue(nodeIds.remove(cluster789.myNodeId));
                }
            }
        }

        assertTrue(nodeIds.isEmpty());
    }

    @Test
    public void testRegisterConcurrent() throws IOException {
        Consul consul = Consul.builder()
                .withHostAndPort(HostAndPort.fromParts("localhost", 8500))
                .build();

        String prefix = UUID.randomUUID().toString();
        consul.keyValueClient().putValue( prefix + "-500", "500");
        consul.keyValueClient().putValue( prefix + "-501", "501");
        consul.keyValueClient().putValue( prefix + "-502", "502");
        // GAP
        consul.keyValueClient().putValue( prefix + "-504", "504");
        consul.keyValueClient().putValue( prefix + "-505", "505");

        CountDownLatch latch = new CountDownLatch(1);

        ExecutorService es = Executors.newFixedThreadPool(2);
        try {
            es.submit(
                    () -> {
                        try (ConsulCluster cluster123 = new ConsulCluster(7777, emptyPeerHandler) {
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
                            assertEquals("506", cluster123.myNodeId);
                        } catch (IOException e) {
                            fail();
                        }
                    }
            );

            es.submit(
                    () -> {
                        try (ConsulCluster cluster456 = new ConsulCluster(8888, emptyPeerHandler) {
                            @Override
                            protected String findMyNodeId() {
                                String s = super.findMyNodeId();
                                latch.countDown();
                                return s;
                            }
                        }) {
                            assertEquals("503", cluster456.myNodeId);
                        } catch (IOException e) {
                            fail();
                        }
                    }
            );
        } finally {
            es.shutdown();
            consul.destroy();
        }
    }

    @Test
    public void testAllocateIds() throws IOException {
        try (ConsulCluster cluster123 = new ConsulCluster(7777, emptyPeerHandler)) {
            Pair<Long, Long> chunk = cluster123.allocateIds(1);
            assertEquals(chunk.getLeft() + 1L, chunk.getRight().longValue());
            Supplier<Long> idSupplier = cluster123.getIdSupplier();
            assertNotNull(idSupplier.get());
        }
    }

    @Test
    public void testGetHealthyNodes() throws IOException {
        try (ConsulCluster cluster123 = new ConsulCluster(7777, emptyPeerHandler)) {
            try (ConsulCluster cluster456 = new ConsulCluster(8888, emptyPeerHandler)) {
                try (ConsulCluster cluster789 = new ConsulCluster(9999, emptyPeerHandler)) {
                    Map<Short, InetSocketAddress> nodesToAddr = cluster123.getHealthyNodes();
                    assertEquals(3, nodesToAddr.size());
                    assertTrue(nodesToAddr.containsKey(Short.valueOf(cluster123.myNodeId)));
                    assertTrue(nodesToAddr.containsKey(Short.valueOf(cluster456.myNodeId)));
                    assertTrue(nodesToAddr.containsKey(Short.valueOf(cluster789.myNodeId)));
                }
            }
        }
    }

    @Test
    public void testNodeHealthListener() throws IOException, InterruptedException {
        Map<Short, InetSocketAddress> nodeIdToAddr = new HashMap<>();
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch addedFirstBatch = new CountDownLatch(2);
        CountDownLatch addedSecondBatch = new CountDownLatch(5);

        try (ConsulCluster cluster123 = new ConsulCluster(7777, emptyPeerHandler)) {
            try (ConsulCluster cluster456 = new ConsulCluster(8888, (nodeId, addr) -> {
                nodeIdToAddr.put(nodeId, addr);
                count.incrementAndGet();
                addedFirstBatch.countDown();
                addedSecondBatch.countDown();
            })) {
                assertEquals(2, cluster123.getHealthyNodes().size());
                assertTrue(addedFirstBatch.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                assertEquals(2, nodeIdToAddr.size());
                assertEquals(2, count.get());
                try (ConsulCluster cluster789 = new ConsulCluster(9999, emptyPeerHandler)) {
                    assertEquals(3, cluster789.getHealthyNodes().size());
                    assertEquals(3, cluster123.getHealthyNodes().size());
                    assertTrue(addedSecondBatch.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                    assertEquals(3, nodeIdToAddr.size());
                    assertEquals(2 + 3, count.get());
                }
            }
        }
    }
}
