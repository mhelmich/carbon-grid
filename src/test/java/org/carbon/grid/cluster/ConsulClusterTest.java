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

import org.apache.commons.lang3.tuple.Pair;
import org.carbon.grid.BaseTest;
import org.carbon.grid.cache.PeerChangeConsumer;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConsulClusterTest extends BaseTest {
    private final PeerChangeConsumer emptyPeerHandler = m -> {};

    @Test
    public void testRegisterMultipleClusters() throws IOException {
        Set<Short> nodeIds = new HashSet<Short>() {{
            add(ConsulCluster.MIN_NODE_ID);
            add((short) (ConsulCluster.MIN_NODE_ID + 1));
            add((short) (ConsulCluster.MIN_NODE_ID + 2));
        }};
        try (ConsulCluster cluster123 = mockConsulCluster(7777, emptyPeerHandler)) {
            try (ConsulCluster cluster456 = mockConsulCluster(8888, emptyPeerHandler)) {
                try (ConsulCluster cluster789 = mockConsulCluster(9999, emptyPeerHandler)) {
                    assertTrue(nodeIds.remove(cluster123.myNodeId()));
                    assertTrue(nodeIds.remove(cluster456.myNodeId()));
                    assertTrue(nodeIds.remove(cluster789.myNodeId()));
                }
            }
        }

        assertTrue(nodeIds.isEmpty());
    }

    @Test
    public void testAllocateIds() throws IOException {
        try (ConsulCluster cluster123 = mockConsulCluster(7777, emptyPeerHandler)) {
            Pair<Long, Long> chunk = cluster123.allocateIds(1);
            assertEquals(chunk.getLeft() + 1L, chunk.getRight().longValue());
            GloballyUniqueIdAllocator idSupplier = cluster123.getIdAllocator();
            assertNotNull(idSupplier.nextUniqueId());
        }
    }

    @Test
    public void testGetHealthyNodes() throws IOException, InterruptedException {
        try (ConsulCluster cluster123 = mockConsulCluster(7777, emptyPeerHandler)) {
            try (ConsulCluster cluster456 = mockConsulCluster(8888, emptyPeerHandler)) {
                try (ConsulCluster cluster789 = mockConsulCluster(9999, emptyPeerHandler)) {
                    // TODO -- Test.flap()
                    Thread.sleep(500);
                    Map<Short, InetSocketAddress> nodesToAddr = cluster123.getHealthyNodes();
                    assertEquals(3, nodesToAddr.size());
                    assertTrue(nodesToAddr.containsKey(cluster123.myNodeId()));
                    assertTrue(nodesToAddr.containsKey(cluster456.myNodeId()));
                    assertTrue(nodesToAddr.containsKey(cluster789.myNodeId()));
                }
            }
        }
    }

    @Test
    public void testNodeHealthListener() throws IOException, InterruptedException {
        AtomicReference<Map<Short, InetSocketAddress>> nodeIdToAddr = new AtomicReference<>(Collections.emptyMap());
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch addedFirstBatch = new CountDownLatch(1);
        CountDownLatch addedSecondBatch = new CountDownLatch(2);
        CountDownLatch lastNodeRemoved = new CountDownLatch(3);

        try (ConsulCluster cluster123 = mockConsulCluster(7777, emptyPeerHandler)) {
            try (ConsulCluster cluster456 = mockConsulCluster(8888, m -> {
                nodeIdToAddr.set(m);
                count.incrementAndGet();
                addedFirstBatch.countDown();
                addedSecondBatch.countDown();
                lastNodeRemoved.countDown();
            })) {
                assertTrue(addedFirstBatch.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                assertEquals(2, cluster123.getHealthyNodes().size());
                assertEquals(2, nodeIdToAddr.get().size());
                assertEquals(1, count.get());
                try (ConsulCluster cluster789 = mockConsulCluster(9999, emptyPeerHandler)) {
                    assertTrue(addedSecondBatch.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                    assertEquals(3, cluster789.getHealthyNodes().size());
                    assertEquals(3, cluster123.getHealthyNodes().size());
                    assertEquals(3, nodeIdToAddr.get().size());
                    assertEquals(2, count.get());
                }

                assertTrue(lastNodeRemoved.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                assertEquals(2, cluster123.getHealthyNodes().size());
                assertEquals(2, nodeIdToAddr.get().size());
                assertEquals(3, count.get());
            }
        }
    }

    @Test
    public void testReplicaSupplier() throws IOException, InterruptedException {
        String dataCenter1 = "dc1";
        String dataCenter2 = "dc2";
        try (ConsulCluster cluster123 = mockConsulCluster(7777, dataCenter1, emptyPeerHandler)) {
            ReplicaIdSupplier supplier = cluster123.getReplicaIdSupplier();
            assertEquals(0, supplier.get().size());
            try (ConsulCluster cluster456 = mockConsulCluster(8888, dataCenter1, emptyPeerHandler)) {
                assertEquals(0, supplier.get().size());
                try (ConsulCluster cluster789 = mockConsulCluster(9999, dataCenter2, emptyPeerHandler)) {
                    // Test.flap
                    Thread.sleep(500);
                    assertEquals(2, supplier.get().size());
                    assertFalse(supplier.get().contains(cluster123.myNodeId()));
                }
            }
        }
    }

    private ConsulCluster mockConsulCluster(int servicePort, String dataCenter, PeerChangeConsumer peerChangeConsumer) {
        return new ConsulCluster(mockServerConfig(servicePort), mockConsulConfig(dataCenter), peerChangeConsumer);
    }

    private ConsulCluster mockConsulCluster(int servicePort, PeerChangeConsumer peerChangeConsumer) {
        return new ConsulCluster(mockServerConfig(servicePort), mockConsulConfig(), peerChangeConsumer);
    }
}
