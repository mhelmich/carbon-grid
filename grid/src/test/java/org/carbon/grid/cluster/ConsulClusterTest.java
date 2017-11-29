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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsulClusterTest {
    private final static int TIMEOUT_SECS = 555;
    @Test
    public void testRegister() throws IOException {
        try (ConsulCluster cluster = new ConsulCluster(9999)) {
            assertEquals(String.valueOf(ConsulCluster.MIN_NODE_ID), cluster.myNodeId);
        }
    }

    @Test
    public void testRegisterMultipleClusters() throws IOException {
        try (ConsulCluster cluster123 = new ConsulCluster(7777)) {
            try (ConsulCluster cluster456 = new ConsulCluster(8888)) {
                try (ConsulCluster cluster789 = new ConsulCluster(9999)) {
                    assertEquals(String.valueOf(ConsulCluster.MIN_NODE_ID), cluster123.myNodeId);
                    assertEquals(String.valueOf(ConsulCluster.MIN_NODE_ID + 1), cluster456.myNodeId);
                    assertEquals(String.valueOf(ConsulCluster.MIN_NODE_ID + 2), cluster789.myNodeId);
                }
            }
        }
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
        consul.keyValueClient().putValue( prefix + "-504", "504");
        consul.keyValueClient().putValue( prefix + "-505", "505");

        CountDownLatch latch = new CountDownLatch(1);

        ExecutorService es = Executors.newFixedThreadPool(2);
        try {
            es.submit(
                    () -> {
                        try (ConsulCluster cluster123 = new ConsulCluster(7777) {
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
                        try (ConsulCluster cluster456 = new ConsulCluster(8888) {
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
        try (ConsulCluster cluster123 = new ConsulCluster(7777)) {
            Pair<Long, Long> chunk = cluster123.allocateIds(1);
            assertEquals(chunk.getLeft() + 1L, chunk.getRight().longValue());
            Supplier<Long> idSupplier = cluster123.getIdSupplier();
            assertNotNull(idSupplier.get());
        }
    }
}
