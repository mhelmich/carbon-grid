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
import org.carbon.grid.BaseTest;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
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
}
