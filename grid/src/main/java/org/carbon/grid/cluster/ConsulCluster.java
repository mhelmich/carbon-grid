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

import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.option.ImmutablePutOptions;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class ConsulCluster implements Cluster {

    private final static long CONSUL_TIMEOUT = 4L;
    private final static String NODE_ID_KEY = "NODE_ID";
    private final static String serviceName = "carbon-grid";
    private final Consul consul;
    private final String myNodeId;

    private final ScheduledExecutorService healthCheckES;

    ConsulCluster(int myServicePort) {
        consul = Consul.builder()
                .withHostAndPort(HostAndPort.fromParts("localhost", 8500))
                .build();
        myNodeId = findMyNodeId();
        healthCheckES = registerMyself(myServicePort);

//        List<ServiceHealth> nodes = consul.healthClient().getHealthyServiceInstances(serviceName).getResponse();
    }

    private String findMyNodeId() {
        KeyValueClient kvClient = consul.keyValueClient();
        String nodeId;
        long modifyIndex;
        do {
            Optional<Value> valueOpt = kvClient.getValue(NODE_ID_KEY);
            modifyIndex = valueOpt.isPresent() ? valueOpt.get().getModifyIndex() : 0L;
            nodeId = valueOpt.isPresent() ? valueOpt.get().getValueAsString().get() : "0";
        } while (!kvClient.putValue(NODE_ID_KEY, "", 0L, ImmutablePutOptions.builder().cas(modifyIndex).build()));
        return nodeId;
    }

    private ScheduledExecutorService registerMyself(int myServicePort) {
        consul.agentClient().register(myServicePort, CONSUL_TIMEOUT, serviceName, myNodeId);
        ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
        es.scheduleAtFixedRate(
                () -> {
                    try {
                        consul.agentClient().pass(myNodeId);
                    } catch (NotRegisteredException xcp) {
                        throw new RuntimeException(xcp);
                    }
                },
                0L,
                CONSUL_TIMEOUT / 2,
                TimeUnit.SECONDS
        );

        return es;
    }

    @Override
    public void close() throws IOException {
        try {
            consul.agentClient().deregister(myNodeId);
        } finally {
            try {
                healthCheckES.shutdown();
            } finally {
                consul.destroy();
            }
        }
    }
}
