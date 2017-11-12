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

package org.carbon.grid;

import io.netty.channel.EventLoopGroup;
import io.netty.util.internal.SocketUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

class NodeRegistry implements Closeable {
    private final ConcurrentHashMap<Short, InetSocketAddress> nodeIdToAddress = new ConcurrentHashMap<>(16, .75f, 2);
    private final ConcurrentHashMap<Short, UdpGridClient> nodeIdToClient = new ConcurrentHashMap<>(128, .75f, 32);

    private final EventLoopGroup workerGroup;
    private final Cache cache;

    NodeRegistry(EventLoopGroup workerGroup, Cache cache) {
        this.workerGroup = workerGroup;
        this.cache = cache;
    }

    void addPeer(short nodeId, String host, int port) {
        nodeIdToAddress.put(nodeId, SocketUtils.socketAddress(host, port));
    }

    private InetSocketAddress lookup(short nodeId) {
        return nodeIdToAddress.get(nodeId);
    }

    UdpGridClient getClientForNode(short nodeId) {
        nodeIdToClient.putIfAbsent(nodeId, new UdpGridClient(lookup(nodeId), workerGroup, cache));
        return nodeIdToClient.get(nodeId);
    }

    @Override
    public void close() throws IOException {
        for (UdpGridClient client : nodeIdToClient.values()) {
            client.close();
        }
    }
}
