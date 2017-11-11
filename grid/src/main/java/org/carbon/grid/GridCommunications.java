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
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

class GridCommunications implements AutoCloseable, Closeable {
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private final NodeRegistry nodeRegistry = new NodeRegistry();
    private final UdpGridServer udpGridServer;
    private final Cache cache;

    GridCommunications(int port, Cache cache) {
        this.udpGridServer = new UdpGridServer(port, workerGroup, cache);
        this.cache = cache;
    }

    void addPeer(short nodeId, String host, int port) {
        nodeRegistry.addPeer(nodeId, host, port);
    }

    Future<Void> send(Message msg) throws IOException {
        // TODO -- factor this into a connection pool
        try (UdpGridClient client = new UdpGridClient(nodeRegistry, workerGroup, cache)) {
            return client.send(msg);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            udpGridServer.close();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
