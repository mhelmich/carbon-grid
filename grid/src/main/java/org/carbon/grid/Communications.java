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

import java.util.concurrent.ConcurrentHashMap;

class Communications {
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private final Cache cache;
    private final UdpGridServer udpGridServer;
    private final ConcurrentHashMap<Short, UdpGridClient> nodeIdsToClient = new ConcurrentHashMap<>();

    Communications(int port, Cache cache) {
        this.udpGridServer = new UdpGridServer(port, workerGroup, cache);
        this.cache = cache;
    }

    void addPeer(short nodeId, String host, int port) {
        nodeIdsToClient.put(nodeId, new UdpGridClient(host, port, workerGroup, cache));
    }
}
