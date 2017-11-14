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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

class NodeRegistry implements Closeable {
    private final ConcurrentHashMap<Short, PeerNode> nodeIdToPeer = new ConcurrentHashMap<>(128, .75f, 2);

    private final EventLoopGroup workerGroup;
    private final InternalCache internalCache;

    NodeRegistry(EventLoopGroup workerGroup, InternalCache internalCache) {
        this.workerGroup = workerGroup;
        this.internalCache = internalCache;
    }

    void addPeer(short nodeId, String host, int port) {
        nodeIdToPeer.putIfAbsent(nodeId, new PeerNode(nodeId, host, port, workerGroup, internalCache));
    }

    PeerNode getPeerForNodeId(short nodeId) {
        return nodeIdToPeer.get(nodeId);
    }

    @Override
    public void close() throws IOException {
        for (PeerNode peer : nodeIdToPeer.values()) {
            peer.close();
        }
    }
}
