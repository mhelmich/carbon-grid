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
import io.netty.util.internal.SocketUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

class GridCommunications implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(GridCommunications.class);
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    final short myNodeId;
    private final int myServerPort;
    private final NodeRegistry nodeRegistry;
    private final TcpGridServer tcpGridServer;

    GridCommunications(int myNodeId, InternalCache internalCache) {
        this(myNodeId, 9876, internalCache);
    }

    GridCommunications(int myNodeId, int port, InternalCache internalCache) {
        this((short)myNodeId, port, internalCache);
    }

    private GridCommunications(short myNodeId, int port, InternalCache internalCache) {
        this.myNodeId = myNodeId;
        this.myServerPort = port;
        this.nodeRegistry = new NodeRegistry(workerGroup, internalCache);
        this.tcpGridServer = new TcpGridServer(port, bossGroup, workerGroup, internalCache, nodeRegistry);
    }

    void addPeer(short nodeId, InetSocketAddress addr) {
        logger.info("adding peer {} {}", nodeId, addr);
        nodeRegistry.addPeer(nodeId, addr);
    }

    void addPeer(short nodeId, String host, int port) {
        addPeer(nodeId, SocketUtils.socketAddress(host, port));
    }

    Future<Void> send(short toNode, Message msg) throws IOException {
        PeerNode peer = nodeRegistry.getPeerForNodeId(toNode);
        return peer.send(msg);
    }

    Future<Void> send(Message msg) throws IOException {
        PeerNode peer = nodeRegistry.getPeerForNodeId(msg.sender);
        return peer.send(msg);
    }

    Future<Void> broadcast(Message msg) throws IOException {
        List<Future<Void>> futures = new LinkedList<>();
        for (PeerNode pn : nodeRegistry.getAllPeers()) {
            futures.add(pn.send(msg));
        }

        return new CompositeFuture(futures);
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException xcp) {
            logger.error("Error while closing", xcp);
        }
    }

    @Override
    public void close() throws IOException {
        closeQuietly(nodeRegistry);
        closeQuietly(tcpGridServer);
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    @Override
    public String toString () {
        return "myNodeId: " + myNodeId + " myServerPort: " + myServerPort;
    }
}
