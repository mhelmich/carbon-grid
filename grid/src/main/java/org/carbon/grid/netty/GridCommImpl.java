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

package org.carbon.grid.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.SocketUtils;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;

class GridCommImpl implements GridComm {
    private final static Logger logger = LoggerFactory.getLogger(GridCommImpl.class);
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private final NonBlockingHashMap<Integer, LatchAndMessage> messageIdToLatchAndMessage = new NonBlockingHashMap<>();
    private final NonBlockingHashMap<Short, NettyClient> nodeIdToClient = new NonBlockingHashMap<>();

    final short myNodeId;
    private final int myServerPort;
    private final NettyServer server;

    GridCommImpl(int myNodeId, int port, NettyCacheImpl cache) {
        this.myNodeId = (short)myNodeId;
        this.myServerPort = port;
        this.server = new NettyServer(port, bossGroup, workerGroup, cache);
    }

    public void addPeer(short nodeId, InetSocketAddress addr) {
        nodeIdToClient.put(nodeId, new NettyClient(nodeId, addr, workerGroup));
    }

    public void addPeer(short nodeId, String host, int port) {
        addPeer(nodeId, SocketUtils.socketAddress(host, port));
    }

    public Future<Void> send(short toNode, NettyMessage msg) throws IOException {
        return null;
    }

    public Future<Void> broadcast(NettyMessage msg) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {
        for (NettyClient client : nodeIdToClient.values()) {
            closeQuietly(client);
        }
        closeQuietly(server);
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
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
    public String toString () {
        return "myNodeId: " + myNodeId + " myServerPort: " + myServerPort;
    }

    private static class LatchAndMessage {
//        final CountDownLatchFuture latch;
//        final NettyMessage msg;
//        LatchAndMessage(CountDownLatchFuture latch, NettyMessage msg) {
//            this.latch = latch;
//            this.msg = msg;
//        }

//        @Override
//        public String toString() {
//            return "latch: " + latch + " msg: " + msg;
//        }
    }
}
