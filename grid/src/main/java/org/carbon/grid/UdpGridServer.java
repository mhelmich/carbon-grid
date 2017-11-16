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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.io.Closeable;
import java.io.IOException;

class UdpGridServer implements Closeable {
    // this is the server-wide map of the last message ids that have been acked by this server
    // for each node in the cluster this map keeps track of what the next message id to expect is
    private final NonBlockingHashMap<Short, Integer> nodeIdToLastAckedMsgId = new NonBlockingHashMap<>();
    private final Channel channel;

    UdpGridServer(int port, EventLoopGroup workerGroup, InternalCache internalCache) {
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(final NioDatagramChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new UdpGridServerHandler(
                                        internalCache,
                                        nodeId -> getLastAckedMsgIdCallback(nodeId),
                                        (nodeId, msgId) -> setLastAckedMsgIdCallback(nodeId, msgId)
                                )
                        );
                    }
                });
        try {
            // bind and start to accept incoming connections
            // (and wait for the bind to finish)
            channel = b.bind(port).sync().channel();
        } catch (InterruptedException xcp) {
            throw new RuntimeException(xcp);
        }
    }

    private Integer getLastAckedMsgIdCallback(short nodeId) {
        return nodeIdToLastAckedMsgId.get(nodeId);
    }

    private void setLastAckedMsgIdCallback(short nodeId, int msgId) {
        nodeIdToLastAckedMsgId.put(nodeId, msgId);
    }

    @Override
    public String toString() {
        return "addr: " + channel.localAddress() + " nodeIdToLastAckedMsgId: " + nodeIdToLastAckedMsgId;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
