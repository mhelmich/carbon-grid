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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;

class UdpGridClient implements Closeable, AutoCloseable {
    private final ChannelFuture channelFuture;
    private final NodeRegistry nodeRegistry;

    UdpGridClient(NodeRegistry nodeRegistry, EventLoopGroup workerGroup, Cache cache) {
        this.nodeRegistry = nodeRegistry;
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
            .channel(NioDatagramChannel.class)
            .option(ChannelOption.SO_BROADCAST, true)
            .handler(new GridClientHandler(cache));
        channelFuture = b.bind(0);
    }

    Future<Void> send(Message request) {
        InetSocketAddress addr = nodeRegistry.lookup(request.node);
        // TODO -- if addr == null ?
        Channel ch = channelFuture.syncUninterruptibly().channel();
        ByteBuf bites = ch.alloc().buffer(request.calcByteSize());
        try {
            try (ByteBufOutputStream out = new ByteBufOutputStream(bites)) {
                request.write(out);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return ch.writeAndFlush(
                    new DatagramPacket(
                            bites,
                            addr
                    )
            );
        } finally {
            bites.release();
        }
    }

    @Override
    public void close() throws IOException {
        channelFuture.channel().close();
    }
}
