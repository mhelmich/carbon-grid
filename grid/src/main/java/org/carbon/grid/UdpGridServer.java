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

import java.io.Closeable;
import java.io.IOException;

class UdpGridServer implements Closeable {
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
                                new GridServerHandler(internalCache)
                        );
                    }
                });
        try {
            // bind and start to accept incoming connections (and wait for the bind to finish)
            channel = b.bind(port).sync().channel();
        } catch (InterruptedException xcp) {
            throw new RuntimeException(xcp);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
