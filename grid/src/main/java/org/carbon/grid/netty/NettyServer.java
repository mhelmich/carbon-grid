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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.Closeable;
import java.io.IOException;

class NettyServer implements Closeable {
    private final Channel channel;

    NettyServer(int port, EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
        ServerBootstrap b = createBootstrap(bossGroup, workerGroup);
        channel = b.bind(port).syncUninterruptibly().channel();
    }

    private ServerBootstrap createBootstrap(EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
        NettyCodecFactory codecFactory = new NettyCodecFactory();
        NettyHandlerFactory handlerFactory = new NettyHandlerFactory();
        return new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ch.pipeline().addLast(
                                        codecFactory.newMessageDecoder(),
                                        handlerFactory.newNettyRequestHandler(),
                                        handlerFactory.newNettyResponseHandler()
                                );
                            }
                        }
                );
    }

    @Override
    public void close() throws IOException {
        channel.close().syncUninterruptibly();
    }
}
