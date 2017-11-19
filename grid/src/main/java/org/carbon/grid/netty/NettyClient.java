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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

class NettyClient implements Closeable {
    private final short theNodeITalkTo;
    private final InetSocketAddress addr;
    private final ChannelFuture channelFuture;
    private Channel channel;

    NettyClient(short theNodeITalkTo, InetSocketAddress addr, EventLoopGroup workerGroup) {
        this.theNodeITalkTo = theNodeITalkTo;
        this.addr = addr;
        channelFuture = createBootstrap(workerGroup).connect(addr);
    }

    private Bootstrap createBootstrap(EventLoopGroup workerGroup) {
        NettyCodecFactory codecFactory = new NettyCodecFactory();
        return new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                             @Override
                             public void initChannel(SocketChannel ch) throws Exception {
                                 ch.pipeline().addLast(
                                         codecFactory.newRequestEncoder(),
                                         codecFactory.newResponseEncoder()
                                 );
                             }
                         }
                );
    }

    ChannelFuture send(NettyMessage msg) throws IOException {
        if (channel == null) {
            synchronized (channelFuture) {
                if (channel == null) {
                    channel = channelFuture.syncUninterruptibly().channel();
                }
            }
        }
        return channel.writeAndFlush(msg);
    }

    @Override
    public String toString() {
        return "theNodeITalkTo: " + theNodeITalkTo + " addr: " + addr;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
