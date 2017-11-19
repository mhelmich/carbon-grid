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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Due to fairly special ordering guarantees, a client never actually receives data.
 * It only sends data to another node. Therefore, it doesn't need decoders
 * or even an inbound handler...because there simply will be no inbound messages.
 * A client is practically a write-only construct.
 */
class TcpGridClient implements Closeable {
    private final short theNodeITalkTo;
    private final InetSocketAddress addr;
    private final ChannelFuture channelFuture;
    private Channel channel;

    TcpGridClient(short theNodeITalkTo, InetSocketAddress addr, EventLoopGroup workerGroup, InternalCache internalCache) {
        this.theNodeITalkTo = theNodeITalkTo;
        this.addr = addr;
        channelFuture = createBootstrap(workerGroup, internalCache).connect(addr);
    }

    ChannelFuture send(Message msg) throws IOException {
        if (channel == null) {
            synchronized (channelFuture) {
                if (channel == null) {
                    channel = channelFuture.syncUninterruptibly().channel();
                }
            }
        }
        return channel.writeAndFlush(msg);
    }

    private Bootstrap createBootstrap(EventLoopGroup workerGroup, InternalCache internalCache) {
        return new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                             @Override
                             public void initChannel(SocketChannel ch) throws Exception {
                                 ch.pipeline().addLast(
                                         new ResponseEncoder(),
                                         new RequestEncoder()
                                 );
                             }
                         }
                );
    }

    @Override
    public void close() throws IOException {
        channelFuture.channel().close();
    }

    @Override
    public String toString() {
        return "theNodeITalkTo: " + theNodeITalkTo + " addr: " + addr;
    }

    private static class ResponseEncoder extends MessageToByteEncoder<Message.Response> {
        @Override
        protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, Message.Response msg, boolean preferDirect) throws Exception {
            return ctx.alloc().ioBuffer(msg.calcMessagesByteSize());
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Message.Response response, ByteBuf outBites) throws Exception {
            try (MessageOutput out = new MessageOutput(outBites)) {
                response.write(out);
            }
        }
    }

    private static class RequestEncoder extends MessageToByteEncoder<Message.Request> {
        @Override
        protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, Message.Request msg, boolean preferDirect) throws Exception {
            return ctx.alloc().ioBuffer(msg.calcMessagesByteSize());
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Message.Request request, ByteBuf outBites) throws Exception {
            try (MessageOutput out = new MessageOutput(outBites)) {
                request.write(out);
            }
        }
    }
}
