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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.ReplayingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

class TcpGridServer extends AbstractServer {
    private final Channel channel;

    TcpGridServer(int port, EventLoopGroup bossGroup, EventLoopGroup workerGroup, InternalCache internalCache, NodeRegistry nodeRegistry) {
        ServerBootstrap b = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ch.pipeline().addLast(
                                        new RequestDecoder(),
                                        new ResponseEncoder(),
                                        new TcpGridServerHandler(internalCache, nodeRegistry)
                                );
                            }
                        }
                );

        channel = b.bind(port).syncUninterruptibly().channel();
    }

    @Override
    public void close() throws IOException {
        channel.close().syncUninterruptibly();
    }

    private static class RequestDecoder extends ReplayingDecoder<Message.Request> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf inBites, List<Object> list) throws Exception {
            Message.MessageType requestMessageType = Message.MessageType.fromByte(inBites.readByte());
            Message.Request request = Message.getRequestForType(requestMessageType);

            try (MessageInput in = new MessageInput(inBites)) {
                request.read(in);
            }

            list.add(request);
        }
    }

    private static class ResponseEncoder extends MessageToByteEncoder<Message.Response> {
        @Override
        protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, Message.Response msg, boolean preferDirect) throws Exception {
            return ctx.alloc().ioBuffer(msg.calcByteSize());
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Message.Response response, ByteBuf outBites) throws Exception {
            try (MessageOutput out = new MessageOutput(outBites)) {
                response.write(out);
            }
        }
    }

    private static class TcpGridServerHandler extends SimpleChannelInboundHandler<Message.Request> {
        private final static Logger logger = LoggerFactory.getLogger(TcpGridServerHandler.class);

        private final InternalCache internalCache;
        private final NodeRegistry nodeRegistry;

        TcpGridServerHandler(InternalCache internalCache, NodeRegistry nodeRegistry) {
            this.internalCache = internalCache;
            this.nodeRegistry = nodeRegistry;
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            logger.info("channel registered: {}", ctx);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.Request request) throws Exception {
            Message.Response response = internalCache.handleRequest(request);
            if (response != null) {
//                PeerNode pn = nodeRegistry.getPeerForNodeId(request.sender);
//                pn.send(response);
                ctx.writeAndFlush(response);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Error in server handler", cause);
        }
    }
}
