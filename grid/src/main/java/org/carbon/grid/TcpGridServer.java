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
import io.netty.handler.codec.ReplayingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Due to fairly special ordering guarantees, a server only receives data.
 * It never uses the socket the inbound connection creates to send data back.
 * In order to send data back, it always uses the client callback mechanism.
 * In a weird sense a server therefore is a read-only component that (by itself)
 * never sends data back.
 */
class TcpGridServer implements Closeable {
    private final Channel channel;

    TcpGridServer(
            int port,
            EventLoopGroup bossGroup,
            EventLoopGroup workerGroup,
            Function<Message.Request, Message.Response> handleRequestCallback,
            Consumer<Message.Response> handleResponseCallback,
            Consumer<Integer> ackMessageCallback,
            BiConsumer<Short, Message> sendMessageCallback
    ) {
        ServerBootstrap b = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ch.pipeline().addLast(
                                        new MessageDecoder(),
                                        new TcpGridServerRequestHandler(sendMessageCallback, handleRequestCallback),
                                        new TcpGridServerResponseHandler(ackMessageCallback, handleResponseCallback)
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

    /**
     * This decoder reads all kinds of messages from the wire -- in particular
     * it has to read requests and responses alike.
     * However, it emits a list of strongly typed messages and this allows handlers
     * to be specific about whether a message is a response or request.
     */
    private static class MessageDecoder extends ReplayingDecoder<Message> {
        private static final Message.DeserializationMessageFactory messageFactory = new Message.DeserializationMessageFactory();
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf inBites, List<Object> list) throws Exception {
            MessageType requestMessageType = MessageType.fromByte(inBites.readByte());
            Message message = messageFactory.createMessageShellForType(requestMessageType);

            try (MessageInput in = new MessageInput(inBites)) {
                message.read(in);
            }

            list.add(message);
        }
    }

    /**
     * This is the netty handler for responses.
     * Beware of the callback that's passed into this class (ackMessageCallBack).
     * The callback is defined in communications unit and handles bookkeeping wrt futures, etc.
     */
    private static class TcpGridServerResponseHandler extends SimpleChannelInboundHandler<Message.Response> {
        private final static Logger logger = LoggerFactory.getLogger(TcpGridServerResponseHandler.class);

        private final Consumer<Message.Response> handleResponseCallback;
        private final Consumer<Integer> ackMessageCallBack;

        TcpGridServerResponseHandler(Consumer<Integer> ackMessageCallback, Consumer<Message.Response> handleResponseCallback) {
            this.ackMessageCallBack = ackMessageCallback;
            this.handleResponseCallback = handleResponseCallback;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.Response response) throws Exception {
            handleResponseCallback.accept(response);
            ackMessageCallBack.accept(response.getMessageSequenceNumber());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Error in server handler", cause);
        }
    }
    /**
     * This is the netty handler for requests.
     * Beware of the callback that's passed into this class (sendMessageCallback).
     * The callback is defined in communications unit and handles the sending of response messages.
     * NEVER USE NETTYS CONTEXT TO REPLY TO THE CLIENT DIRECTLY!
     */
    private static class TcpGridServerRequestHandler extends SimpleChannelInboundHandler<Message.Request> {
        private final static Logger logger = LoggerFactory.getLogger(TcpGridServerRequestHandler.class);

        private final Function<Message.Request, Message.Response> handleRequestCallback;
        private final BiConsumer<Short, Message> sendMessageCallback;

        TcpGridServerRequestHandler(BiConsumer<Short, Message> sendMessageCallback, Function<Message.Request, Message.Response> handleRequestCallback) {
            this.sendMessageCallback = sendMessageCallback;
            this.handleRequestCallback = handleRequestCallback;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.Request request) throws Exception {
            Message.Response response = handleRequestCallback.apply(request);
            // in case we don't have anything to say, let's save us the trouble
            if (response != null) {
                sendMessageCallback.accept(request.getSender(), response);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Error in server handler", cause);
        }
    }
}
