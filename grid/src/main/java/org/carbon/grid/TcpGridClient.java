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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.ReplayingDecoder;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class TcpGridClient extends AbstractClient {
    private final ChannelFuture channelFuture;
    private final NonBlockingHashMap<Integer, LatchAndMessage> messageIdToLatchAndMessage = new NonBlockingHashMap<>();
    private final AtomicInteger messageIdGenerator = new AtomicInteger(Integer.MIN_VALUE);

    TcpGridClient(short theNodeITalkTo, InetSocketAddress addr, EventLoopGroup workerGroup, InternalCache internalCache) {
        super(theNodeITalkTo, addr);
        channelFuture = createBootstrap(workerGroup, internalCache).connect(addr);
    }

    @Override
    CountDownLatchFuture send(Message msg) throws IOException {
        CountDownLatchFuture latch = new CountDownLatchFuture();
        msg.messageId = generateNextMessageId();
        messageIdToLatchAndMessage.put(msg.messageId, new LatchAndMessage(latch, msg));
        innerSend(msg);
        return latch;
    }

    @Override
    protected Bootstrap createBootstrap(EventLoopGroup workerGroup, InternalCache internalCache) {
        return new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                             @Override
                             public void initChannel(SocketChannel ch) throws Exception {
                                 ch.pipeline().addLast(
                                         new ResponseDecoder(),
                                         new RequestEncoder(),
                                         new TcpGridClientHandler(internalCache, msgId -> ackResponseCallback(msgId))
                                 );
                             }
                         }
                );
    }

    private int generateNextMessageId() {
        int newId = messageIdGenerator.incrementAndGet();
        if (newId == Integer.MAX_VALUE) {
            // the only way the value cannot be "current" is when a different
            // thread swept in and "incremented" the integer
            // and since the only way the int can be incremented from here
            // is flowing over, the result is irrelevant to us
            messageIdGenerator.compareAndSet(newId, Integer.MIN_VALUE);
        }

        // this also means newId is never MIN_VALUE
        return newId;
    }

    void ackResponseCallback(int messageId) {
        LatchAndMessage lAndM = messageIdToLatchAndMessage.remove(messageId);
        lAndM.latch.countDown();
    }

    private ChannelFuture innerSend(Message msg) throws IOException {
        return channelFuture.syncUninterruptibly().channel().writeAndFlush(msg);
    }

    @Override
    public void close() throws IOException {
        channelFuture.channel().close();
    }

    private static class ResponseDecoder extends ReplayingDecoder<Message.Response> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf inBites, List<Object> list) throws Exception {
            Message.MessageType responseMessageType = Message.MessageType.fromByte(inBites.readByte());
            Message.Response response = Message.getResponseForType(responseMessageType);

            try (MessageInput in = new MessageInput(inBites)) {
                response.read(in);
            }

            list.add(response);
        }
    }

    private static class RequestEncoder extends MessageToByteEncoder<Message.Request> {
        @Override
        protected void encode(ChannelHandlerContext ctx, Message.Request request, ByteBuf outBites) throws Exception {
            try (MessageOutput out = new MessageOutput(outBites)) {
                request.write(out);
            }
        }
    }

    private static class TcpGridClientHandler extends SimpleChannelInboundHandler<Message.Response> {
        private final static Logger logger = LoggerFactory.getLogger(TcpGridClientHandler.class);

        private final InternalCache internalCache;
        private final Consumer<Integer> messageAckCallback;

        TcpGridClientHandler(InternalCache internalCache, Consumer<Integer> messageAckCallback) {
            this.internalCache = internalCache;
            this.messageAckCallback = messageAckCallback;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.Response response) throws Exception {
            logger.info("received message: {}", response);
            internalCache.handleResponse(response);
            messageAckCallback.accept(response.messageId);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Error in the client handler", cause);
        }
    }

    private static class LatchAndMessage {
        final CountDownLatchFuture latch;
        final Message msg;
        LatchAndMessage(CountDownLatchFuture latch, Message msg) {
            this.latch = latch;
            this.msg = msg;
        }

        @Override
        public String toString() {
            return "latch: " + latch + " msg: " + msg;
        }
    }
}
