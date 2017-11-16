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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import java.util.function.Function;

class GridServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private final static Logger logger = LoggerFactory.getLogger(GridServerHandler.class);
    private final InternalCache internalCache;
    private final Function<Short, Integer> getLastAckedMsgForNode;
    private final BiConsumer<Short, Integer> setLastAckedMsgForNode;

    GridServerHandler(InternalCache internalCache, Function<Short, Integer> getLastAckedMsgForNode, BiConsumer<Short, Integer> setLastAckedMsgForNode) {
        this.internalCache = internalCache;
        this.getLastAckedMsgForNode = getLastAckedMsgForNode;
        this.setLastAckedMsgForNode = setLastAckedMsgForNode;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        ByteBuf inBites = packet.content();
        Message.MessageType requestMessageType = Message.MessageType.fromByte(inBites.readByte());
        Message.Request request = Message.getRequestForType(requestMessageType);

        try (MessageInput in = new MessageInput(inBites)) {
            request.read(in);
        }

        logger.info("received message: {}", request);
        if (shouldHandleMessage(request)) {
            Message.Response response = internalCache.handleRequest(request);

            if (response != null) {
                ByteBuf outBites = ctx.alloc().buffer(response.calcByteSize());
                try (MessageOutput out = new MessageOutput(outBites)) {
                    response.write(out);
                }

                ctx.writeAndFlush(new DatagramPacket(outBites, packet.sender()));
                setLastAckedMsgForNode.accept(request.sender, request.messageId);
            }
        } else {
            logger.info("dropped message with id {} because ids have a gap", request.messageId);
            // drop and resend last acked messageId + 1
            requestResend(ctx, getLastAckedMsgForNode.apply(request.sender) + 1, packet.sender());
        }
    }

    private void requestResend(ChannelHandlerContext ctx, int messageIdToResend, InetSocketAddress sender) throws IOException {
//        ByteBuf outBites = ctx.alloc().buffer(request.calcByteSize());
//        try (MessageOutput out = new MessageOutput(outBites)) {
//            response.write(out);
//        }
//
//        ctx.writeAndFlush(new DatagramPacket(outBites, sender));
    }

    private boolean shouldHandleMessage(Message.Request request) {
        Integer lastAckedMessageId = getLastAckedMsgForNode.apply(request.sender);
        if (lastAckedMessageId == null) {
            return true;
        }

        return isNext(lastAckedMessageId, request.messageId);
    }

    private boolean isNext(int lastAckedMessageId, int receivedMessageId) {
        logger.info("lastAckedMessageId {} receivedMessageId {}", lastAckedMessageId, receivedMessageId);
        if (lastAckedMessageId == Integer.MAX_VALUE) {
            return receivedMessageId == Integer.MIN_VALUE;
        } else {
            return lastAckedMessageId + 1 == receivedMessageId;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Error in server handler", cause);
    }
}
