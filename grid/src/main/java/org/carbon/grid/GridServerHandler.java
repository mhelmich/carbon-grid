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
import java.util.concurrent.ConcurrentHashMap;

class GridServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private final static Logger logger = LoggerFactory.getLogger(GridServerHandler.class);
    private final static ConcurrentHashMap<Short, Integer> nodeToLastAckedMessageId = new ConcurrentHashMap<>(128, .75f, 64);
    private final InternalCache internalCache;

    GridServerHandler(InternalCache internalCache) {
        this.internalCache = internalCache;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        ByteBuf inBites = packet.content();
        Message.MessageType requestMessageType = Message.MessageType.fromByte(inBites.readByte());
        Message.Request request = Message.getRequestForType(requestMessageType);

        try (MessageInput in = new MessageInput(inBites)) {
            request.read(in);
        }

        if (shouldHandleMessage(request)) {
            logger.info("received message type: {} messageId {}", requestMessageType, request.messageId);
            Message.Response response = internalCache.handleRequest(request);

            ByteBuf outBites = ctx.alloc().buffer(response.calcByteSize());
            try (MessageOutput out = new MessageOutput(outBites)) {
                response.write(out);
            }

            ctx.writeAndFlush(new DatagramPacket(outBites, packet.sender()));
            nodeToLastAckedMessageId.put(request.sender, request.messageId);
        } else {
            // drop and resend last acked messageId + 1
            requestResend(ctx, nodeToLastAckedMessageId.get(request.sender) + 1, packet.sender());
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
        Integer lastAckedMessageId = nodeToLastAckedMessageId.get(request.sender);
        return lastAckedMessageId == null || isNext(lastAckedMessageId, request.messageId);
    }

    private boolean isNext(int lastAckedMessageId, int receivedMessageId) {
        if (lastAckedMessageId == Integer.MAX_VALUE) {
            return receivedMessageId == Integer.MIN_VALUE;
        } else {
            return lastAckedMessageId + 1 == receivedMessageId;
        }
    }
}
