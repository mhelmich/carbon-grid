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

import java.util.function.Consumer;

class GridClientHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private final static Logger logger = LoggerFactory.getLogger(GridClientHandler.class);
    private final InternalCache internalCache;
    private final Consumer<Integer> messageAckCallback;
    private final Consumer<Integer> messageResendCallback;

    GridClientHandler(InternalCache internalCache, Consumer<Integer> messageAckCallback, Consumer<Integer> messageResendCallback) {
        this.internalCache = internalCache;
        this.messageAckCallback = messageAckCallback;
        this.messageResendCallback = messageResendCallback;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        ByteBuf inBites = packet.content();
        Message.MessageType messageType = Message.MessageType.fromByte(inBites.readByte());
        Message.Response response = Message.getResponseForType(messageType);

        try (MessageInput in = new MessageInput(inBites)) {
            response.read(in);
        }

        logger.info("Received message type: {} messageId {}", messageType, response.messageId);
        internalCache.handleResponse(response);
        if (Message.MessageType.RESEND.equals(response.type)) {
            messageResendCallback.accept(response.messageId);
        } else {
            messageAckCallback.accept(response.messageId);
        }
    }
}
