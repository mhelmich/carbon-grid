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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;

class GridServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private final Cache cache;

    GridServerHandler(Cache cache) {
        this.cache = cache;
    }

//    @Override
//    public void channelRead(ChannelHandlerContext ctx, DatagramPacket o) {
//        Message request = (Message)o;
//        Message response = cache.handleMessage(request);
//        ctx.writeAndFlush(response);
//    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        ByteBuf in = packet.content();
        Message.MessageType messageType = Message.MessageType.fromByte(in.readByte());
        System.err.println(packet.content().toString(CharsetUtil.UTF_8));
        ctx.writeAndFlush(new DatagramPacket(Unpooled.copiedBuffer(packet.content()), packet.sender()));
    }

//    @Override
//    public void channelReadComplete(ChannelHandlerContext ctx) {
//        ctx.flush();
//    }
}
