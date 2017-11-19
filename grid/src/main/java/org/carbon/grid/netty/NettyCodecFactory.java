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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.ReplayingDecoder;
import org.carbon.grid.MessageInput;
import org.carbon.grid.MessageOutput;

import java.util.List;

class NettyCodecFactory {
    private static final NettyMessageFactory messageFactory = new NettyMessageFactory();

    private static class MessageDecoder extends ReplayingDecoder<NettyMessage> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf inBites, List<Object> list) throws Exception {
            NettyMessageType requestMessageType = messageFactory.getMessageTypeFromByte(inBites.readByte());
            NettyMessage message = messageFactory.getMessageForType(requestMessageType);

            try (MessageInput in = new MessageInput(inBites)) {
                message.read(in);
            }

            list.add(message);
        }
    }

    MessageDecoder newMessageDecoder() {
        return new MessageDecoder();
    }

    private static class ResponseEncoder extends MessageToByteEncoder<NettyMessage.Response> {
        @Override
        protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, NettyMessage.Response msg, boolean preferDirect) throws Exception {
            return ctx.alloc().ioBuffer(msg.byteSize());
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, NettyMessage.Response response, ByteBuf outBites) throws Exception {
            try (MessageOutput out = new MessageOutput(outBites)) {
                response.write(out);
            }
        }
    }

    ResponseEncoder newResponseEncoder() {
        return new ResponseEncoder();
    }

    private static class RequestEncoder extends MessageToByteEncoder<NettyMessage.Request> {
        @Override
        protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, NettyMessage.Request msg, boolean preferDirect) throws Exception {
            return ctx.alloc().ioBuffer(msg.byteSize());
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, NettyMessage.Request request, ByteBuf outBites) throws Exception {
            try (MessageOutput out = new MessageOutput(outBites)) {
                request.write(out);
            }
        }
    }

    RequestEncoder newRequestEncoder() {
        return new RequestEncoder();
    }
}
