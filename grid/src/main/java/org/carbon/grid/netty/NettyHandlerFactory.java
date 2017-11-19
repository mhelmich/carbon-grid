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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

class NettyHandlerFactory {
    private final static NettyMessageFactory messageFactory = new NettyMessageFactory();

    private final NettyCacheImpl cacheLine;

    NettyHandlerFactory(NettyCacheImpl cacheLine) {
        this.cacheLine = cacheLine;
    }

    static class NettyRequestHandler extends SimpleChannelInboundHandler<NettyMessage.Request> {
        private NettyRequestHandler() {}

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NettyMessage.Request request) throws Exception {

        }
    }

    NettyRequestHandler newNettyRequestHandler() {
        return new NettyRequestHandler();
    }

    static class NettyResponseHandler extends SimpleChannelInboundHandler<NettyMessage.Response> {
        private NettyResponseHandler() {}

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NettyMessage.Response response) throws Exception {

        }
    }

    NettyResponseHandler newNettyResponseHandler() {
        return new NettyResponseHandler();
    }
}
