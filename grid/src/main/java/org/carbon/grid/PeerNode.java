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

import io.netty.channel.EventLoopGroup;
import io.netty.util.internal.SocketUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;

class PeerNode implements Closeable {
    private final short theNodeITalkTo;
    private final InetSocketAddress peerAddr;
    private final TcpGridClient client;

    PeerNode(short theNodeITalkTo, InetSocketAddress peerAddr, EventLoopGroup workerGroup, InternalCache internalCache) {
        this.theNodeITalkTo = theNodeITalkTo;
        this.peerAddr = peerAddr;
        this.client = new TcpGridClient(theNodeITalkTo, peerAddr, workerGroup, internalCache);
    }

    PeerNode(short theNodeITalkTo, String host, int port, EventLoopGroup workerGroup, InternalCache internalCache) {
        this(theNodeITalkTo, SocketUtils.socketAddress(host, port), workerGroup, internalCache);
    }

    Future<Void> send(Message msg) throws IOException {
        return client.send(msg);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public String toString() {
        return "theNodeITalkTo: " + theNodeITalkTo + " addr: " + peerAddr;
    }
}
