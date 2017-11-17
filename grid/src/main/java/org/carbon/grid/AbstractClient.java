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
import io.netty.channel.EventLoopGroup;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;

abstract class AbstractClient implements Closeable {
    private final short theNodeITalkTo;
    final InetSocketAddress addr;

    AbstractClient(short theNodeITalkTo, InetSocketAddress addr) {
        this.theNodeITalkTo = theNodeITalkTo;
        this.addr = addr;
    }

    @Override
    public String toString() {
        return "theNodeITalkTo: " + theNodeITalkTo + " addr: " + addr;
    }

    abstract Future<Void> send(Message msg) throws IOException;
    abstract protected Bootstrap createBootstrap(EventLoopGroup workerGroup, InternalCache internalCache);
}
