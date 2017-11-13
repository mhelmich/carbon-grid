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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class PeerNode implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(PeerNode.class);

    private final ConcurrentHashMap<Integer, CountDownLatch> messageIdToLatch = new ConcurrentHashMap<>(128, .75f, 64);
    private final LinkedBlockingQueue<Message> messageBackLog = new LinkedBlockingQueue<>();
    private final short nodeId;
    private final InetSocketAddress peerAddr;
    private final UdpGridClient client;

    PeerNode(short nodeId, String host, int port, EventLoopGroup workerGroup, Cache cache) {
        this.nodeId = nodeId;
        this.peerAddr = SocketUtils.socketAddress(host, port);
        this.client = new UdpGridClient(peerAddr, workerGroup, cache, this::clientCallback);
    }

    Future<Void> send(Message msg) throws IOException {
        CountDownLatch latch = new CountDownLatch(1);
        messageIdToLatch.put(msg.messageId, latch);
        Future<Void> nettyFuture = client.send(msg);
        return new CarbonGridFuture(nettyFuture, latch);
    }

    private void clientCallback(Integer messageId) {
        CountDownLatch latch = messageIdToLatch.remove(messageId);
        latch.countDown();
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public String toString() {
        return "nodeId: " + nodeId + " addr: " + peerAddr;
    }

    static class CarbonGridFuture implements Future<Void> {
        private final Future<Void> nettyFuture;
        private final CountDownLatch latch;

        CarbonGridFuture(Future<Void> nettyFuture, CountDownLatch latch) {
            this.nettyFuture = nettyFuture;
            this.latch = latch;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            // counting down the latch and
            // have the client handler do the clean up
            latch.countDown();
            return nettyFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return nettyFuture.isCancelled() && latch.getCount() == 0;
        }

        @Override
        public boolean isDone() {
            return nettyFuture.isDone() && latch.getCount() == 0;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            nettyFuture.get();
            latch.await();
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            nettyFuture.get(timeout, unit);
            latch.await(timeout, unit);
            return null;
        }
    }
}
