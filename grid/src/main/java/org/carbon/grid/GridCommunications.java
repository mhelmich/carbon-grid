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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.SocketUtils;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class handles message passing between nodes.
 * It's crucially important that messages are received and processed
 * in the order they are sent! I can't stress this enough.
 * The message that is being sent first needs to be processed first!
 *
 * Therefore, this implements no regular client - server behavior.
 * In our scenario, there can be only one pipe from a node to another node.
 * That means each node *only* receives messages in its server component and
 * *only* sends messages with its client component.
 * The server never (!!!) replies in the worker socket that is created with each
 * incoming connection. It always always always replies using the dedicated client
 * for the node it wants to talk to.
 */
class GridCommunications implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(GridCommunications.class);
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private final NonBlockingHashMapLong<LatchAndMessage> messageIdToLatchAndMessage = new NonBlockingHashMapLong<>();
    private final NonBlockingHashMap<Short, TcpGridClient> nodeIdToClient = new NonBlockingHashMap<>();
    private final NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>> cacheLineIdToBacklog = new NonBlockingHashMapLong<>();

    private final AtomicInteger sequence = new AtomicInteger();

    final short myNodeId;
    private final int myServerPort;
    private final TcpGridServer tcpGridServer;
    private final InternalCache internalCache;

    GridCommunications(int myNodeId, int port, InternalCache internalCache) {
        this((short)myNodeId, port, internalCache);
    }

    private GridCommunications(short myNodeId, int port, InternalCache internalCache) {
        this.myNodeId = myNodeId;
        this.myServerPort = port;
        this.tcpGridServer = new TcpGridServer(
                port,
                bossGroup,
                workerGroup,
                this::handleMessage
        );
        this.internalCache = internalCache;
    }

    private void addPeer(short nodeId, InetSocketAddress addr) {
        logger.info("adding peer {} {}", nodeId, addr);
        nodeIdToClient.put(nodeId, new TcpGridClient(nodeId, addr, workerGroup, internalCache));
    }

    void addPeer(short nodeId, String host, int port) {
        addPeer(nodeId, SocketUtils.socketAddress(host, port));
    }

    Future<Void> send(short toNode, Message msg) throws IOException {
        TcpGridClient client = nodeIdToClient.get(toNode);
        if (client == null) throw new RuntimeException("couldn't find client for node " + toNode + " and can't answer message " + msg.getMessageSequenceNumber());

        CompletableFuture<Void> latch = new CompletableFuture<>();
        // generate message id
        // (relative) uniqueness is enough
        // this is not a sequence number that guarantees full ordering and no gaps
        msg.setMessageSequenceNumber(nextSequenceIdForMessageToSend());
        // do bookkeeping in order to be able to track the response
        long hash = hashNodeIdMessageSeq(toNode, msg.getMessageSequenceNumber());
        messageIdToLatchAndMessage.put(hash, new LatchAndMessage(latch, msg));
        logger.info("{} sending {} to {}", myNodeId, msg, toNode);
        try {
            client.send(msg).addListener(new SendCompleteListener(hash, messageIdToLatchAndMessage));
        } catch (IOException xcp) {
            // clean the map up
            // otherwise we will collect a ton of dead wood over time
            messageIdToLatchAndMessage.remove(hash);
            throw xcp;
        }
        return latch;
    }

    // a broadcast today is just pinging every node
    // in the cluster in a loop
    Future<Void> broadcast(Message msg) throws IOException {
        List<Future<Void>> futures = new LinkedList<>();
        for (Short nodeId : nodeIdToClient.keySet()) {
            futures.add(send(nodeId, msg.copy()));
        }
        return new CompositeFuture(futures);
    }

    // doesn't block
    private void sendResponse(short nodeId, Message msg) {
        TcpGridClient client = nodeIdToClient.get(nodeId);
        if (client == null) throw new RuntimeException("couldn't find client for node " + nodeId + " and can't answer message " + msg.getMessageSequenceNumber());
        try {
            client.send(msg);
        } catch (IOException xcp) {
            // the surrounding netty handler catches exceptions
            throw new RuntimeException(xcp);
        }
    }

    private void ackResponse(Message.Response response) {
        long hash = hashNodeIdMessageSeq(response.getSender(), response.getMessageSequenceNumber());
        LatchAndMessage lAndM = messageIdToLatchAndMessage.remove(hash);
        if (lAndM == null) {
            logger.info("{} is seeing message id {} from {} again", myNodeId, response.getMessageSequenceNumber(), response.getSender());
        } else {
            lAndM.latch.complete(null);
        }
    }

    private void removeIfEmpty(long backlogHash, NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>> hashToBacklog) {
        hashToBacklog.computeIfPresent(backlogHash,
                (key, value) -> value.peek() == null ? null : value
        );
    }

    ////////////////////////////////////////////
    ///////////////////////////////////
    /////////////////////////////
    // ASYNC CALLBACK FROM NETTY
    @VisibleForTesting
    void handleMessage(Message msg) {
        NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>> hashToBacklog = getBacklogMap();
        long backlogHash = hashNodeIdCacheLineId(msg.sender, msg.lineId);
        ConcurrentLinkedQueue<Message> backlog = hashToBacklog.putIfAbsent(backlogHash, new ConcurrentLinkedQueue<>());
        if (backlog == null) {
            logger.info("{} processing message {} with hash {}", myNodeId, msg, backlogHash);
            // go ahead and process
            if (Message.Request.class.isInstance(msg)) {
                handleRequest((Message.Request)msg);
            } else {
                handleResponse((Message.Response)msg);
            }

            backlog = hashToBacklog.get(backlogHash);
            while (backlog.peek() != null) {
                Message backlogMsg = backlog.poll();
                logger.info("{} processing message {} with hash {}", myNodeId, backlogMsg, backlogHash);
                if (Message.Request.class.isInstance(backlogMsg)) {
                    handleRequest((Message.Request)backlogMsg);
                } else {
                    handleResponse((Message.Response)backlogMsg);
                }
                // remove the hash if the backlog is empty
                removeIfEmpty(backlogHash, hashToBacklog);
            }

            // remove the hash if the backlog is empty
            removeIfEmpty(backlogHash, hashToBacklog);
        } else {
            backlog.add(msg);
            logger.info("{} putting message into the backlog {} with hash {}", myNodeId, msg, backlogHash);
        }
    }

    @VisibleForTesting
    NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>> getBacklogMap() {
        return cacheLineIdToBacklog;
    }

    private long hashNodeIdMessageSeq(short nodeId, int messageSequence) {
        return Hashing
                .murmur3_128()
                .newHasher()
                .putShort(nodeId)
                .putInt(messageSequence)
                .hash()
                .asLong();
    }

    private long hashNodeIdCacheLineId(short nodeId, long cacheLineId) {
        return Hashing
                .murmur3_128()
                .newHasher()
                .putShort(nodeId)
                .putLong(cacheLineId)
                .hash()
                .asLong();
    }

    private void handleRequest(Message.Request request) {
        Message.Response response = internalCache.handleRequest(request);
        // in case we don't have anything to say, let's save us the trouble
        if (response != null) {
            sendResponse(request.getSender(), response);
        }
    }

    private void handleResponse(Message.Response response) {
        internalCache.handleResponse(response);
        ackResponse(response);
    }

    private int nextSequenceIdForMessageToSend() {
        int seq;
        int newSeq;
        do {
            seq = sequence.get();
            newSeq = (seq == Integer.MAX_VALUE) ? Integer.MIN_VALUE : seq + 1;
        } while(!sequence.compareAndSet(seq, newSeq));
        return newSeq;
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException xcp) {
            logger.error("Error while closing", xcp);
        }
    }

    @Override
    public void close() throws IOException {
        for (TcpGridClient client : nodeIdToClient.values()) {
            closeQuietly(client);
        }
        closeQuietly(tcpGridServer);
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    @Override
    public String toString () {
        return "myNodeId: " + myNodeId + " myServerPort: " + myServerPort;
    }

    private static class LatchAndMessage {
        final CompletableFuture<Void> latch;
        final Message msg;
        LatchAndMessage(CompletableFuture<Void> latch, Message msg) {
            this.latch = latch;
            this.msg = msg;
        }

        @Override
        public String toString() {
            return "latch: " + latch + " msg: " + msg;
        }
    }

    private static class SendCompleteListener implements ChannelFutureListener {
        private final long hash;
        private final NonBlockingHashMapLong<LatchAndMessage> messageIdToLatchAndMessage;

        SendCompleteListener(long hash, NonBlockingHashMapLong<LatchAndMessage> messageIdToLatchAndMessage) {
            this.hash = hash;
            this.messageIdToLatchAndMessage = messageIdToLatchAndMessage;
        }

        @Override
        public void operationComplete(ChannelFuture f) throws Exception {
            // for now we just log an error and move on with our lives
            // you might conceive of retry logic here
            // but then we need to take care of ordering on the receiving end which we don't do today
            // best to throw up an error and leave it to the client to decide what to do
            Throwable t = f.cause();
            if (f.isDone() && t != null) {
                // log out errors first just in case
                logger.error("", t);
                LatchAndMessage lam = messageIdToLatchAndMessage.remove(hash);
                // if lam is ever null for some reason ... there's a big bug in our bookkeeping somewhere
                lam.latch.completeExceptionally(t);
            }
        }
    }
}
