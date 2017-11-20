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
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;

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
    private final SequenceGenerator seqGen = new SequenceGenerator();

    private final NonBlockingHashMapLong<LatchAndMessage> messageIdToLatchAndMessage = new NonBlockingHashMapLong<>();
    private final NonBlockingHashMap<Short, TcpGridClient> nodeIdToClient = new NonBlockingHashMap<>();
    private final NonBlockingHashMapLong<PriorityBlockingQueue<Message>> cacheLineIdToBacklog = new NonBlockingHashMapLong<>();

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

    void addPeer(short nodeId, InetSocketAddress addr) {
        logger.info("adding peer {} {}", nodeId, addr);
        nodeIdToClient.put(nodeId, new TcpGridClient(nodeId, addr, workerGroup, internalCache));
    }

    void addPeer(short nodeId, String host, int port) {
        addPeer(nodeId, SocketUtils.socketAddress(host, port));
    }

    Future<Void> send(short toNode, Message msg) throws IOException {
        TcpGridClient client = nodeIdToClient.get(toNode);
        if (client == null) throw new RuntimeException("couldn't find client for node " + toNode + " and can't answer message " + msg.getMessageSequenceNumber());

        CountDownLatchFuture latch = new CountDownLatchFuture();
        // generate message id
        // (relative) uniqueness is enough
        // this is not a sequence number that guarantees full ordering and no gaps
        msg.setMessageSequenceNumber(nextSequenceIdForMessageToSend(toNode));
        // do bookkeeping in order to be able to track the response
        long hash = hashNodeIdMessageSeq(toNode, msg.getMessageSequenceNumber());
        messageIdToLatchAndMessage.put(hash, new LatchAndMessage(latch, msg));
        logger.info("{} sending {} to {}", myNodeId, msg, toNode);
        try {
            client.send(msg);
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
            lAndM.latch.countDown();
        }
        seqGen.markResponseHandled(response.getSender(), response.getMessageSequenceNumber());
    }

    ////////////////////////////////////////////
    ///////////////////////////////////
    /////////////////////////////
    // ASYNC CALLBACK FROM NETTY
    @VisibleForTesting
    void handleMessage(Message msg) {
        logger.info("{} receiving {} from {}", myNodeId, msg, msg.getSender());
        long backlogHash = hashNodeIdCacheLineId(msg.sender, msg.lineId);
        if (isNextInLine(msg)) {
            if (Message.Request.class.isInstance(msg)) {
                handleRequest((Message.Request)msg);
            } else {
                handleResponse((Message.Response)msg);
            }

            logger.info("{} haz handled {}", myNodeId, msg);

            // well it seems I'm the thread keeping everybody waiting
            // then it's only fair to process the rest of the messages
            PriorityBlockingQueue<Message> backlog = getBacklogMap().get(backlogHash);
            if (backlog != null) {
                while (backlog.peek() != null && isNextInLine(backlog.peek())) {
                    Message piggyBackMsg = backlog.poll();
                    if (Message.Request.class.isInstance(msg)) {
                        handleRequest((Message.Request)piggyBackMsg);
                    } else {
                        handleResponse((Message.Response)piggyBackMsg);
                    }
                }
                if (backlog.peek() == null) {
                    getBacklogMap().remove(backlogHash);
                }
            }
        } else {
            // if you're not the next message to process,
            // we throw you into the (sorted) waiting line
            PriorityBlockingQueue<Message> backlog = getBacklogMap().putIfAbsent(
                    backlogHash,
                    new PriorityBlockingQueue<>(17, Comparator.comparingInt(Message::getMessageSequenceNumber)));
            backlog = (backlog == null) ? getBacklogMap().get(backlogHash) : backlog;
            backlog.add(msg);
            logger.info("{} putting message into the backlog {}", myNodeId, msg);
        }
    }

    private boolean isNextInLine(Message msg) {
        return seqGen.isNextFor(msg);
    }

    @VisibleForTesting
    NonBlockingHashMapLong<PriorityBlockingQueue<Message>> getBacklogMap() {
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
            // it's actually crucially important to mark a request handled
            // only after sending the response
            seqGen.markRequestHandled(request.getSender(), request.getMessageSequenceNumber());
        }
    }

    @VisibleForTesting
    void handleResponse(Message.Response response) {
        internalCache.handleResponse(response);
        ackResponse(response);
    }

    @VisibleForTesting
    int nextSequenceIdForMessageToSend(short nodeId) {
        return seqGen.nextSequenceForMessageToSend(nodeId);
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
        final CountDownLatchFuture latch;
        final Message msg;
        LatchAndMessage(CountDownLatchFuture latch, Message msg) {
            this.latch = latch;
            this.msg = msg;
        }

        @Override
        public String toString() {
            return "latch: " + latch + " msg: " + msg;
        }
    }
}
