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

    private final NonBlockingHashMap<Integer, LatchAndMessage> messageIdToLatchAndMessage = new NonBlockingHashMap<>();
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
        msg.setMessageSequenceNumber(nextSequenceId(msg.getSender()));
        // do bookkeeping in order to be able to track the response
        messageIdToLatchAndMessage.put(msg.getMessageSequenceNumber(), new LatchAndMessage(latch, msg));
        try {
            client.send(msg);
        } catch (IOException xcp) {
            // clean up in the map
            // otherwise we will collect a ton of dead wood over time
            messageIdToLatchAndMessage.remove(msg.getMessageSequenceNumber());
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

    private void ackResponse(int messageId) {
        LatchAndMessage lAndM = messageIdToLatchAndMessage.remove(messageId);
        if (lAndM == null) {
            logger.info("seeing message id {} again", messageId);
        } else {
            lAndM.latch.countDown();
        }
    }

    ////////////////////////////////////////////
    ///////////////////////////////////
    /////////////////////////////
    // ASYNC CALLBACK FROM NETTY
    private void handleMessage(Message msg) {
        if (isNextInLine(msg.getSender(), msg.getMessageSequenceNumber())) {
            if (Message.Request.class.isInstance(msg)) {
                handleRequest((Message.Request)msg);
            } else {
                handleResponse((Message.Response)msg);
            }

            PriorityBlockingQueue<Message> backlog = cacheLineIdToBacklog.get(msg.lineId);
            while (backlog != null
                    && backlog.peek() != null) {
                Message piggyBackMsg = backlog.poll();
                if (Message.Request.class.isInstance(msg)) {
                    handleRequest((Message.Request)piggyBackMsg);
                } else {
                    handleResponse((Message.Response)piggyBackMsg);
                }
            }
        } else {
            cacheLineIdToBacklog.putIfAbsent(msg.lineId, new PriorityBlockingQueue<>(17, Comparator.comparingInt(Message::getMessageSequenceNumber)));
            cacheLineIdToBacklog.get(msg.lineId).add(msg);
        }
    }

    ////////////////////////////////////////////
    ///////////////////////////////////
    /////////////////////////////
    // ASYNC CALLBACK FROM NETTY
    private void handleRequest(Message.Request request) {
        Message.Response response = internalCache.handleRequest(request);
        // in case we don't have anything to say, let's save us the trouble
        if (response != null) {
            sendResponse(request.getSender(), response);
        }
        if (isNextInLine(request.getSender(), request.getMessageSequenceNumber())) {

            while (cacheLineIdToBacklog.get(request.lineId) != null
                    && cacheLineIdToBacklog.get(request.lineId).peek() != null) {
                Message piggyBackRequest = cacheLineIdToBacklog.get(request.lineId).poll();
                handleMessage(piggyBackRequest);
            }
        } else {
            cacheLineIdToBacklog.putIfAbsent(request.lineId, new PriorityBlockingQueue<>(17, Comparator.comparingInt(Message::getMessageSequenceNumber)));
            cacheLineIdToBacklog.get(request.lineId).add(request);
        }
    }

    ////////////////////////////////////////////
    ///////////////////////////////////
    /////////////////////////////
    // ASYNC CALLBACK FROM NETTY
    private void handleResponse(Message.Response response) {
        internalCache.handleResponse(response);
        ackResponse(response.getMessageSequenceNumber());
    }

    private boolean isNextInLine(short nodeId, int sequenceNumber) {
        return seqGen.isNextFor(nodeId, sequenceNumber);
    }

    private int nextSequenceId(short nodeId) {
        return seqGen.next(nodeId);
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
