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

package org.carbon.grid.cache;

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
import java.util.Random;
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
 *
 * This is a good source of info regarding netty as well:
 * https://github.com/netty/netty/wiki
 */
class GridCommunications implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(GridCommunications.class);
    private final static int RESPONSE_SEND_RETRIES = 3;
    private final static int RESPONSE_SEND_RETRIES_WAIT = 100;
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private final NonBlockingHashMapLong<LatchAndMessage> messageIdToLatchAndMessage = new NonBlockingHashMapLong<>();
    // TODO -- the map holding all connections could morph into a guava cache
    // we'd need to keep track of the connection info separately though
    // that could reduce the number of open connections if we don't really need them
    private final NonBlockingHashMap<Short, TcpGridClient> nodeIdToClient = new NonBlockingHashMap<>();
    private final NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>> cacheLineIdToBacklog = new NonBlockingHashMapLong<>();

    private final AtomicInteger sequence = new AtomicInteger(new Random().nextInt());

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
        logger.info("{} [send] {} to {} hash {}", myNodeId, msg, toNode, hash);
        try {
            // do error handling in an async handler (netty-style)
            client.send(msg).addListener(new FailingCompleteListener(hash, messageIdToLatchAndMessage));
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

    // this method is supposed to used when handling a response requires sending another request
    // an example can be handling ownership of cache lines when processing an ownership change
    // still requires getting the cache line in question
    // in those situations receiving a change_ownership response is followed by sending
    // a get or getx request
    void reactToResponse(Message.Response responseIReceived, short toNode, Message.Request requestToSend) {
        // search for the message that I sent myself back in the day to carry over the associated future
        long hash = hashNodeIdMessageSeq(myNodeId, responseIReceived.getMessageSequenceNumber());
        LatchAndMessage lAndM = messageIdToLatchAndMessage.remove(hash);
        if (lAndM == null) {
            logger.warn("{} [reactToResponse] won't react hash {} message {}", myNodeId, hash, responseIReceived);
            return;
        }

        TcpGridClient client = nodeIdToClient.get(toNode);
        if (client == null) {
            RuntimeException rte = new RuntimeException("couldn't find client for node " + toNode + " and can't answer message " + requestToSend.getMessageSequenceNumber());
            lAndM.latch.completeExceptionally(rte);
            throw rte;
        }

        // carry over the original latch since somebody might be waiting on it
        requestToSend.setMessageSequenceNumber(nextSequenceIdForMessageToSend());
        long newHash = hashNodeIdMessageSeq(toNode, requestToSend.getMessageSequenceNumber());
        messageIdToLatchAndMessage.put(newHash, new LatchAndMessage(lAndM.latch, requestToSend));
        logger.info("{} [reactToResponse] {} to {} hash {}", myNodeId, requestToSend, toNode, hash);
        try {
            // do error handling in an async handler (netty-style)
            client.send(requestToSend).addListener(new FailingCompleteListener(newHash, messageIdToLatchAndMessage));
        } catch (IOException xcp) {
            // clean the map up
            // otherwise we will collect a ton of dead wood over time
            messageIdToLatchAndMessage.remove(newHash);
            lAndM.latch.completeExceptionally(xcp);
            throw new RuntimeException(xcp);
        }
    }

    // in some cases (e.g. when a cache line is locked) a handler might decide to toss
    // a message into the backlog
    // this method takes care of that
    // it returns true if the message was indeed added to the backlog
    // it returns false if the message couldn't be added to the backlog
    boolean addToCacheLineBacklog(Message message) {
        long backlogHash = hashNodeIdCacheLineId(message.sender, message.lineId);
        ConcurrentLinkedQueue<Message> backlog = cacheLineIdToBacklog.get(backlogHash);
        if (backlog == null) {
            logger.warn("you asked for message {} to be added to the backlog but there was none", message);
            return false;
        } else {
            backlog.add(message);
            return true;
        }
    }

    ////////////////////////////////////////////
    ///////////////////////////////////
    /////////////////////////////
    // ASYNC CALLBACK FROM NETTY
    @VisibleForTesting
    void handleMessage(Message msg) {
        long backlogHash = hashNodeIdCacheLineId(msg.sender, msg.lineId);
        // this queue acts as token to indicate that a different thread is processing a message for this cache line
        // is also buffers all incoming messages for this cache line in a queue
        ConcurrentLinkedQueue<Message> backlog = cacheLineIdToBacklog.putIfAbsent(backlogHash, new ConcurrentLinkedQueue<>());
        if (backlog == null) {
            logger.info("{} [handleMessage] {} with hash {}", myNodeId, msg, backlogHash);
            // go ahead and process
            innerHandleMessage(msg);

            // after processing this particular message went down well,
            // this thread is somewhat responsible for cleaning out the backlog of messages it caused
            backlog = cacheLineIdToBacklog.get(backlogHash);
            while (backlog.peek() != null) {
                Message backlogMsg = backlog.poll();
                logger.info("{} processing backlog message {} with hash {}", myNodeId, backlogMsg, backlogHash);
                innerHandleMessage(backlogMsg);
                // remove the hash if the backlog is empty
                // this runs atomically
                removeIfEmpty(backlogHash, cacheLineIdToBacklog);
            }

            // remove the hash if the backlog is empty
            // this runs atomically
            removeIfEmpty(backlogHash, cacheLineIdToBacklog);
        } else {
            backlog.add(msg);
            logger.info("{} putting message into the backlog {} with hash {}", myNodeId, msg, backlogHash);
        }
    }

    private void removeIfEmpty(long backlogHash, NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>> hashToBacklog) {
        hashToBacklog.computeIfPresent(backlogHash,
                (key, value) -> value.peek() == null ? null : value
        );
    }

    // generates the hash for the latch map
    @VisibleForTesting
    long hashNodeIdMessageSeq(short nodeId, int messageSequence) {
        return Hashing
                .murmur3_128()
                .newHasher()
                .putShort(nodeId)
                .putInt(messageSequence)
                .hash()
                .asLong();
    }

    // Generates the hash for the cache line backlog.
    // This also controls the concurrency in the cache.
    // If the hash is the cache line id only that means,
    // there can be max one thread per cache line.
    // If the hash is made up of node id and cache line id,
    // there can be one thread per cache line per node.
    @VisibleForTesting
    long hashNodeIdCacheLineId(short nodeId, long cacheLineId) {
        return cacheLineId;
//        return Hashing
//                .murmur3_128()
//                .newHasher()
//                .putShort(nodeId)
//                .putLong(cacheLineId)
//                .hash()
//                .asLong();
    }

    private void innerHandleMessage(Message msg) {
        if (msg != null) {
            if (Message.Request.class.isInstance(msg)) {
                innerHandleRequest((Message.Request)msg);
            } else {
                innerHandleResponse((Message.Response)msg);
            }
        }
    }

    private void innerHandleRequest(Message.Request request) {
        Message.Response response = internalCache.handleRequest(request);
        // in case we don't have anything to say, let's save us the trouble
        if (response != null) {
            sendResponse(request.getSender(), response);
        }
    }

    // doesn't block
    private void sendResponse(short nodeId, Message msg) {
        TcpGridClient client = nodeIdToClient.get(nodeId);
        if (client == null) throw new RuntimeException("couldn't find client for node " + nodeId + " and can't answer message " + msg.getMessageSequenceNumber());
        try {
            client.send(msg).addListener(new RetryingCompleteListener(nodeId, msg, nodeIdToClient));
        } catch (IOException xcp) {
            // the surrounding netty handler catches exceptions
            throw new RuntimeException(xcp);
        }
    }

    private void innerHandleResponse(Message.Response response) {
        internalCache.handleResponse(response);
        ackResponse(response);
    }

    // does bookkeeping around latches and cleaning up
    // the error handling part is done in an async handler (see implementation below)
    private void ackResponse(Message.Response response) {
        long hash = hashNodeIdMessageSeq(response.getSender(), response.getMessageSequenceNumber());
        LatchAndMessage lAndM = messageIdToLatchAndMessage.remove(hash);
        if (lAndM == null) {
            logger.info("{} [ackResponse] failure id {} sender {} hash {} message {}", myNodeId, response.getMessageSequenceNumber(), response.getSender(), hash, response);
        } else {
            logger.info("{} [ackResponse] success id {} sender {} hash {} message {}", myNodeId, response.getMessageSequenceNumber(), response.getSender(), hash, response);
            lAndM.latch.complete(null);
        }
    }

    // this sequence is just a revolving integer that generates a somewhat unique id for a message
    // this is the easiest (and smallest) way I could come up with to get some sort of pseudo id going
    // it's not UUIDs but also smaller :)
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
        messageIdToLatchAndMessage.clear();
        cacheLineIdToBacklog.clear();
    }

    @Override
    public String toString () {
        return "myNodeId: " + myNodeId + " myServerPort: " + myServerPort;
    }

    static class LatchAndMessage {
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

    /**
     * When sending a response to a remote node, it might wait for it and block other requests.
     * In those cases it seems beneficial to retry sending messages.
     */
    private static class RetryingCompleteListener implements ChannelFutureListener {
        private final short nodeId;
        private final Message msg;
        private final NonBlockingHashMap<Short, TcpGridClient> nodeIdToClient;
        private final int count;

        RetryingCompleteListener(short nodeId, Message msg, NonBlockingHashMap<Short, TcpGridClient> nodeIdToClient) {
            this(nodeId, msg, nodeIdToClient, 0);
        }

        RetryingCompleteListener(short nodeId, Message msg, NonBlockingHashMap<Short, TcpGridClient> nodeIdToClient, int count) {
            this.nodeId = nodeId;
            this.msg = msg;
            this.nodeIdToClient = nodeIdToClient;
            this.count = count;
        }

        @Override
        public void operationComplete(ChannelFuture f) throws Exception {
            Throwable cause = f.cause();
            // according to https://netty.io/4.1/api/io/netty/channel/ChannelFuture.html
            // isDone and cause!=null constitute a failure
            if (f.isDone() && cause != null && count <= RESPONSE_SEND_RETRIES) {
                logger.warn("sending {} failed ... retrying", msg, cause);
                // wait a little wait time for retries
                Thread.sleep(RESPONSE_SEND_RETRIES_WAIT * count);
                TcpGridClient client = nodeIdToClient.get(nodeId);
                // yes, that should be interesting if this is null
                if (client == null) throw new RuntimeException("couldn't find client for node " + nodeId + " and can't answer message " + msg.getMessageSequenceNumber());
                try {
                    client.send(msg).addListener(new RetryingCompleteListener(nodeId, msg, nodeIdToClient, count + 1));
                } catch (IOException xcp) {
                    // the surrounding netty handler catches exceptions
                    throw new RuntimeException(xcp);
                }
            }
        }
    }

    /**
     * This listener listens on request sending and fails to the consumer if sending a request fails.
     * Other than sending responses, it doesn't seem right to transparently retry because the ordering
     * of messages might be off.
     */
    private static class FailingCompleteListener implements ChannelFutureListener {
        private final long hash;
        private final NonBlockingHashMapLong<LatchAndMessage> messageIdToLatchAndMessage;

        FailingCompleteListener(long hash, NonBlockingHashMapLong<LatchAndMessage> messageIdToLatchAndMessage) {
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