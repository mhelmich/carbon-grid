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
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is the main class.
 * It implements the MEOSI protocol and makes sure all cache lines are always in
 * a consistent state.
 *
 * Design ideas:
 * http://blog.paralleluniverse.co/2012/07/26/galaxy-internals-part-1/
 * http://blog.paralleluniverse.co/2012/08/03/galaxy-internals-part-2/
 * http://blog.paralleluniverse.co/2012/08/09/galaxy-internals-part-3/
 *
 * http://developer.amd.com/wordpress/media/2012/10/24593_APM_v21.pdf
 *
 * ToDo List:
 * -  XXX basic message passing and marshalling logic
 * - build netty encoders and decoders
 * - MOESI coherence protocol implementation
 * - clustering & synchronization (cache line ids, cluster members, ...)
 * - fail over / topology changes / node failures
 * - slab allocation and page management
 * - resilient memory across nodes
 *
 */
class InternalCacheImpl implements InternalCache, Closeable {
    private final static Logger logger = LoggerFactory.getLogger(InternalCacheImpl.class);
    // pseudo unique cache line id generator
    // TODO -- the ids need to be globally unique
    private static Random random = new Random();
    private static Set<Long> usedIds = new HashSet<>();

    // the cache lines I own
    private final NonBlockingHashMapLong<CacheLine> owned = new NonBlockingHashMapLong<>();
    // the cache lines I'm sharing and somebody else owns
    private final NonBlockingHashMapLong<CacheLine> shared = new NonBlockingHashMapLong<>();

    final GridCommunications comms;
    final short myNodeId;

    private final static int TIMEOUT_SECS = 555;

    InternalCacheImpl(int myNodeId, int myPort) {
        this.myNodeId = (short) myNodeId;
        comms = new GridCommunications(myNodeId, myPort, this);
    }

    ///////////////////////////////////////
    ////////////////////////////////
    //////////////////////////
    // ENTRY POINT OF ASYNC RESPONSE PROCESSING
    // INSIDE OF NETTY WORKER THREADS
    // KEEP THIS QUICK AND NIMBLE
    @Override
    public void handleResponse(Message.Response response) {
        switch (response.type) {
            case ACK:
                handleACK((Message.ACK)response);
                return;
            case PUT:
                handlePUT((Message.PUT)response);
                return;
            case PUTX:
                handlePUTX((Message.PUTX)response);
                return;
            case INVACK:
                handleINVACK((Message.INVACK)response);
                return;
            case OWNER_CHANGED:
                handleOWNER_CHANGED((Message.OWNER_CHANGED)response);
                return;
            default:
                throw new RuntimeException("Unknown type " + response.type);
        }
    }

    ///////////////////////////////////////
    ////////////////////////////////
    //////////////////////////
    // ENTRY POINT OF ASYNC REQUEST PROCESSING
    // INSIDE OF NETTY WORKER THREADS
    // KEEP THIS QUICK AND NIMBLE
    @Override
    public Message.Response handleRequest(Message.Request request) {
        switch (request.type) {
            case GET:
                return handleGET((Message.GET)request);
            case GETX:
                return handleGETX((Message.GETX)request);
            case INV:
                return handleINV((Message.INV)request);
            default:
                throw new RuntimeException("Unknown type " + request.type);
        }
    }

    private Message.Response handleGETX(Message.GETX getx) {
        logger.info("cache handler {} getx: {}", this, getx);
        CacheLine line = owned.remove(getx.lineId);
        if (line != null) {
            // I'm the owner
            // let's demote the cache line to invalid,
            // pack it up and ship it to the requester
            Set<Short> sharersToSend = line.getSharers();

            // change local status and status of cache line
            line.setState(CacheLineState.INVALID);
            line.setOwner(getx.getSender());
            line.clearSharers();
            shared.put(getx.lineId, line);

            // massage sharers list
            sharersToSend.remove(getx.getSender());
            sharersToSend.remove(myNodeId);

            // compose message
            return new Message.PUTX(
                    getx.getMessageSequenceNumber(),
                    myNodeId,
                    line.getId(),
                    line.getVersion(),
                    sharersToSend,
                    line.resetReaderAndGetReadOnlyData()
            );
        } else {
            // I'm not the owner
            // let's see whether I find the line
            // in the sharer map and see who the new owner is
            line = shared.get(getx.lineId);
            if (line == null) {
                return new Message.ACK(getx.getMessageSequenceNumber(), myNodeId, getx.lineId);
            } else {
                return new Message.OWNER_CHANGED(
                        getx.getMessageSequenceNumber(),
                        myNodeId,
                        line.getId(),
                        line.getOwner(),
                        MessageType.GETX
                );
            }
        }
    }

    private Message.Response handleGET(Message.GET get) {
        logger.info("cache handler {} get: {}", this, get);
        CacheLine line = owned.get(get.lineId);
        if (line != null) {
            if (CacheLineState.EXCLUSIVE.equals(line.getState())) {
                line.setState(CacheLineState.OWNED);
            }
            line.addSharer(get.getSender());
            return new Message.PUT(
                    get.getMessageSequenceNumber(),
                    myNodeId,
                    line.getId(),
                    line.getVersion(),
                    line.resetReaderAndGetReadOnlyData());
        } else {
            line = shared.get(get.lineId);
            if (line != null) {
                // I'm not the owner
                // let's see whether I find the line
                // in the sharer map and see who the new owner is
                return new Message.OWNER_CHANGED(
                        get.getMessageSequenceNumber(),
                        myNodeId,
                        line.getId(),
                        line.getOwner(),
                        MessageType.GET
                );
            } else {
                // I don't know this cache line at all
                // looks like I'm part of a desperate broadcast
                // I'll just acknowledge the message and move on with my life
                return new Message.ACK(
                        get.getMessageSequenceNumber(),
                        myNodeId,
                        get.lineId
                );
            }
        }
    }

    private void handleACK(Message.ACK ack) {
        logger.info("cache handler {} ack: {}", this, ack);
    }

    private void handlePUTX(Message.PUTX putx) {
        logger.info("cache handler {} putx: {}", this, putx);
        final CacheLine oldLine = shared.get(putx.lineId);
        if (oldLine == null) {
            // looks like I didn't have this log line before
            // pure luxury, I can create a brand new object
            CacheLine newLine = new CacheLine(putx.lineId, putx.version, myNodeId, putx.data);
            newLine.setSharers(putx.sharers);
            newLine.setState(
                    newLine.getSharers().isEmpty() ? CacheLineState.EXCLUSIVE : CacheLineState.OWNED
            );
            owned.put(putx.lineId, newLine);
        } else {
            // promote line from shared to owned
            // It's somewhat important to keep the same object around
            // (as opposed to creating a new one and tossing the old one away).
            // One reason for that is that there might be other threads
            // waiting to grab a lock on this line.
            // If we switch the objects under the waiters all hell will break loose.
            oldLine.setOwner(myNodeId);
            oldLine.setSharers(putx.sharers);
            oldLine.setState(
                    oldLine.getSharers().isEmpty() ? CacheLineState.EXCLUSIVE : CacheLineState.OWNED
            );
            oldLine.setData(putx.data);
            oldLine.setVersion(putx.version);
            // I rather live with double-occurrence than the line not
            // showing up at all
            owned.put(putx.lineId, oldLine);
            shared.remove(putx.lineId);
        }
    }

    private void handlePUT(Message.PUT put) {
        logger.info("cache handler {} put: {}", this, put);
        // this is fairly straight forward
        // just take the new data and shove it in
        // TODO -- make sure we're keeping the same object around (mostly for locking reasons)
        CacheLine line = new CacheLine(put.lineId, put.version, put.getSender(), put.data);
        line.setState(CacheLineState.SHARED);
        shared.put(line.getId(), line);
    }

    private Message.Response handleINV(Message.INV inv) {
        logger.info("cache handler {} inv: {}", this, inv);
        CacheLine line = shared.get(inv.lineId);
        if (line != null) {
            // we're in luck I know the line
            line.setState(CacheLineState.INVALID);
            line.setOwner(inv.getSender());
        }
        return new Message.INVACK(
                inv.getMessageSequenceNumber(),
                myNodeId,
                inv.lineId
        );
    }

    private void handleINVACK(Message.INVACK invack) {
        logger.info("cache handler {} invack: {}", this, invack);
        CacheLine line = owned.get(invack.lineId);
        if (line != null) {
            line.removeSharer(invack.getSender());
            if (line.getSharers().isEmpty()) {
                line.setState(CacheLineState.EXCLUSIVE);
            }
        }
    }

    private void handleOWNER_CHANGED(Message.OWNER_CHANGED ownerChanged) {
        logger.info("cache handler {} ownerChanged: {}", this, ownerChanged);
        CacheLine line = shared.get(ownerChanged.lineId);
        if (line != null) {
            line.setState(CacheLineState.INVALID);
            line.setOwner(ownerChanged.newOwner);

            Message.Request messageToSend;
            switch (ownerChanged.originalMsgType) {
                case GET:
                    messageToSend = new Message.GET(myNodeId, ownerChanged.lineId);
                    break;
                case GETX:
                    messageToSend = new Message.GETX(myNodeId, ownerChanged.lineId);
                    break;
                default:
                    throw new RuntimeException("unknown message type " + ownerChanged.originalMsgType);
            }

            comms.reactToResponse(ownerChanged, ownerChanged.newOwner, messageToSend);
        }
    }

    // TODO -- this needs more work obviously
    private long nextClusterUniqueCacheLineId() {
        long newId = random.nextLong();
        while (usedIds.contains(newId)) {
            newId = random.nextLong();
        }
        usedIds.add(newId);
        return newId;
    }

    @Override
    public long allocateEmpty() throws IOException {
        long newLineId = nextClusterUniqueCacheLineId();
        CacheLine line = new CacheLine(newLineId, Integer.MIN_VALUE, comms.myNodeId, null);
        line.setState(CacheLineState.OWNED);
        owned.put(line.getId(), line);
        return line.getId();
    }

    @Override
    public long allocateWithData(ByteBuf buffer) throws IOException {
        CacheLine line = wrap(buffer);
        owned.put(line.getId(), line);
        return line.getId();
    }

    @Override
    public long allocateWithData(ByteBuffer buffer) throws IOException {
        return allocateWithData(Unpooled.wrappedBuffer(buffer));
    }

    @Override
    public long allocateWithData(byte[] bytes) throws IOException {
        ByteBuf buffer = Unpooled
                .directBuffer(bytes.length)
                .writeBytes(bytes);
        return allocateWithData(buffer);
    }

    private CacheLine wrap(ByteBuf bytebuf) {
        long newLineId = nextClusterUniqueCacheLineId();
        CacheLine line = new CacheLine(newLineId, Integer.MIN_VALUE, comms.myNodeId, bytebuf);
        line.setState(CacheLineState.EXCLUSIVE);
        return line;
    }

    @Override
    public ByteBuf get(long lineId) throws IOException {
        CacheLine line = getLineLocally(lineId);
        if (line == null) {
            CacheLine remoteLine = getLineRemotely(lineId);
            if (remoteLine == null) {
                return null;
            } else {
                return remoteLine.resetReaderAndGetReadOnlyData();
            }
        } else {
            return line.resetReaderAndGetReadOnlyData();
        }
    }

    @Override
    public ByteBuffer getBB(long lineId) throws IOException {
        return get(lineId).nioBuffer();
    }

    @Override
    public ByteBuf getx(long lineId) throws IOException {
        return getxLineRemotely(lineId).resetReaderAndGetReadOnlyData();
    }

    @Override
    public ByteBuffer getxBB(long lineId) throws IOException {
        return getx(lineId).nioBuffer();
    }

    @Override
    public void put(long lineId, ByteBuf buffer) {

    }

    @Override
    public void put(long lineId, ByteBuffer buffer) {
        put(lineId, Unpooled.wrappedBuffer(buffer));
    }

    private CacheLine getxLineRemotely(long lineId) throws IOException {
        Message.GETX getx = new Message.GETX(myNodeId, lineId);
        CacheLine line = getLineLocally(lineId);
        if (line != null) {
            innerUnicast(line.getOwner(), getx);
        } else {
            innerBroadcast(getx);
        }
        return getLineLocally(lineId);
    }

    private CacheLine getLineRemotely(long lineId) throws IOException {
        // it might be worth checking the shared map to see whether
        // we find a stub but the line is invalid
        Message.GET get = new Message.GET(comms.myNodeId, lineId);
        CacheLine line = shared.get(lineId);
        if (line != null) {
            // AHA!!
            innerUnicast(line.getOwner(), get);
        } else {
            // nope we don't know anything
            // we gotta find out where this thing is first
            innerBroadcast(get);
        }
        return getLineLocally(lineId);
    }

    private void innerUnicast(short nodeToSendTo, Message msgToSend) throws IOException {
        try {
            comms.send(nodeToSendTo, msgToSend).get(TIMEOUT_SECS, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException xcp) {
            throw new IOException(xcp);
        }
    }

    private void innerBroadcast(Message msgToSend) throws IOException {
        try {
            // TODO -- make it so that broadcasts only wait for the minimum number of relevant messages
            comms.broadcast(msgToSend).get(TIMEOUT_SECS, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException xcp) {
            throw new IOException(xcp);
        }
    }

    // this method will return null even if we know about the line
    // but can't use it
    private CacheLine getLineLocally(long lineId) {
        CacheLine line = innerGetLineLocally(lineId);
        if (line != null && CacheLineState.INVALID.equals(line.getState())) {
            // if this line is marked invalid, pretend we don't know it :)
            return null;
        } else {
            return line;
        }
    }

    // visible for testing
    CacheLine innerGetLineLocally(long lineId) {
        CacheLine line = owned.get(lineId);
        if (line == null) {
            line = shared.get(lineId);
        }
        return line;
    }

    @Override
    public void close() throws IOException {
        comms.close();
        for (CacheLine line : owned.values()) {
            line.releaseData();
        }
        for (CacheLine line : shared.values()) {
            line.releaseData();
        }
    }

    @Override
    public String toString() {
        return "myNodeId: " + myNodeId;
    }
}
