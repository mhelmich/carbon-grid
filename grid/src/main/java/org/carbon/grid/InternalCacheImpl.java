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

    private final static int TIMEOUT_SECS = 5;

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
            default:
                throw new RuntimeException("Unknown type " + request.type);
        }
    }

    private Message.Response handleGETX(Message.GETX getx) {
        CacheLine line = owned.remove(getx.lineId);
        if (line != null) {
            // I'm the owner
            // let's demote the cache line to invalid,
            // pack it up and ship it to the requester
            Set<Short> sharersToSend = line.getSharers();

            // change local status and status of cache line
            line.setState(CacheLineState.INVALID);
            line.setOwner(getx.sender);
            line.clearSharers();
            shared.put(getx.lineId, line);

            // massage sharers list
            sharersToSend.remove(getx.sender);
            sharersToSend.remove(myNodeId);

            // compose message
            Message.PUTX putx = new Message.PUTX(getx, myNodeId);
            putx.lineId = line.getId();
            putx.version = line.getVersion();
            putx.sharers = sharersToSend;
            putx.data = line.resetReaderAndGetReadOnlyData();
            return putx;
        } else {
            // I'm not the owner
            // let's see whether I find the line
            // in the sharer map and see who the new owner is
            // todo: send owner changed
            return null;
        }
    }

    private Message.Response handleGET(Message.GET get) {
        logger.info("cache handler get: {}", get);
        CacheLine line = owned.get(get.lineId);
        if (line != null) {
            line.addSharer(get.sender);
            return new Message.PUT(get, myNodeId, get.lineId, line.getVersion(), line.resetReaderAndGetReadOnlyData());
        } else {
            line = shared.get(get.lineId);
            if (line != null) {
                // I'm not the owner
                // let's see whether I find the line
                // in the sharer map and see who the new owner is
                // todo: send owner changed
                return null;
            } else {
                // I don't know this cache line at all
                // looks like I'm part of a desperate broadcast
                // I'll just acknowledge the message and move on with my life
                return new Message.ACK(get, myNodeId);
            }
        }
    }

    private void handleACK(Message.ACK ack) {
        logger.info("cache handler ack: {}", ack);
    }

    private void handlePUTX(Message.PUTX putx) {
        final CacheLine oldLine = shared.get(putx.lineId);
        if (oldLine == null) {
            // looks like I didn't have this log line before
            // pure luxury, I can create a brand new object
            CacheLine newLine = new CacheLine(putx.lineId, putx.version, myNodeId, putx.data);
            for (short s : putx.sharers) {
                newLine.addSharer(s);
            }
            newLine.setState(CacheLineState.OWNED);
            owned.put(putx.lineId, newLine);
        } else {
            // promote line from shared to owned
            // It's somewhat important to keep the same object around
            // (as opposed to creating a new one and tossing the old one away).
            // One reason for that is that there might be other threads
            // waiting to grab a lock on this line.
            // If we switch the objects under the waiters all hell will break loose.
            oldLine.setOwner(myNodeId);
            oldLine.setState(CacheLineState.OWNED);
            oldLine.setSharers(putx.sharers);
            oldLine.setData(putx.data);
            oldLine.setVersion(putx.version);
            // I rather live with double-occurrence than the line not
            // showing up at all
            owned.put(putx.lineId, oldLine);
            shared.remove(putx.lineId);
        }
    }

    private void handlePUT(Message.PUT put) {
        // this is fairly straight forward
        // just take the new data and shove it in
        // TODO -- make sure we're keeping the same object around (mostly for locking reasons)
        CacheLine line = new CacheLine(put.lineId, put.version, put.sender, put.data);
        line.setState(CacheLineState.SHARED);
        shared.put(line.getId(), line);
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
        line.setState(CacheLineState.OWNED);
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

    private CacheLine getxLineRemotely(long lineId) throws IOException {
        CacheLine line = getLineLocally(lineId);
        Message.GETX getx = new Message.GETX(myNodeId, lineId);
        if (line != null) {
            try {
                comms.send(line.getOwner(), getx).get(TIMEOUT_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException xcp) {
                throw new IOException(xcp);
            }
            return getLineLocally(lineId);
        } else {
            return null;
        }
    }

    private CacheLine getLineRemotely(long lineId) throws IOException {
        Message.GET get = new Message.GET(comms.myNodeId, lineId);
        // TODO -- make it so that broadcasts only wait for the minimum number of relevant messages
        // as opposed to for all outstanding messages regardless of whether they are relevant or not
        try {
            comms.broadcast(get).get(TIMEOUT_SECS, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException xcp) {
            throw new IOException(xcp);
        }
        return getLineLocally(lineId);
    }

    // visible for testing
    CacheLine getLineLocally(long lineId) {
        CacheLine line = owned.get(lineId);
        if (line == null) {
            line = shared.get(lineId);
        }
        return line;
    }

    @Override
    public void close() throws IOException {
        comms.close();
    }
}
