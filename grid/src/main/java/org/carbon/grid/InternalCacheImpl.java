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

class InternalCacheImpl implements InternalCache, Closeable {
    private final static Logger logger = LoggerFactory.getLogger(InternalCacheImpl.class);
    private static Random random = new Random();
    private static Set<Long> usedIds = new HashSet<>();

    private final NonBlockingHashMapLong<CacheLine> owned = new NonBlockingHashMapLong<>();
    private final NonBlockingHashMapLong<CacheLine> shared = new NonBlockingHashMapLong<>();

    final GridCommunications comms;

    InternalCacheImpl(int myNodeId, int myPort) {
        comms = new GridCommunications(myNodeId, myPort, this);
    }

    @Override
    public void handleResponse(Message.Response response) {
        switch (response.type) {
            case ACK:
                handleACK((Message.ACK)response);
                return;
            case PUT:
                handlePUT((Message.PUT)response);
                return;
            default:
                throw new RuntimeException("Unknown type " + response.type);
        }
    }

    @Override
    public Message.Response handleRequest(Message.Request request) {
        switch (request.type) {
            case GET:
                return handleGET((Message.GET)request);
            default:
                throw new RuntimeException("Unknown type " + request.type);
        }
    }

    private Message.Response handleGET(Message.GET get) {
        logger.info("cache handler get: {}", get);
        CacheLine line = owned.get(get.lineId);
        if (line != null) {
            line.addSharer(get.sender);
            return new Message.PUT(get, get.lineId, line.getVersion(), line.getData());
        } else {
            return new Message.ACK(get);
        }
    }

    private void handleACK(Message.ACK ack) {
        logger.info("cache handler ack: {}", ack);
    }

    private void handlePUT(Message.PUT put) {
        logger.info("cache handler put: {}", put);
        CacheLine line = new CacheLine(put.lineId, put.version, put.sender, put.data);
        line.setState(CacheLineState.SHARED);
        shared.put(line.getId(), line);
    }

    private long nextClusterUniqueCacheLineId() {
        long newId = random.nextLong();
        while (usedIds.contains(newId)) {
            newId = random.nextLong();
        }
        usedIds.add(newId);
        return newId;
    }

    @Override
    public long allocateEmpty() {
        long newLineId = nextClusterUniqueCacheLineId();
        CacheLine line = new CacheLine(newLineId, Integer.MIN_VALUE, comms.myNodeId, null);
        line.setState(CacheLineState.OWNED);
        owned.put(line.getId(), line);
        return line.getId();
    }

    @Override
    public long allocateWithData(ByteBuf buffer) {
        CacheLine line = wrap(buffer);
        owned.put(line.getId(), line);
        return line.getId();
    }

    @Override
    public long allocateWithData(ByteBuffer buffer) {
        return allocateWithData(Unpooled.wrappedBuffer(buffer));
    }

    @Override
    public long allocateWithData(byte[] bytes) {
        return allocateWithData(Unpooled.wrappedBuffer(bytes));
    }

    private CacheLine wrap(ByteBuf bytebuf) {
        long newLineId = nextClusterUniqueCacheLineId();
        CacheLine line = new CacheLine(newLineId, Integer.MIN_VALUE, comms.myNodeId, bytebuf);
        line.setState(CacheLineState.OWNED);
        return line;
    }

    @Override
    public ByteBuf get(long lineId) {
        CacheLine line = getLineLocally(lineId);
        if (line == null) {
            CacheLine remoteLine = getLineRemotely(lineId);
            if (remoteLine == null) {
                return null;
            } else {
                return remoteLine.getData();
            }
        } else {
            return line.getData();
        }
    }

    @Override
    public ByteBuffer getBB(long lineId) {
        return get(lineId).nioBuffer();
    }

    @Override
    public ByteBuf getx(long lineId) {
        return null;
    }

    @Override
    public ByteBuffer getxBB(long lineId) {
        return getx(lineId).nioBuffer();
    }

    private CacheLine getLineRemotely(long lineId) {
        return null;
    }

    private CacheLine getLineLocally(long lineId) {
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
