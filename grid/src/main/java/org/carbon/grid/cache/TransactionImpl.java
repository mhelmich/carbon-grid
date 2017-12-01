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

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This class is obviously NOT THREAD SAFE!
 */
class TransactionImpl implements Transaction {
    private final static Logger logger = LoggerFactory.getLogger(InternalCacheImpl.class);
    private final InternalCacheImpl cache;
    private final Map<Long, Undo> undoInfo = new HashMap<>();
    private final List<Long> lockedLines = new LinkedList<>();
    private final List<Message> messagesToSend = new LinkedList<>();

    TransactionImpl(InternalCacheImpl cache) {
        this.cache = cache;
    }

    void recordUndo(CacheLine line, ByteBuf newBuffer) {
        recordUndo(line.getId(), line.getVersion(), newBuffer);
    }

    void recordUndo(long lineId, int version, ByteBuf newBuffer) {
        addToLockedLines(lineId);
        undoInfo.put(lineId, new Undo(lineId, version, newBuffer));
    }

    void recordMessageToSend(Message msg) {
        messagesToSend.add(msg);
    }

    void addToLockedLines(long lineId) {
        lockedLines.add(lineId);
    }

    @Override
    public void commit() {
        try {
            // make all data changes
            for (Undo undo : undoInfo.values()) {
                CacheLine line = cache.innerGetLineLocally(undo.lineId);
                // cache lines need to exist locally
                if (line == null) throw new IllegalStateException("Line with id " + undo.lineId + " doesn't exist");
                // cache lines need to be locked
                if (!line.isLocked()) throw new IllegalStateException("Cache line " + line.getId() + " is not in state locked");
                // cache lines need to be in exclusive state
                if (!CacheLineState.EXCLUSIVE.equals(line.getState())) throw new IllegalStateException("Cache line with id " + undo.lineId + " is not in EXCLUSIVE state");

                line.setOwner(cache.myNodeId());
                line.setVersion(undo.version);
                line.setData(undo.buffer);
            }
            // TODO -- send all messages
        } finally {
            // release all lines
            releaseAllLines();
        }
    }

    @Override
    public void rollback() {
        try {
            for (Undo undoInfo : undoInfo.values()) {
                try {
                    undoInfo.buffer.release();
                } catch (Exception xcp) {
                    logger.error("Can't release buffer", xcp);
                }
            }
        } finally {
            releaseAllLines();
        }
    }

    private void releaseAllLines() {
        for (long lineId : lockedLines) {
            CacheLine line = cache.innerGetLineLocally(lineId);
            if (line != null) {
                try {
                    line.unlock();
                } catch (Exception xcp) {
                    logger.error("While releasing buffers in a txn line with id " + lineId + " disappeared", xcp);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        rollback();
    }

    private static class Undo {
        final long lineId;
        final int version;
        final ByteBuf buffer;

        Undo(long lineId, int version, ByteBuf buffer) {
            this.lineId = lineId;
            this.version = version;
            this.buffer = buffer;
        }
    }
}
