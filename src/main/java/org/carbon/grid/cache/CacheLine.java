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
import io.netty.buffer.Unpooled;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

class CacheLine {
    private final static Logger logger = LoggerFactory.getLogger(CacheLine.class);
    private final long id;
    private volatile CacheLineState state;
    private volatile int version;
    private short owner = -1;
    private NonBlockingHashSet<Short> sharers;
    private byte flags;
    private ByteBuf data;
    private final AtomicBoolean isLocked = new AtomicBoolean(false);

    CacheLine(long id, int version, short owner, CacheLineState state, ByteBuf data) {
        this.id = id;
        this.state = state;
        this.version = version;
        this.owner = owner;
        this.data = data;
    }

    long getId() {
        return id;
    }

    void setState(CacheLineState state) {
        this.state = state;
    }

    CacheLineState getState() {
        return state;
    }

    void setOwner(short owner) {
        this.owner = owner;
    }

    short getOwner() {
        return owner;
    }

    void setVersion(int version) {
        this.version = version;
    }

    int getVersion() {
        return version;
    }

    void setFlags(byte flags) {
        this.flags = flags;
    }

    byte getFlags() {
        return flags;
    }

    ByteBuf resetReaderAndGetReadOnlyData() {
        return (data != null) ? data.resetReaderIndex().asReadOnly() : Unpooled.EMPTY_BUFFER;
    }

    // releases the backing ByteBuf before flipping the pointers
    void setData(ByteBuf data) {
        version++;
        releaseData();
        this.data = data;
    }

    void releaseData() {
        if (data != null) {
            data.release();
            data = null;
        }
    }

    void setSharers(Set<Short> sharers) {
        if (sharers != null && !sharers.isEmpty()) {
            if (this.sharers == null) {
                this.sharers = new NonBlockingHashSet<>();
            } else {
                this.sharers.clear();
            }
            this.sharers.addAll(sharers);
        }
    }

    void addSharer(short newSharer) {
        if (sharers == null) {
            sharers = new NonBlockingHashSet<>();
        }
        sharers.add(newSharer);
    }

    void removeSharer(short sharer) {
        sharers.remove(sharer);
        if (sharers.isEmpty()) {
            sharers = null;
        }
    }

    Set<Short> getSharers() {
        return sharers == null ? Collections.emptySet() : sharers;
    }

    void clearSharers() {
        sharers = null;
    }

    @Override
    public String toString() {
        return "id: " + id + " version: " + version + " state: " + state + " owner: " + owner + " sharers: " + sharers + " data: " + ((data == null) ? "null" : data.toString());
    }

    void lock() {
        while (!isLocked.compareAndSet(false, true)) {
            try {
                // give up the thread and try again later
                Thread.sleep(1);
            } catch (InterruptedException xcp) {
                throw new RuntimeException(xcp);
            }
        }
    }

    void unlock() {
        if (!isLocked.compareAndSet(true, false)) {
            logger.warn("thread {} tried to unlock line {} but was unlocked already", Thread.currentThread().getName(), id);
        }
    }

    boolean isLocked() {
        return isLocked.get();
    }
}
