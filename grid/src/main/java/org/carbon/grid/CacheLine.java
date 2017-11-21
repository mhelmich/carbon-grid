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
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

class CacheLine {
    private final long id;
    private volatile CacheLineState state;
    private volatile int version;
    private short owner = -1;
    private NonBlockingHashSet<Short> sharers;
    private byte flags;
    private ByteBuf data;
    private final ReentrantLock lock = new ReentrantLock();

    CacheLine(long id, int version, short owner, ByteBuf data) {
        this.id = id;
        this.version = version;
        this.owner = owner;
        this.data = data;
    }

    long getId() {
        return id;
    }

    void setState(CacheLineState state) {
        // TODO -- if the state is set to INVALID
        // I could go and aggressively purge data to free up memory
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

    void setData(ByteBuf data) {
        version++;
        this.data.release();
        this.data = data;
    }

    void releaseData() {
        if (data != null) {
            data.release();
        }
    }

    void setSharers(Set<Short> sharers) {
        if (!sharers.isEmpty()) {
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
        return "id: " + id + " state: " + state + " owner: " + owner + " sharers: " + sharers + " data size: " + data.capacity();
    }

    void lock() {
        lock.tryLock();
    }

    void unlock() {
        lock.unlock();
    }
}
