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
import org.cliffc.high_scale_lib.NonBlockingSetInt;

import java.util.Set;

class CacheLine {
    private final long id;
    private volatile CacheLineState state;
    private volatile int version;
    private short owner = -1;
    private NonBlockingSetInt sharers;
    private byte flags;
    private final ByteBuf data;

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
        version++;
        this.state = state;
    }

    CacheLineState getState() {
        return state;
    }

    void setOwner(short owner) {
        version++;
        this.owner = owner;
    }

    short getOwner() {
        return owner;
    }

    int getVersion() {
        return version;
    }

    void setFlags(byte flags) {
        version++;
        this.flags = flags;
    }

    byte getFlags() {
        return flags;
    }

    ByteBuf getData() {
        return data;
    }

    void addSharer(short newSharer) {
        if (sharers == null) {
            synchronized (this) {
                if (sharers == null) {
                    sharers = new NonBlockingSetInt();
                }
            }
        }

        sharers.add(newSharer);
    }

    Set<Integer> getSharers() {
        return sharers;
    }

    @Override
    public String toString() {
        return "id: " + id + " state: " + state + " owner: " + owner + " data size: " + data.capacity();
    }
}
