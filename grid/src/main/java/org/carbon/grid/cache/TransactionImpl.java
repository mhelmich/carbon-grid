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

import java.io.Closeable;
import java.io.IOException;

class TransactionImpl implements Transaction, Closeable {
    private final InternalCache cache;

    TransactionImpl(InternalCache cache) {
        this.cache = cache;
    }

    void recordUndo(CacheLine line, ByteBuf newBuffer) {

    }

    void recordUndo(long lineId, int version, ByteBuf buffer) {

    }

    void addToLockedLines(long lineId) {

    }

    void commit() {

    }

    void rollback() {

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
