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

/**
 * The interface to the in-memory cache.
 */
public interface Cache extends Closeable {
    int getMaxCacheLineSize();
    ByteBuf allocateBuffer(int capacity);
    long allocateEmpty(Transaction txn) throws IOException;
    long allocateWithData(ByteBuf buffer, Transaction txn) throws IOException;
    long allocateWithData(byte[] bytes, Transaction txn) throws IOException;
    ByteBuf get(long lineId) throws IOException;
    ByteBuf getx(long lineId, Transaction txn) throws IOException;
    void put(long lineId, ByteBuf buffer, Transaction txn) throws IOException;
    Transaction newTransaction();
}
