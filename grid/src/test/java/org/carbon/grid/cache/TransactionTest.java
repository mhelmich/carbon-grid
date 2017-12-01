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

import com.google.inject.Provider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.carbon.grid.CarbonGrid;
import org.carbon.grid.cluster.GloballyUniqueIdAllocator;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class TransactionTest {
    private static final Set<ByteBuf> buffers = new HashSet<>();
    private final Random random = new Random();
    private static final AtomicLong idAllocator = new AtomicLong(0);

    @Test
    public void testCommitExistingLine() throws IOException, NoSuchFieldException, IllegalAccessException {
        ByteBuf initialBuffer = newRandomBuffer();
        ByteBuf bufferToOverrideWith = newRandomBuffer();

        try (InternalCacheImpl cache = mockCache(123, 23455)) {
            CacheLine line = putNewEmptyCacheLineIntoCache(cache);
            line.lock();
            line.setData(initialBuffer);
            TransactionImpl txn = (TransactionImpl) cache.newTransaction();
            txn.recordUndo(line, bufferToOverrideWith);
            txn.commit();
            assertNotNull(line.resetReaderAndGetReadOnlyData());
            assertEquals(bufferToOverrideWith, line.resetReaderAndGetReadOnlyData());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCommitNoExistingLine() throws IOException {
        ByteBuf buffer = newRandomBuffer();

        int newVersion = 631;
        long lineId = 987654321;

        try (InternalCacheImpl cache = mockCache(123, 23455)) {
            TransactionImpl txn = (TransactionImpl) cache.newTransaction();
            txn.recordUndo(lineId, newVersion, buffer);
            txn.commit();
        }
    }

    @Test
    public void testRollback() throws IOException, NoSuchFieldException, IllegalAccessException {
        ByteBuf initialBuffer = newRandomBuffer();
        ByteBuf bufferToOverrideWith = newRandomBuffer();

        try (InternalCacheImpl cache = mockCache(123, 23455)) {
            CacheLine line = putNewEmptyCacheLineIntoCache(cache);
            line.lock();
            line.setData(initialBuffer);
            TransactionImpl txn = (TransactionImpl) cache.newTransaction();
            txn.recordUndo(line, bufferToOverrideWith);
            txn.rollback();
            assertNotNull(line.resetReaderAndGetReadOnlyData());
            assertEquals(initialBuffer, line.resetReaderAndGetReadOnlyData());
        }
    }

    private CacheLine putNewEmptyCacheLineIntoCache(InternalCacheImpl cache) throws NoSuchFieldException, IllegalAccessException {
        NonBlockingHashMapLong<CacheLine> ownedLines = getOwnedCacheLines(cache);
        CacheLine line = newEmptyCacheLine(cache);
        ownedLines.put(line.getId(), line);
        return line;
    }

    private CacheLine newEmptyCacheLine(InternalCacheImpl cache) {
        return new CacheLine(
                newLineId(cache),
                0,
                (short)13579,
                CacheLineState.EXCLUSIVE,
                null
        );
    }

    private long newLineId(InternalCacheImpl cache) {
        long id;
        do {
            id = random.nextLong();
        } while (cache.innerGetLineLocally(id) != null);
        // not perfect but effective
        return id;
    }

    private ByteBuf newRandomBuffer() {
        byte[] bites = new byte[1024];
        random.nextBytes(bites);
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(1024);
        buffer.writeBytes(bites);
        buffers.add(buffer);
        return buffer;
    }

    // not the nicest but effective
    @AfterClass
    public static void clearBuffers() {
        for (ByteBuf buf : buffers) {
            if (buf.refCnt() > 0) {
                buf.release();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private NonBlockingHashMapLong<CacheLine> getOwnedCacheLines(InternalCacheImpl cache) throws NoSuchFieldException, IllegalAccessException {
        Field field = InternalCacheImpl.class.getDeclaredField("owned");
        field.setAccessible(true);
        return (NonBlockingHashMapLong<CacheLine>) field.get(cache);
    }

    private InternalCacheImpl mockCache(int nodeId, int port) {
        return mockCache((short) nodeId, port);
    }

    private InternalCacheImpl mockCache(short nodeId, int port) {
        return new InternalCacheImpl(mockNodeIdProvider(nodeId), mockServerConfig(port), mockIdAllocatorProvider());
    }

    private CarbonGrid.ServerConfig mockServerConfig(int port) {
        CarbonGrid.ServerConfig sc = Mockito.mock(CarbonGrid.ServerConfig.class);
        when(sc.port()).thenReturn(port);
        when(sc.timeout()).thenReturn(60);
        return sc;
    }

    private Provider<Short> mockNodeIdProvider(short nodeId) {
        return () -> nodeId;
    }

    private Provider<GloballyUniqueIdAllocator> mockIdAllocatorProvider() {
        return () -> idAllocator::incrementAndGet;
    }
}
