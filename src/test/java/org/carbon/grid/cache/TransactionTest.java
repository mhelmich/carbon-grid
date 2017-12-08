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
import io.netty.buffer.Unpooled;
import org.carbon.grid.BaseTest;
import org.carbon.grid.CarbonGrid;
import org.carbon.grid.cluster.GloballyUniqueIdAllocator;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TransactionTest extends BaseTest {
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

    @Test
    public void testProcessMessagesAfterLock() throws IOException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        ByteBuf bufferToOverrideWith = newRandomBuffer();
        CountDownLatch finishedHandleMessage = new CountDownLatch(1);
        try (InternalCacheImpl cache = new InternalCacheImpl(mockNodeIdProvider((short)123), Mockito.mock(CarbonGrid.CacheConfig.class), mockServerConfig(22344), mockIdAllocatorProvider()) {
            @Override
            public void handleResponse(Message.Response response) {
                super.handleResponse(response);
                finishedHandleMessage.countDown();
            }
        }) {
            CacheLine line = putNewEmptyCacheLineIntoCache(cache);
            TransactionImpl txn = (TransactionImpl) cache.newTransaction();
            line.lock();
            txn.addToLockedLines(line.getId());
            Message msg = new Message.PUTX(123456789, (short) 999, line.getId(), line.getVersion() + 1, Collections.emptySet(), bufferToOverrideWith);
            assertEquals(Unpooled.EMPTY_BUFFER, line.resetReaderAndGetReadOnlyData());
            cache.comms.handleMessage(msg);
            txn.rollback();
            assertTrue(finishedHandleMessage.await(555, TimeUnit.SECONDS));
            assertEquals(bufferToOverrideWith, line.resetReaderAndGetReadOnlyData());
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
        return new InternalCacheImpl(mockNodeIdProvider(nodeId), Mockito.mock(CarbonGrid.CacheConfig.class), mockServerConfig(port), mockIdAllocatorProvider());
    }

    private Provider<Short> mockNodeIdProvider(short nodeId) {
        return () -> nodeId;
    }

    private Provider<GloballyUniqueIdAllocator> mockIdAllocatorProvider() {
        return () -> idAllocator::incrementAndGet;
    }
}
