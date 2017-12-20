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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.SocketUtils;
import org.carbon.grid.BaseTest;
import org.carbon.grid.cluster.GloballyUniqueIdAllocator;
import org.carbon.grid.cluster.ReplicaIdSupplier;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class CacheTest extends BaseTest {
    private static final AtomicLong idAllocator = new AtomicLong(0);

    @Test
    public void testPingPong() throws IOException, ExecutionException, InterruptedException {
        short node1 = 123;
        short node2 = 456;
        int port1 = 4444;
        int port2 = 5555;
        try (InternalCacheImpl cache123 = mockCache(node1, port1)) {
            try (InternalCacheImpl cache456 = mockCache(node2, port2)) {
                cache123.handlePeerChange(ImmutableMap.of(
                        node2, SocketUtils.socketAddress("localhost", port2)
                ));
                cache456.handlePeerChange(ImmutableMap.of(
                        node1, SocketUtils.socketAddress("localhost", port1)
                ));

                Message.GET get = new Message.GET(node1, 999L);
                Future<Void> f1 = cache123.comms.send(cache456.myNodeId(), get);
                f1.get();
                assertTrue(f1.isDone());
            }
        }
    }

    @Test
    public void testGetPutCache() throws IOException {
        ThreeCaches threeCaches = createCluster();
        String testData = "testing_test";
        ByteBuf localBB = null;
        ByteBuf remoteBB = null;
        try {
            Transaction txn = threeCaches.cache123.newTransaction();
            long newBlockId = threeCaches.cache123.allocateWithData(testData.getBytes(), txn);
            txn.commit();
            localBB = threeCaches.cache123.get(newBlockId);
            assertEquals(2, localBB.refCnt());
            assertEqualsBites(testData.getBytes(), localBB);

            remoteBB = threeCaches.cache456.get(newBlockId);
            assertEquals(2, remoteBB.refCnt());
            assertEqualsBites(testData.getBytes(), remoteBB);
        } finally {
            releaseByteBuf(localBB, remoteBB);
            closeThreeCaches(threeCaches);
        }
        assertEquals(0, localBB.refCnt());
        assertEquals(0, remoteBB.refCnt());
    }

    @Test
    public void testAbsentLine() throws IOException {
        ByteBuf buffer = null;
        try (InternalCacheImpl cache123 = mockCache(123, 5555)) {
            buffer = cache123.get(123);
            assertNull(buffer);
        } finally {
            releaseByteBuf(buffer);
        }
    }

    @Test
    public void testAllocateEmpty() throws IOException {
        ByteBuf buffer = null;
        try (InternalCacheImpl cache123 = mockCache(123, 5555)) {
            Transaction txn = cache123.newTransaction();
            long emptyBlock = cache123.allocateEmpty(txn);
            txn.commit();
            buffer = cache123.get(emptyBlock);
            assertNotNull(buffer);
            assertEquals(0, buffer.readableBytes());
            assertEquals(1, buffer.refCnt());
            CacheLine line = cache123.innerGetLineLocally(emptyBlock);
            assertEquals(CacheLineState.EXCLUSIVE, line.getState());
        } finally {
            releaseByteBuf(buffer);
        }

        // this is the static EMPTY_BUFFER
        // that guy always has a reference count of one
        assertEquals(1, buffer.refCnt());
    }

    @Test
    public void testGetxPutxBasic() throws IOException {
        ThreeCaches threeCaches = createCluster();
        String testData = "testing_test";
        ByteBuf localBB = null;
        ByteBuf localBB2 = null;
        ByteBuf remoteBB = null;
        try {
            Transaction txn = threeCaches.cache123.newTransaction();
            // set a line in cache123 and verify both caches
            long newCacheLineId = threeCaches.cache123.allocateWithData(testData.getBytes(), txn);
            txn.commit();
            localBB = threeCaches.cache123.get(newCacheLineId);
            // one ref in the cache and one locally
            assertEquals(2, localBB.refCnt());
            assertEqualsBites(testData.getBytes(), localBB);
            CacheLine line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertNotNull(line123);
            assertEquals(CacheLineState.EXCLUSIVE, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId(), line123.getOwner());
            CacheLine line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNull(line456);

            // get the line in cache456 and verify the state in cache456
            localBB2 = threeCaches.cache456.get(newCacheLineId);
            // one ref in the cache and one locally
            assertEquals(2, localBB2.refCnt());
            assertEqualsBites(testData.getBytes(), localBB2);
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            assertEquals(CacheLineState.SHARED, line456.getState());
            assertEquals(threeCaches.cache123.myNodeId(), line456.getOwner());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.OWNED, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId(), line123.getOwner());
            assertTrue(line123.getSharers().size() == 1);

            txn = threeCaches.cache456.newTransaction();
            // transfer ownership to cache456
            remoteBB = threeCaches.cache456.getx(newCacheLineId, txn);
            txn.rollback();
            assertEquals(2, remoteBB.refCnt());
            // after getx both caches should not have a reference to the ByteBuf anymore
            // there should just be the local reference
            assertEquals(1, localBB.refCnt());
            assertEquals(1, localBB2.refCnt());
            assertEqualsBites(testData.getBytes(), remoteBB);
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            // since there are no sharers this line will be in status EXCLUSIVE
            assertEquals(CacheLineState.EXCLUSIVE, line456.getState());
            assertEquals(threeCaches.cache456.myNodeId(), line456.getOwner());
            assertTrue(line456.getSharers().isEmpty());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.INVALID, line123.getState());
            assertEquals(threeCaches.cache456.myNodeId(), line123.getOwner());
        } finally {
            releaseByteBuf(localBB, localBB2, remoteBB);
            closeThreeCaches(threeCaches);
        }

        assertEquals(0, localBB.refCnt());
        assertEquals(0, localBB2.refCnt());
        assertEquals(0, remoteBB.refCnt());
    }

    @Test
    public void testGetxPutxThreeWay() throws IOException {
        ThreeCaches threeCaches = createCluster();
        String testData = "testing_test";
        ByteBuf localBB = null;
        ByteBuf localBB2 = null;
        ByteBuf localBB3 = null;
        ByteBuf remoteBB = null;
        try {
            Transaction txn = threeCaches.cache123.newTransaction();
            // set a line in cache123 and verify all other caches
            // we expect nobody else to know about this cache line
            long newCacheLineId = threeCaches.cache123.allocateWithData(testData.getBytes(), txn);
            txn.commit();
            localBB = threeCaches.cache123.get(newCacheLineId);
            assertEqualsBites(testData.getBytes(), localBB);
            assertEquals(2, localBB.refCnt());
            CacheLine line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertNotNull(line123);
            assertEquals(CacheLineState.EXCLUSIVE, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId(), line123.getOwner());
            CacheLine line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNull(line456);
            CacheLine line789 = threeCaches.cache789.innerGetLineLocally(newCacheLineId);
            assertNull(line789);

            // get the line in cache456 and verify the state in cache456
            localBB2 = threeCaches.cache456.get(newCacheLineId);
            assertEqualsBites(testData.getBytes(), localBB2);
            assertEquals(2, localBB2.refCnt());
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            assertEquals(CacheLineState.SHARED, line456.getState());
            assertEquals(threeCaches.cache123.myNodeId(), line456.getOwner());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.OWNED, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId(), line123.getOwner());
            assertTrue(line123.getSharers().size() == 1);
            // pull the cache line into cache 3
            localBB3 = threeCaches.cache789.get(newCacheLineId);
            assertEquals(2, localBB3.refCnt());
            assertEqualsBites(testData.getBytes(), localBB3);
            line789 = threeCaches.cache789.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.SHARED, line789.getState());
            assertTrue(line123.getSharers().size() == 2);

            txn = threeCaches.cache456.newTransaction();
            // transfer ownership to cache456
            remoteBB = threeCaches.cache456.getx(newCacheLineId, txn);
            txn.rollback();
            assertEquals(2, remoteBB.refCnt());
            assertEquals(1, localBB.refCnt());
            assertEquals(1, localBB2.refCnt());
            assertEquals(1, localBB3.refCnt());
            assertEqualsBites(testData.getBytes(), remoteBB);
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            // since there are no sharers this line will be in status EXCLUSIVE
            assertEquals(CacheLineState.EXCLUSIVE, line456.getState());
            assertEquals(threeCaches.cache456.myNodeId(), line456.getOwner());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.INVALID, line123.getState());
            assertEquals(threeCaches.cache456.myNodeId(), line123.getOwner());
            // but as it turns out cache 789 never heard about the change in ownership
            assertEquals(threeCaches.cache456.myNodeId(), line789.getOwner());
        } finally {
            releaseByteBuf(localBB, localBB2, localBB3, remoteBB);
            closeThreeCaches(threeCaches);
        }

        assertEquals(0, localBB.refCnt());
        assertEquals(0, localBB2.refCnt());
        assertEquals(0, localBB3.refCnt());
        assertEquals(0, remoteBB.refCnt());
    }

    @Test
    public void testOwnershipMoveInvalidate() throws IOException {
        ThreeCaches threeCaches = createCluster();
        String testData = "testing_test";
        ByteBuf buffer123 = null;
        ByteBuf buffer456 = null;
        try {
            Transaction txn = threeCaches.cache123.newTransaction();
            // set a line in cache123 and verify all other caches
            // we expect nobody else to know about this cache line
            long newCacheLineId = threeCaches.cache123.allocateWithData(testData.getBytes(), txn);
            txn.commit();
            buffer123 = threeCaches.cache123.get(newCacheLineId);
            assertEquals(2, buffer123.refCnt());
            assertEqualsBites(testData.getBytes(), buffer123);
            CacheLine line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertNotNull(line123);
            assertEquals(CacheLineState.EXCLUSIVE, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId(), line123.getOwner());
            CacheLine line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNull(line456);
            CacheLine line789 = threeCaches.cache789.innerGetLineLocally(newCacheLineId);
            assertNull(line789);
            threeCaches.cache789.get(newCacheLineId);
            line789 = threeCaches.cache789.innerGetLineLocally(newCacheLineId);
            assertNotNull(line789);

            txn = threeCaches.cache456.newTransaction();
            // transfer ownership to cache456
            buffer456 = threeCaches.cache456.getx(newCacheLineId, txn);
            txn.rollback();
            // now something fascinating happens:
            // asynchronously one node will ask the other node to invalidate
            // the line locally but that requires a bunch of messages to be sent
            // and received and state changed here and there
            // unfortunately that will take some time and I can't assert on any state
            // ... at least without latching and waiting *sigh*
            assertEquals(2, buffer456.refCnt());
            assertEqualsBites(testData.getBytes(), buffer456);
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            assertEquals(CacheLineState.EXCLUSIVE, line456.getState());
            assertEquals(threeCaches.cache456.myNodeId(), line456.getOwner());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.INVALID, line123.getState());
            assertEquals(threeCaches.cache456.myNodeId(), line123.getOwner());
            // putx also sends messages to every sharer the invalidate line in question
            // and then it also changes ownership in the process
            assertEquals(threeCaches.cache456.myNodeId(), line789.getOwner());
            // now we're trying to get the cache line from 789
            // that in turn should make a few round trips necessary
            assertEquals(CacheLineState.INVALID, line789.getState());
        } finally {
            releaseByteBuf(buffer123, buffer456);
            closeThreeCaches(threeCaches);
        }

        assertEquals(0, buffer123.refCnt());
        assertEquals(0, buffer456.refCnt());
    }

    @Test
    public void testAllocateByteBuf() throws IOException {
        ThreeCaches threeCaches = createCluster();
        try {
            ByteBuf buffer = threeCaches.cache123.allocateBuffer(1024);
            try {
                assertEquals(1024, buffer.capacity());
            } finally {
                releaseByteBuf(buffer);
            }
        } finally {
            closeThreeCaches(threeCaches);
        }
    }

    @Test
    public void testPut() throws IOException {
        long lineId = 1234567890;
        ByteBuf buffer = newRandomBuffer();
        ByteBuf putBuffer = null;
        ThreeCaches threeCaches = createCluster();
        Transaction txn = threeCaches.cache123.newTransaction();
        try {
            threeCaches.cache123.put(lineId, buffer, txn);
            CacheLine localLine = threeCaches.cache123.innerGetLineLocally(lineId);
            assertNotNull(localLine);
            assertEquals(CacheLineState.EXCLUSIVE, localLine.getState());
            txn.commit();
            putBuffer = threeCaches.cache123.get(lineId);
            assertEquals(buffer, putBuffer);
        } finally {
            releaseByteBuf(buffer, putBuffer);
            closeThreeCaches(threeCaches);
            assertEquals(0, buffer.refCnt());
        }
    }

    @Test
    public void testBackup() throws IOException, InterruptedException {
        //////////////////////////////////
        /////////////////////////////
        // set up three caches
        CountDownLatch backupCalledForAllBackups = new CountDownLatch(2);

        short node1 = 123;
        short node2 = 456;
        short node3 = 789;
        int port1 = 4444;
        int port2 = 5555;
        int port3 = 6666;
        ReplicaIdSupplier supplier1 = Mockito.mock(ReplicaIdSupplier.class);
        when(supplier1.get()).thenReturn(ImmutableList.of(node2, node3));
        ReplicaIdSupplier supplier2 = Mockito.mock(ReplicaIdSupplier.class);
        when(supplier2.get()).thenReturn(ImmutableList.of(node1, node3));
        ReplicaIdSupplier supplier3 = Mockito.mock(ReplicaIdSupplier.class);
        when(supplier3.get()).thenReturn(ImmutableList.of(node1, node2));
        Replication replication1 = new ReplicationImpl(mockBackupConfig());
        Replication replication2 = new ReplicationImpl(mockBackupConfig()) {
            @Override
            public void backUp(short leaderId, long leaderEpoch, CacheLine line) {
                super.backUp(leaderId, leaderEpoch, line);
                backupCalledForAllBackups.countDown();
            }
        };
        Replication replication3 = new ReplicationImpl(mockBackupConfig()) {
            @Override
            public void backUp(short leaderId, long leaderEpoch, CacheLine line) {
                super.backUp(leaderId, leaderEpoch, line);
                backupCalledForAllBackups.countDown();
            }
        };
        InternalCacheImpl internalCache1 = mockCache(node1, port1, () -> supplier1, replication1);
        InternalCacheImpl internalCache2 = mockCache(node2, port2, () -> supplier2, replication2);
        InternalCacheImpl internalCache3 = mockCache(node3, port3, () -> supplier3, replication3);

        internalCache1.handlePeerChange(ImmutableMap.of(
                node2, SocketUtils.socketAddress("localhost", port2),
                node3, SocketUtils.socketAddress("localhost", port3)
        ));
        internalCache2.handlePeerChange(ImmutableMap.of(
                node1, SocketUtils.socketAddress("localhost", port1),
                node3, SocketUtils.socketAddress("localhost", port3)
        ));
        internalCache3.handlePeerChange(ImmutableMap.of(
                node1, SocketUtils.socketAddress("localhost", port1),
                node2, SocketUtils.socketAddress("localhost", port2)
        ));

        long lineId = 1234567890;
        ByteBuf buffer = newRandomBuffer();

        try {
            //////////////////////////////////
            /////////////////////////////
            // run the actual test
            Transaction txn = internalCache1.newTransaction();
            internalCache1.put(lineId, buffer, txn);
            CacheLine localLine = internalCache1.innerGetLineLocally(lineId);
            assertNotNull(localLine);
            assertEquals(CacheLineState.EXCLUSIVE, localLine.getState());
            txn.commit();

            assertTrue(backupCalledForAllBackups.await(TIMEOUT_SECS, TimeUnit.SECONDS));
            assertEquals(1, replication2.getCacheLinesForLeader(node1).size());
            assertEquals(1, replication3.getCacheLinesForLeader(node1).size());
            assertTrue(replication2.getCacheLinesForLeader(node1).containsKey(lineId));
            assertTrue(replication3.getCacheLinesForLeader(node1).containsKey(lineId));
            assertEquals(localLine.getVersion(), replication2.getCacheLinesForLeader(node1).get(lineId).getVersion());
            assertEquals(localLine.getVersion(), replication3.getCacheLinesForLeader(node1).get(lineId).getVersion());
            assertEqualsByteBuf(localLine.resetReaderAndGetReadOnlyData(), replication2.getCacheLinesForLeader(node1).get(lineId).resetReaderAndGetReadOnlyData());
            assertEqualsByteBuf(localLine.resetReaderAndGetReadOnlyData(), replication3.getCacheLinesForLeader(node1).get(lineId).resetReaderAndGetReadOnlyData());
        } finally {
            releaseByteBuf(buffer);
            try {
                internalCache1.close();
            } finally {
                try {
                    internalCache2.close();
                } finally {
                    internalCache3.close();
                }
            }
        }

        assertEquals(0, buffer.refCnt());
    }

    private byte[] getAllBytesFromBuffer(ByteBuf buffer) {
        byte[] bites = new byte[buffer.readableBytes()];
        buffer.readBytes(bites, 0, bites.length);
        return bites;
    }

    private void assertEqualsBites(byte[] bites, ByteBuf buffer) {
        assertEquals(bites.length, buffer.capacity());
        byte[] bufferBites = getAllBytesFromBuffer(buffer);
        assertTrue(Arrays.equals(bites, bufferBites));
    }

    private void assertEqualsByteBuf(ByteBuf b1, ByteBuf b2) {
        assertEquals(b1.capacity(), b2.capacity());
        byte[] bites1 = getAllBytesFromBuffer(b1);
        byte[] bites2 = getAllBytesFromBuffer(b2);
        assertTrue(Arrays.equals(bites1, bites2));
    }

    private void closeThreeCaches(ThreeCaches threeCaches) throws IOException {
        try {
            threeCaches.cache123.close();
        } finally {
            try {
                threeCaches.cache456.close();
            } finally {
                threeCaches.cache789.close();
            }
        }
    }

    private ThreeCaches createCluster() {
        short node1 = 123;
        short node2 = 456;
        short node3 = 789;
        int port1 = 4444;
        int port2 = 5555;
        int port3 = 6666;
        InternalCacheImpl internalCache1 = mockCache(node1, port1);
        InternalCacheImpl internalCache2 = mockCache(node2, port2);
        InternalCacheImpl internalCache3 = mockCache(node3, port3);

        internalCache1.handlePeerChange(ImmutableMap.of(
                node2, SocketUtils.socketAddress("localhost", port2),
                node3, SocketUtils.socketAddress("localhost", port3)
        ));
        internalCache2.handlePeerChange(ImmutableMap.of(
                node1, SocketUtils.socketAddress("localhost", port1),
                node3, SocketUtils.socketAddress("localhost", port3)
        ));
        internalCache3.handlePeerChange(ImmutableMap.of(
                node1, SocketUtils.socketAddress("localhost", port1),
                node2, SocketUtils.socketAddress("localhost", port2)
        ));

        return new ThreeCaches(internalCache1, internalCache2, internalCache3);
    }

    private void releaseByteBuf(ByteBuf... buf) {
        for (ByteBuf b : buf) {
            if (b != null) {
                b.release();
            }
        }
    }

    private static class ThreeCaches {
        final InternalCacheImpl cache123;
        final InternalCacheImpl cache456;
        final InternalCacheImpl cache789;
        ThreeCaches(InternalCacheImpl cache123, InternalCacheImpl cache456, InternalCacheImpl cache789) {
            this.cache123 = cache123;
            this.cache456 = cache456;
            this.cache789 = cache789;
        }
    }

    private InternalCacheImpl mockCache(int nodeId, int port) {
        return mockCache((short) nodeId, port);
    }

    private InternalCacheImpl mockCache(short nodeId, int port) {
        return mockCache(nodeId, port, mockReplicaIdProvider(), mockBackup());
    }

    private InternalCacheImpl mockCache(short nodeId, int port, Provider<ReplicaIdSupplier> replicaIdSupplierProvider, Replication replication) {
        return new InternalCacheImpl(mockNodeIdProvider(nodeId), mockCacheConfig(), mockServerConfig(port), mockIdAllocatorProvider(), replicaIdSupplierProvider, replication);
    }

    private Provider<Short> mockNodeIdProvider(short nodeId) {
        return () -> nodeId;
    }

    private Provider<GloballyUniqueIdAllocator> mockIdAllocatorProvider() {
        return () -> idAllocator::incrementAndGet;
    }

    private Provider<ReplicaIdSupplier> mockReplicaIdProvider() {
        ReplicaIdSupplier supplier = Mockito.mock(ReplicaIdSupplier.class);
        // fake it so that there are no backups but also no NPEs
        when(supplier.get()).thenReturn(Collections.emptyList());
        return () -> supplier;
    }

    private Replication mockBackup() {
        return Mockito.mock(Replication.class);
    }
}
