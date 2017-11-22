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
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CacheTest {
    @Test
    public void testPingPong() throws IOException, ExecutionException, InterruptedException {
        short node1 = 123;
        short node2 = 456;
        int port1 = 4444;
        int port2 = 5555;
        try (InternalCacheImpl cache123 = new InternalCacheImpl(node1, port1)) {
            try (InternalCacheImpl cache456 = new InternalCacheImpl(node2, port2)) {
                cache123.comms.addPeer(node2, "localhost", port2);
                cache456.comms.addPeer(node1, "localhost", port1);

                Message.GET get = new Message.GET(node1, 999L);
                Future<Void> f1 = cache123.comms.send(cache456.myNodeId, get);
                f1.get();
                assertTrue(f1.isDone());
            }
        }
    }

    @Test
    public void testGetPutCache() throws IOException {
        ThreeCaches threeCaches = createCluster();
        String testData = "testing_test";
        try {
            long newBlockId = threeCaches.cache123.allocateWithData(testData.getBytes());
            ByteBuf localBB = threeCaches.cache123.get(newBlockId);
            assertEqualsBites(testData.getBytes(), localBB);

            ByteBuf remoteBB = threeCaches.cache456.get(newBlockId);
            assertEqualsBites(testData.getBytes(), remoteBB);
        } finally {
            closeThreeCaches(threeCaches);
        }
    }

    @Test
    public void testAbsentLine() throws IOException {
        try (InternalCacheImpl cache123 = new InternalCacheImpl(123, 5555)) {
            ByteBuf buffer = cache123.get(123);
            assertNull(buffer);
        }
    }

    @Test
    public void testAllocateEmpty() throws IOException {
        try (InternalCacheImpl cache123 = new InternalCacheImpl(123, 5555)) {
            long emptyBlock = cache123.allocateEmpty();
            ByteBuf buffer = cache123.get(emptyBlock);
            assertNotNull(buffer);
            assertEquals(0, buffer.readableBytes());
        }
    }

    @Test
    public void testGetxPutxBasic() throws IOException {
        ThreeCaches threeCaches = createCluster();
        String testData = "testing_test";
        try {
            // set a line in cache123 and verify both caches
            long newCacheLineId = threeCaches.cache123.allocateWithData(testData.getBytes());
            ByteBuf localBB = threeCaches.cache123.get(newCacheLineId);
            assertEqualsBites(testData.getBytes(), localBB);
            CacheLine line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertNotNull(line123);
            assertEquals(CacheLineState.EXCLUSIVE, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId, line123.getOwner());
            CacheLine line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNull(line456);

            // get the line in cache456 and verify the state in cache456
            ByteBuf localBB2 = threeCaches.cache456.get(newCacheLineId);
            assertEqualsBites(testData.getBytes(), localBB2);
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            assertEquals(CacheLineState.SHARED, line456.getState());
            assertEquals(threeCaches.cache123.myNodeId, line456.getOwner());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.OWNED, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId, line123.getOwner());
            assertTrue(line123.getSharers().size() == 1);

            // transfer ownership to cache456
            ByteBuf remoteBB = threeCaches.cache456.getx(newCacheLineId);
            assertEqualsBites(testData.getBytes(), remoteBB);
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            // since there are no sharers this line will be in status EXCLUSIVE
            assertEquals(CacheLineState.EXCLUSIVE, line456.getState());
            assertEquals(threeCaches.cache456.myNodeId, line456.getOwner());
            assertTrue(line456.getSharers().isEmpty());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.INVALID, line123.getState());
            assertEquals(threeCaches.cache456.myNodeId, line123.getOwner());
        } finally {
            closeThreeCaches(threeCaches);
        }
    }

    @Test
    public void testGetxPutxThreeWay() throws IOException {
        ThreeCaches threeCaches = createCluster();
        String testData = "testing_test";
        try {
            // set a line in cache123 and verify all other caches
            // we expect nobody else to know about this cache line
            long newCacheLineId = threeCaches.cache123.allocateWithData(testData.getBytes());
            ByteBuf localBB = threeCaches.cache123.get(newCacheLineId);
            assertEqualsBites(testData.getBytes(), localBB);
            CacheLine line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertNotNull(line123);
            assertEquals(CacheLineState.EXCLUSIVE, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId, line123.getOwner());
            CacheLine line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNull(line456);
            CacheLine line789 = threeCaches.cache789.innerGetLineLocally(newCacheLineId);
            assertNull(line789);

            // get the line in cache456 and verify the state in cache456
            ByteBuf localBB2 = threeCaches.cache456.get(newCacheLineId);
            assertEqualsBites(testData.getBytes(), localBB2);
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            assertEquals(CacheLineState.SHARED, line456.getState());
            assertEquals(threeCaches.cache123.myNodeId, line456.getOwner());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.OWNED, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId, line123.getOwner());
            assertTrue(line123.getSharers().size() == 1);
            // pull the cache line into cache 3
            ByteBuf localBB3 = threeCaches.cache789.get(newCacheLineId);
            assertEqualsBites(testData.getBytes(), localBB3);
            line789 = threeCaches.cache789.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.SHARED, line789.getState());
            assertTrue(line123.getSharers().size() == 2);

            // transfer ownership to cache456
            ByteBuf remoteBB = threeCaches.cache456.getx(newCacheLineId);
            assertEqualsBites(testData.getBytes(), remoteBB);
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            // since there are no sharers this line will be in status EXCLUSIVE
            assertEquals(CacheLineState.OWNED, line456.getState());
            assertEquals(threeCaches.cache456.myNodeId, line456.getOwner());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.INVALID, line123.getState());
            assertEquals(threeCaches.cache456.myNodeId, line123.getOwner());
            // but as it turns out cache 789 never heard about the change in ownership
            assertEquals(threeCaches.cache123.myNodeId, line789.getOwner());
        } finally {
            closeThreeCaches(threeCaches);
        }
    }

    @Test
    public void testOwnershipMoveInvalidate() throws IOException {
        ThreeCaches threeCaches = createCluster();
        String testData = "testing_test";
        try {
            // set a line in cache123 and verify all other caches
            // we expect nobody else to know about this cache line
            long newCacheLineId = threeCaches.cache123.allocateWithData(testData.getBytes());
            ByteBuf localBB = threeCaches.cache123.get(newCacheLineId);
            // TODO - fix ref count
            assertEquals(1, localBB.refCnt());
            assertEqualsBites(testData.getBytes(), localBB);
            CacheLine line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertNotNull(line123);
            assertEquals(CacheLineState.EXCLUSIVE, line123.getState());
            assertEquals(threeCaches.cache123.myNodeId, line123.getOwner());
            CacheLine line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNull(line456);
            CacheLine line789 = threeCaches.cache789.innerGetLineLocally(newCacheLineId);
            assertNull(line789);
            threeCaches.cache789.get(newCacheLineId);
            line789 = threeCaches.cache789.innerGetLineLocally(newCacheLineId);
            assertNotNull(line789);

            // transfer ownership to cache456
            ByteBuf remoteBB = threeCaches.cache456.getx(newCacheLineId);
            // TODO - fix ref count
            assertEquals(1, remoteBB.refCnt());
            assertEqualsBites(testData.getBytes(), remoteBB);
            line456 = threeCaches.cache456.innerGetLineLocally(newCacheLineId);
            assertNotNull(line456);
            // since there are no sharers this line will be in status EXCLUSIVE
            assertEquals(CacheLineState.OWNED, line456.getState());
            assertEquals(threeCaches.cache456.myNodeId, line456.getOwner());
            line123 = threeCaches.cache123.innerGetLineLocally(newCacheLineId);
            assertEquals(CacheLineState.INVALID, line123.getState());
            assertEquals(threeCaches.cache456.myNodeId, line123.getOwner());
            // but as it turns out cache 789 never heard about the change in ownership
            assertEquals(threeCaches.cache123.myNodeId, line789.getOwner());

            // now we're trying to get the cache line from 789
            // that in turn should make a few round trips necessary
            threeCaches.cache789.get(newCacheLineId);
            assertEquals(CacheLineState.SHARED, line789.getState());
            // TODO -- fix that test and find out when to do a remote get
//            assertEquals(threeCaches.cache456.myNodeId, line789.getOwner());
            assertEquals(threeCaches.cache123.myNodeId, line789.getOwner());
        } finally {
            closeThreeCaches(threeCaches);
        }
    }

    private byte[] getAllBytesFromBuffer(ByteBuf buffer) {
        byte[] bites = new byte[buffer.readableBytes()];
        buffer.readBytes(bites, 0, bites.length);
        return bites;
    }

    private void assertEqualsBites(byte[] bites, ByteBuf buffer) {
        byte[] bufferBites = getAllBytesFromBuffer(buffer);
        assertTrue(Arrays.equals(bites, bufferBites));
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
        InternalCacheImpl internalCache1 = new InternalCacheImpl(node1, port1);
        InternalCacheImpl internalCache2 = new InternalCacheImpl(node2, port2);
        InternalCacheImpl internalCache3 = new InternalCacheImpl(node3, port3);

        internalCache1.comms.addPeer(node2, "localhost", port2);
        internalCache1.comms.addPeer(node3, "localhost", port3);
        internalCache2.comms.addPeer(node1, "localhost", port1);
        internalCache2.comms.addPeer(node3, "localhost", port3);
        internalCache3.comms.addPeer(node1, "localhost", port1);
        internalCache3.comms.addPeer(node2, "localhost", port2);

        return new ThreeCaches(internalCache1, internalCache2, internalCache3);
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
}