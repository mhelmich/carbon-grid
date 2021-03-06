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

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.util.internal.SocketUtils;
import org.carbon.grid.BaseTest;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class GridCommunicationsTest extends BaseTest {
    @Test
    public void testBacklog() throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        short sender = 444;
        long lineId = 1234567890;
        InternalCache cacheMock = Mockito.mock(InternalCache.class);
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch threadEnteredHandler = new CountDownLatch(1);
        CountDownLatch threadFinishedHandling = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger(0);
        doAnswer(inv -> {
            Message.Response resp = inv.getArgumentAt(0, Message.Response.class);
            count.incrementAndGet();
            threadEnteredHandler.countDown();
            if (resp.getMessageSequenceNumber() == Integer.MIN_VALUE) {
                assertTrue(latch.await(TIMEOUT_SECS, TimeUnit.SECONDS));
            }
            return null;
        }).when(cacheMock).handleResponse(any(Message.Response.class));
        ExecutorService es = Executors.newFixedThreadPool(1);
        try {
            try (GridCommunications comm = mockGridCommunications(sender, 4444, cacheMock)) {
                NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>> backlogMap = getBacklogMap(comm);
                Message.ACK ack1 = new Message.ACK(Integer.MAX_VALUE, sender, lineId);
                Message.ACK ack2 = new Message.ACK(Integer.MIN_VALUE, sender, lineId);

                assertEquals(0, backlogMap.size());
                assertFalse(backlogMap.contains(comm.hashNodeIdCacheLineId(ack1.sender, ack1.lineId)));

                es.submit(() -> {
                    comm.handleMessage(ack2);
                    threadFinishedHandling.countDown();
                    return null;
                });
                assertTrue(threadEnteredHandler.await(TIMEOUT_SECS, TimeUnit.SECONDS));

                assertEquals(1, getBacklogMap(comm).size());
                ConcurrentLinkedQueue<Message> backlog = backlogMap.get(comm.hashNodeIdCacheLineId(ack1.sender, ack1.lineId));
                assertNotNull(backlog);
                // ack2 remains in the backlog until it finished processing
                assertEquals(1, backlog.size());

                comm.handleMessage(ack1);
                assertEquals(1, backlogMap.size());
                assertEquals(2, backlog.size());

                latch.countDown();
                assertTrue(threadFinishedHandling.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                assertEquals(0, backlog.size());

                assertEquals(2, count.get());
                backlog = backlogMap.get(comm.hashNodeIdCacheLineId(ack1.sender, ack1.lineId));
                assertNull(backlog);
            }
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testAddingToBacklogFromHandler() throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        short sender = 444;
        long lineId = 1234567890;
        InternalCache cacheMock = Mockito.mock(InternalCache.class);
        CountDownLatch threadEnteredHandler = new CountDownLatch(1);
        CountDownLatch threadBlocking = new CountDownLatch(1);
        CountDownLatch threadFinishedProcessing = new CountDownLatch(1);
        doAnswer(inv -> {
            threadEnteredHandler.countDown();
            threadBlocking.await(TIMEOUT_SECS, TimeUnit.SECONDS);
            return null;
        }).when(cacheMock).handleResponse(any(Message.Response.class));

        ExecutorService es = Executors.newFixedThreadPool(1);
        try {
            try (GridCommunications comm = mockGridCommunications(sender, 4444, cacheMock)) {
                NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>> backlogMap = getBacklogMap(comm);
                Message.ACK ack1 = new Message.ACK(Integer.MAX_VALUE, sender, lineId);
                Message.ACK ack2 = new Message.ACK(Integer.MIN_VALUE, sender, lineId);
                ConcurrentLinkedQueue<Message> backlog = backlogMap.get(comm.hashNodeIdCacheLineId(sender, lineId));
                assertNull(backlog);

                es.submit(() -> {
                    comm.handleMessage(ack2);
                    threadFinishedProcessing.countDown();
                    return null;
                });

                assertTrue(threadEnteredHandler.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                backlog = backlogMap.get(comm.hashNodeIdCacheLineId(sender, lineId));
                assertNotNull(backlog);
                assertEquals(1, backlog.size());
                assertTrue(comm.addToCacheLineBacklog(ack1));
                assertEquals(2, backlog.size());
                threadBlocking.countDown();
                assertTrue(threadFinishedProcessing.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                assertEquals(0, backlog.size());
                backlog = backlogMap.get(comm.hashNodeIdCacheLineId(sender, lineId));
                assertNull(backlog);
            }
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testReactToResponse() throws IOException, NoSuchFieldException, IllegalAccessException {
        short sender = 444;
        short newOwner = 888;
        long lineId = 1234567890;
        int messageRequestId = 987654321;
        InternalCache cacheMock = Mockito.mock(InternalCache.class);

        TcpGridClient mockClient = Mockito.mock(TcpGridClient.class);
        ChannelFuture mockFuture = Mockito.mock(ChannelFuture.class);
        when(mockClient.send(any(Message.class))).thenReturn(mockFuture);

        Message.GET requestToSend = new Message.GET(sender, lineId);
        Message.OWNER_CHANGED oc1 = new Message.OWNER_CHANGED(messageRequestId, (short)999, lineId, newOwner, MessageType.GET);
        try (GridCommunications comm = mockGridCommunications(sender, 4444, cacheMock)) {

            getNodeIdToClient(comm).put(newOwner, mockClient);
            NonBlockingHashMapLong<GridCommunications.LatchAndMessage> msgIdToLatch = getMessageIdToLatchAndMessage(comm);
            Message.GET get = new Message.GET(sender, lineId);
            get.messageSequenceNumber = messageRequestId;

            CompletableFuture<MessageType> latch = new CompletableFuture<>();
            msgIdToLatch.put(comm.hashNodeIdMessageSeq(comm.myNodeId(), oc1.getMessageSequenceNumber()), new GridCommunications.LatchAndMessage(latch, get));
            comm.reactToResponse(oc1, oc1.newOwner, requestToSend);
            assertNotNull(msgIdToLatch.get(comm.hashNodeIdMessageSeq(oc1.newOwner, requestToSend.getMessageSequenceNumber())));
        }
    }

    @Test
    public void testBroadcastWaitingForSpecificMessages() throws IOException, InterruptedException, TimeoutException, ExecutionException {
        short sender555 = 555;
        short sender666 = 666;
        short sender777 = 777;
        int port555 = 8888;
        int port666 = 9999;
        int port777 = 7777;
        long lineId = 1234567890;

        CountDownLatch receivedPut = new CountDownLatch(1);
        AtomicInteger responsesReceived = new AtomicInteger(0);
        CountDownLatch blockAckHandler = new CountDownLatch(1);

        InternalCache cacheMock555 = Mockito.mock(InternalCache.class);
        doAnswer(inv -> {
            Message.Response resp = inv.getArgumentAt(0, Message.Response.class);
            responsesReceived.incrementAndGet();
            if (MessageType.PUT.equals(resp.type)) {
                receivedPut.countDown();
            }
            return null;
        }).when(cacheMock555).handleResponse(any(Message.Response.class));

        InternalCache cacheMock666 = Mockito.mock(InternalCache.class);
        doAnswer(inv -> {
            Message.Request req = inv.getArgumentAt(0, Message.Request.class);
            assertTrue(blockAckHandler.await(TIMEOUT_SECS, TimeUnit.SECONDS));
            return new Message.ACK(req.messageSequenceNumber, sender666, lineId);
        }).when(cacheMock666).handleRequest(any(Message.Request.class));

        ByteBuf buffer = newRandomBuffer().retain();
        InternalCache cacheMock777 = Mockito.mock(InternalCache.class);
        doAnswer(inv -> {
            Message.Request req = inv.getArgumentAt(0, Message.Request.class);
            return new Message.PUT(req.messageSequenceNumber, sender777, lineId, 5, buffer);
        }).when(cacheMock777).handleRequest(any(Message.Request.class));

        try (GridCommunications comm555 = mockGridCommunications(sender555, port555, cacheMock555)) {
            try (GridCommunications comm666 = mockGridCommunications(sender666, port666, cacheMock666)) {
                try (GridCommunications comm777 = mockGridCommunications(sender777, port777, cacheMock777)) {

                    comm555.setPeers(ImmutableMap.of(
                            sender666, SocketUtils.socketAddress("localhost", port666),
                            sender777, SocketUtils.socketAddress("localhost", port777)
                    ));
                    comm666.setPeers(ImmutableMap.of(
                            sender555, SocketUtils.socketAddress("localhost", port555),
                            sender777, SocketUtils.socketAddress("localhost", port777)
                    ));
                    comm777.setPeers(ImmutableMap.of(
                            sender555, SocketUtils.socketAddress("localhost", port555),
                            sender666, SocketUtils.socketAddress("localhost", port666)
                    ));

                    Message.GET get = new Message.GET(sender555, lineId);
                    comm555.broadcast(get, MessageType.PUT).get(TIMEOUT_SECS, TimeUnit.SECONDS);

                    assertTrue(receivedPut.await(TIMEOUT_SECS, TimeUnit.SECONDS));
                    assertEquals(1, responsesReceived.get());
                    blockAckHandler.countDown();
                }
            }
        } finally {
            buffer.release();
        }
    }

    @SuppressWarnings("unchecked")
    private NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>> getBacklogMap(GridCommunications comms) throws NoSuchFieldException, IllegalAccessException {
        Field field = GridCommunications.class.getDeclaredField("cacheLineIdToBacklog");
        field.setAccessible(true);
        return (NonBlockingHashMapLong<ConcurrentLinkedQueue<Message>>) field.get(comms);
    }

    @SuppressWarnings("unchecked")
    private NonBlockingHashMapLong<GridCommunications.LatchAndMessage> getMessageIdToLatchAndMessage(GridCommunications comms) throws NoSuchFieldException, IllegalAccessException {
        Field field = GridCommunications.class.getDeclaredField("messageIdToLatchAndMessage");
        field.setAccessible(true);
        return (NonBlockingHashMapLong<GridCommunications.LatchAndMessage>) field.get(comms);
    }

    @SuppressWarnings("unchecked")
    private LoadingCache<Short, TcpGridClient> getNodeIdToClient(GridCommunications comms) throws IllegalAccessException, NoSuchFieldException {
        Field field = GridCommunications.class.getDeclaredField("nodeIdToClient");
        field.setAccessible(true);
        return (LoadingCache<Short, TcpGridClient>) field.get(comms);
    }

    private GridCommunications mockGridCommunications(int myNodeId, int port, InternalCache internalCache) {
        return new GridCommunications(mockNodeIdProvider((short)myNodeId), mockServerConfig(port), internalCache);
    }

    private Provider<Short> mockNodeIdProvider(short nodeId) {
        return () -> nodeId;
    }
}
