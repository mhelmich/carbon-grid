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

import io.netty.channel.ChannelFuture;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class GridCommunicationsTest {
    private final static int TIMEOUT = 55;
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
                assertTrue(latch.await(TIMEOUT, TimeUnit.SECONDS));
            }
            return null;
        }).when(cacheMock).handleResponse(any(Message.Response.class));
        ExecutorService es = Executors.newFixedThreadPool(1);
        try {
            try (GridCommunications comm = new GridCommunications(sender, 4444, cacheMock)) {
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
                assertTrue(threadEnteredHandler.await(TIMEOUT, TimeUnit.SECONDS));

                assertEquals(1, getBacklogMap(comm).size());
                ConcurrentLinkedQueue<Message> backlog = backlogMap.get(comm.hashNodeIdCacheLineId(ack1.sender, ack1.lineId));
                assertNotNull(backlog);
                assertEquals(0, backlog.size());

                comm.handleMessage(ack1);
                assertEquals(1, backlogMap.size());
                assertEquals(1, backlog.size());

                latch.countDown();
                assertTrue(threadFinishedHandling.await(TIMEOUT, TimeUnit.SECONDS));

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
            threadBlocking.await(TIMEOUT, TimeUnit.SECONDS);
            return null;
        }).when(cacheMock).handleResponse(any(Message.Response.class));

        ExecutorService es = Executors.newFixedThreadPool(1);
        try {
            try (GridCommunications comm = new GridCommunications(sender, 4444, cacheMock)) {
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

                assertTrue(threadEnteredHandler.await(TIMEOUT, TimeUnit.SECONDS));
                backlog = backlogMap.get(comm.hashNodeIdCacheLineId(sender, lineId));
                assertNotNull(backlog);
                assertEquals(0, backlog.size());
                assertTrue(comm.addToCacheLineBacklog(ack1));
                assertEquals(1, backlog.size());
                threadBlocking.countDown();
                assertTrue(threadFinishedProcessing.await(TIMEOUT, TimeUnit.SECONDS));
                backlog = backlogMap.get(comm.hashNodeIdCacheLineId(sender, lineId));
                assertNull(backlog);
            }
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testReactToResponse() throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
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
        try (GridCommunications comm = new GridCommunications(sender, 4444, cacheMock)) {

            getNodeIdToClient(comm).put(newOwner, mockClient);
            NonBlockingHashMapLong<GridCommunications.LatchAndMessage> msgIdToLatch = getMessageIdToLatchAndMessage(comm);
            Message.GET get = new Message.GET(sender, lineId);
            get.messageSequenceNumber = messageRequestId;

            CarbonCompletableFuture latch = new CarbonCompletableFuture();
            msgIdToLatch.put(comm.hashNodeIdMessageSeq(comm.myNodeId, oc1.getMessageSequenceNumber()), new GridCommunications.LatchAndMessage(latch, get));
            comm.reactToResponse(oc1, oc1.newOwner, requestToSend);
            assertNotNull(msgIdToLatch.get(comm.hashNodeIdMessageSeq(oc1.newOwner, requestToSend.getMessageSequenceNumber())));
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
    private NonBlockingHashMap<Short, TcpGridClient> getNodeIdToClient(GridCommunications comms) throws IllegalAccessException, NoSuchFieldException {
        Field field = GridCommunications.class.getDeclaredField("nodeIdToClient");
        field.setAccessible(true);
        return (NonBlockingHashMap<Short, TcpGridClient>) field.get(comms);
    }
}
