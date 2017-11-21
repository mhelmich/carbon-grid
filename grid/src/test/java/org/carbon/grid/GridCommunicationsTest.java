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

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
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

public class GridCommunicationsTest {
    private final static int TIMEOUT = 55;
    @Test
    public void testBacklog() throws IOException, InterruptedException {
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
                Message.ACK ack1 = new Message.ACK(Integer.MAX_VALUE, sender, lineId);
                Message.ACK ack2 = new Message.ACK(Integer.MIN_VALUE, sender, lineId);

                assertEquals(0, comm.getBacklogMap().size());
                assertFalse(comm.getBacklogMap().contains(comm.hashNodeIdCacheLineId(ack1.sender, ack1.lineId)));

                es.submit(() -> {
                    comm.handleMessage(ack2);
                    threadFinishedHandling.countDown();
                    return null;
                });
                assertTrue(threadEnteredHandler.await(TIMEOUT, TimeUnit.SECONDS));

                assertEquals(1, comm.getBacklogMap().size());
                ConcurrentLinkedQueue<Message> backlog = comm.getBacklogMap().get(comm.hashNodeIdCacheLineId(ack1.sender, ack1.lineId));
                assertNotNull(backlog);
                assertEquals(0, backlog.size());

                comm.handleMessage(ack1);
                assertEquals(1, comm.getBacklogMap().size());
                assertEquals(1, backlog.size());

                latch.countDown();
                assertTrue(threadFinishedHandling.await(TIMEOUT, TimeUnit.SECONDS));

                assertEquals(2, count.get());
                backlog = comm.getBacklogMap().get(comm.hashNodeIdCacheLineId(ack1.sender, ack1.lineId));
                assertNull(backlog);
            }
        } finally {
            es.shutdown();
        }
    }
}
