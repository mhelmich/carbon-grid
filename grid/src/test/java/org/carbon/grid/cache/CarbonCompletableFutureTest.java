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

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CarbonCompletableFutureTest {
    private final static int TIMEOUT = 555;
    @Test
    public void testBasic() {
        CarbonCompletableFuture f = new CarbonCompletableFuture();
        assertFalse(f.complete(null));
        assertTrue(f.isDone());

        f = new CarbonCompletableFuture();
        assertTrue(f.isDone());

        f = new CarbonCompletableFuture();
        assertTrue(f.complete(null, null));
        assertTrue(f.isDone());

        f = new CarbonCompletableFuture();
        assertTrue(f.complete(null, MessageType.GET));
        assertTrue(f.isDone());

        f = new CarbonCompletableFuture(MessageType.GET);
        assertTrue(f.complete(null, MessageType.GET));
        assertTrue(f.isDone());

        f = new CarbonCompletableFuture(MessageType.GET, MessageType.GET);
        assertFalse(f.complete(null, MessageType.GET));
        assertFalse(f.isDone());
        assertTrue(f.complete(null, MessageType.GET));
        assertTrue(f.isDone());

        f = new CarbonCompletableFuture(MessageType.GET);
        assertFalse(f.complete(null, MessageType.PUT));
        assertFalse(f.isDone());
        assertTrue(f.complete(null, MessageType.GET));
        assertTrue(f.isDone());

        f = new CarbonCompletableFuture(MessageType.GET, MessageType.PUT);
        assertFalse(f.complete(null, MessageType.GET));
        assertFalse(f.isDone());
        assertTrue(f.complete(null, MessageType.PUT));
        assertTrue(f.isDone());

        f = new CarbonCompletableFuture(MessageType.GET, MessageType.PUT);
        assertFalse(f.complete(null, MessageType.ACK));
        assertFalse(f.isDone());
        assertFalse(f.complete(null, MessageType.ACK));
        assertFalse(f.isDone());
        assertFalse(f.complete(null, MessageType.ACK));
        assertFalse(f.isDone());
        assertFalse(f.complete(null, MessageType.ACK));
        assertFalse(f.isDone());
        assertFalse(f.complete(null, MessageType.PUT));
        assertFalse(f.isDone());
        assertTrue(f.complete(null, MessageType.GET));
        assertTrue(f.isDone());

        f = new CarbonCompletableFuture(MessageType.GET, MessageType.PUT);
        assertFalse(f.complete(null, MessageType.ACK));
        assertFalse(f.isDone());
        assertFalse(f.complete(null, MessageType.ACK));
        assertFalse(f.isDone());
        assertFalse(f.complete(null, MessageType.ACK));
        assertFalse(f.isDone());
        assertFalse(f.complete(null, MessageType.PUT));
        assertFalse(f.isDone());
        assertFalse(f.complete(null, MessageType.ACK));
        assertFalse(f.isDone());
        assertTrue(f.complete(null, MessageType.GET));
        assertTrue(f.isDone());
    }

    @Test
    public void testNestedCompletableFutures() {
        CarbonCompletableFuture farAwayFuture = new CarbonCompletableFuture();
        assertTrue(farAwayFuture.isDone());

        CarbonCompletableFuture f1 = new CarbonCompletableFuture();
        CarbonCompletableFuture f2 = new CarbonCompletableFuture();
        CarbonCompletableFuture f3 = new CarbonCompletableFuture();
        CarbonCompletableFuture f4 = new CarbonCompletableFuture();

        farAwayFuture = new CarbonCompletableFuture();
        farAwayFuture.addFuture(f1);
        farAwayFuture.addFuture(f2);
        farAwayFuture.addFuture(f3);
        farAwayFuture.addFuture(f4);
        assertFalse(farAwayFuture.isDone());
        assertFalse(farAwayFuture.complete(null));
        assertFalse(farAwayFuture.complete(null));
        assertFalse(farAwayFuture.complete(null));
        assertFalse(farAwayFuture.complete(null));
        assertFalse(farAwayFuture.complete(null));
        assertFalse(farAwayFuture.isDone());

        f2.complete(null);
        assertFalse(farAwayFuture.isDone());

        f3.complete(null);
        f1.complete(null);
        assertFalse(farAwayFuture.isDone());

        f4.complete(null);
        assertTrue(farAwayFuture.isDone());
    }

    @Test
    public void testExceptionWithNestedCompletableFutures() {
        CarbonCompletableFuture farAwayFuture = new CarbonCompletableFuture();
        assertTrue(farAwayFuture.isDone());

        CarbonCompletableFuture f1 = new CarbonCompletableFuture();
        CarbonCompletableFuture f2 = new CarbonCompletableFuture();
        CarbonCompletableFuture f3 = new CarbonCompletableFuture();
        CarbonCompletableFuture f4 = new CarbonCompletableFuture();

        farAwayFuture = new CarbonCompletableFuture();
        farAwayFuture.addFuture(f1);
        farAwayFuture.addFuture(f2);
        farAwayFuture.addFuture(f3);
        farAwayFuture.addFuture(f4);

        f3.complete(null);
        f1.complete(null);
        assertFalse(farAwayFuture.isDone());

        f4.completeExceptionally(new Exception("BOOOOM -- planted on purpose"));
        assertFalse(farAwayFuture.isDone());

        f2.complete(null);
        assertTrue(farAwayFuture.isDone());
    }

    @Test
    public void testNestFutureConcurrently() throws InterruptedException {
        CarbonCompletableFuture farAwayFuture = new CarbonCompletableFuture();

        CarbonCompletableFuture f1 = new CarbonCompletableFuture();
        CarbonCompletableFuture f2 = new CarbonCompletableFuture();

        farAwayFuture.addFuture(f1);
        farAwayFuture.addFuture(f2);

        CountDownLatch threadFinished = new CountDownLatch(1);
        ExecutorService es = Executors.newFixedThreadPool(1);
        try {
            es.submit(() -> {
                try {
                    farAwayFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    // I hope this never happens
                    fail();
                }
                threadFinished.countDown();
            });

            f1.complete(null);
            assertEquals(1, threadFinished.getCount());

            f2.complete(null);
            assertTrue(threadFinished.await(TIMEOUT, TimeUnit.SECONDS));
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testNestFutureConcurrentlyFailure() throws InterruptedException {
        CarbonCompletableFuture farAwayFuture = new CarbonCompletableFuture();

        CarbonCompletableFuture f1 = new CarbonCompletableFuture();
        CarbonCompletableFuture f2 = new CarbonCompletableFuture();
        CarbonCompletableFuture f3 = new CarbonCompletableFuture();

        farAwayFuture.addFuture(f1);
        farAwayFuture.addFuture(f2);
        farAwayFuture.addFuture(f3);

        CountDownLatch threadRecordedException = new CountDownLatch(1);
        CountDownLatch threadFinished = new CountDownLatch(1);
        ExecutorService es = Executors.newFixedThreadPool(1);
        try {
            es.submit(() -> {
                try {
                    farAwayFuture.get();
                } catch (Exception xcp) {
                    threadRecordedException.countDown();
                }
                threadFinished.countDown();
            });

            f1.complete(null);
            assertEquals(1, threadRecordedException.getCount());

            f2.completeExceptionally(new Exception("BOOOM -- planted for you on purpose"));
            assertEquals(1, threadRecordedException.getCount());
            assertEquals(1, threadFinished.getCount());

            f3.complete(null);
            assertTrue(threadRecordedException.await(TIMEOUT, TimeUnit.SECONDS));
            assertTrue(threadFinished.await(TIMEOUT, TimeUnit.SECONDS));
        } finally {
            es.shutdown();
        }
    }
}
