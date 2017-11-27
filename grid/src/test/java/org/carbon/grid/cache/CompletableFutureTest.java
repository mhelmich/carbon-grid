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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompletableFutureTest {
    private final static int TIMEOUT = 555;
    @Test
    public void testUtilWaitForAll() {
        CompletableFuture<MessageType> f1 = new CompletableFuture<>();
        CompletableFuture<MessageType> f2 = new CompletableFuture<>();
        CompletableFuture<MessageType> f3 = new CompletableFuture<>();
        Set<CompletableFuture<MessageType>> futures = new HashSet<CompletableFuture<MessageType>>() {{
            add(f1);
            add(f2);
            add(f3);
        }};

        CompletableFuture<Void> f = CompletableFutureUtil.waitForAllMessages(futures);
        assertFalse(f.isDone());
        f1.complete(MessageType.GET);
        assertFalse(f.isDone());
        f2.complete(MessageType.PUT);
        assertFalse(f.isDone());
        f3.complete(MessageType.INVACK);
        assertTrue(f.isDone());
    }

    @Test
    public void testUtilWaitForFewTypeCompletesFirst() {
        CompletableFuture<MessageType> f1 = new CompletableFuture<>();
        CompletableFuture<MessageType> f2 = new CompletableFuture<>();
        CompletableFuture<MessageType> f3 = new CompletableFuture<>();
        Set<CompletableFuture<MessageType>> futures = new HashSet<CompletableFuture<MessageType>>() {{
            add(f1);
            add(f2);
            add(f3);
        }};

        CompletableFuture<Void> f = CompletableFutureUtil.waitForAllMessagesOrSpecifiedList(futures, MessageType.PUT);
        assertFalse(f.isDone());
        f1.complete(MessageType.GET);
        assertFalse(f.isDone());
        f2.complete(MessageType.PUT);
        assertTrue(f.isDone());
    }

    @Test
    public void testUtilWaitForFewMessagesCompleteFirst() {
        CompletableFuture<MessageType> f1 = new CompletableFuture<>();
        CompletableFuture<MessageType> f2 = new CompletableFuture<>();
        CompletableFuture<MessageType> f3 = new CompletableFuture<>();
        Set<CompletableFuture<MessageType>> futures = new HashSet<CompletableFuture<MessageType>>() {{
            add(f1);
            add(f2);
            add(f3);
        }};

        CompletableFuture<Void> farAwayFuture = CompletableFutureUtil.waitForAllMessagesOrSpecifiedList(futures, MessageType.PUT);
        assertFalse(farAwayFuture.isDone());
        f1.complete(MessageType.ACK);
        assertFalse(farAwayFuture.isDone());
        f2.complete(MessageType.ACK);
        assertFalse(farAwayFuture.isDone());
        f3.complete(MessageType.ACK);
        assertTrue(farAwayFuture.isDone());
    }

    @Test
    public void testUtilWaitForFewFailure() {
        CompletableFuture<MessageType> f1 = new CompletableFuture<>();
        CompletableFuture<MessageType> f2 = new CompletableFuture<>();
        CompletableFuture<MessageType> f3 = new CompletableFuture<>();
        Set<CompletableFuture<MessageType>> futures = new HashSet<CompletableFuture<MessageType>>() {{
            add(f1);
            add(f2);
            add(f3);
        }};

        CompletableFuture<Void> farAwayFuture = CompletableFutureUtil.waitForAllMessagesOrSpecifiedList(futures, MessageType.PUT);
        assertFalse(farAwayFuture.isDone());
        f1.complete(MessageType.ACK);
        assertFalse(farAwayFuture.isDone());
        f2.completeExceptionally(new Exception("BOOM -- this has been planted on purpose"));
        assertTrue(farAwayFuture.isDone());
        assertTrue(farAwayFuture.isCompletedExceptionally());
    }

    @Test
    public void testFutureConcurrently() throws InterruptedException {
        CompletableFuture<MessageType> f1 = new CompletableFuture<>();
        CompletableFuture<MessageType> f2 = new CompletableFuture<>();
        CompletableFuture<MessageType> f3 = new CompletableFuture<>();
        Set<CompletableFuture<MessageType>> futures = new HashSet<CompletableFuture<MessageType>>() {{
            add(f1);
            add(f2);
            add(f3);
        }};

        CompletableFuture<Void> farAwayFuture = CompletableFutureUtil.waitForAllMessagesOrSpecifiedList(futures, MessageType.PUT);

        CountDownLatch threadFinished = new CountDownLatch(1);
        ExecutorService es = Executors.newFixedThreadPool(1);
        try {
            es.submit(() -> {
                try {
                    farAwayFuture.get(TIMEOUT, TimeUnit.SECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    fail();
                }
                threadFinished.countDown();
            });

            f1.complete(MessageType.ACK);
            f2.complete(MessageType.ACK);
            f3.complete(MessageType.ACK);

            assertTrue(threadFinished.await(TIMEOUT, TimeUnit.SECONDS));
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testFutureConcurrentlyFailure() throws InterruptedException {
        CompletableFuture<MessageType> f1 = new CompletableFuture<>();
        CompletableFuture<MessageType> f2 = new CompletableFuture<>();
        CompletableFuture<MessageType> f3 = new CompletableFuture<>();
        Set<CompletableFuture<MessageType>> futures = new HashSet<CompletableFuture<MessageType>>() {{
            add(f1);
            add(f2);
            add(f3);
        }};

        CompletableFuture<Void> farAwayFuture = CompletableFutureUtil.waitForAllMessagesOrSpecifiedList(futures, MessageType.PUT);

        CountDownLatch threadFinished = new CountDownLatch(1);
        CountDownLatch threadCaughtException = new CountDownLatch(1);
        ExecutorService es = Executors.newFixedThreadPool(1);
        try {
            es.submit(() -> {
                try {
                    farAwayFuture.get(TIMEOUT, TimeUnit.SECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    threadCaughtException.countDown();
                }
                threadFinished.countDown();
            });

            f1.complete(MessageType.ACK);
            f3.completeExceptionally(new Exception("BOOOM -- this has been planted on purpose"));

            assertTrue(threadCaughtException.await(TIMEOUT, TimeUnit.SECONDS));
            assertTrue(threadFinished.await(TIMEOUT, TimeUnit.SECONDS));
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testMultipleSpecificMessages() {
        CompletableFuture<MessageType> f1 = new CompletableFuture<>();
        CompletableFuture<MessageType> f2 = new CompletableFuture<>();
        CompletableFuture<MessageType> f3 = new CompletableFuture<>();
        Set<CompletableFuture<MessageType>> futures = new HashSet<CompletableFuture<MessageType>>() {{
            add(f1);
            add(f2);
            add(f3);
        }};

        CompletableFuture<Void> farAwayFuture = CompletableFutureUtil.waitForAllMessagesOrSpecifiedList(futures, MessageType.PUT, MessageType.PUT);
        assertFalse(farAwayFuture.isDone());
        f1.complete(MessageType.PUT);
        assertFalse(farAwayFuture.isDone());
        f2.complete(MessageType.PUT);
        assertTrue(farAwayFuture.isDone());
    }
}
