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

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is a little bit of a misch-masch of two different concepts.
 * Generally you can think of it as a composite future waiting for other futures to complete (or messages to arrive).
 * But this future has two modes:
 * - one in which after creation of this future it can collect multiple other futures and wait on ALL of them
 * --- all futures need to complete successful or unsuccessful
 * - in the other mode, you can provide a list of message types that it's supposed to listen for
 * --- as soon as these messages types (also in their counts) have been observed, the future as a whole completes
 */
class CarbonCompletableFuture extends CompletableFuture<Void> {
    private final boolean shouldWaitForAll;
    private final Collection<CompletableFuture<Void>> futuresSoFar;
    private final AtomicReference<CompletableFuture<Void>> farAwayFuture;
    private final NonBlockingHashMap<MessageType, AtomicInteger> typeToCount;

    CarbonCompletableFuture() {
        this.shouldWaitForAll = true;
        this.typeToCount = null;
        this.futuresSoFar = new LinkedList<>();
        this.farAwayFuture = new AtomicReference<>(null);
    }

    CarbonCompletableFuture(MessageType... messagesToWaitFor) {
        this.shouldWaitForAll = false;
        this.typeToCount = new NonBlockingHashMap<>();
        for (MessageType type : messagesToWaitFor) {
            this.typeToCount.putIfAbsent(type, new AtomicInteger(0));
            this.typeToCount.get(type).incrementAndGet();
        }
        this.futuresSoFar = null;
        this.farAwayFuture = null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (shouldWaitForAll) {
            bakeFarAwayFuture();
            return farAwayFuture.get().cancel(mayInterruptIfRunning);
        } else {
            return super.cancel(mayInterruptIfRunning);
        }
    }

    @Override
    public boolean isCancelled() {
        if (shouldWaitForAll) {
            bakeFarAwayFuture();
            return farAwayFuture.get().isCancelled();
        } else {
            return super.isCancelled();
        }
    }

    @Override
    public boolean isDone() {
        if (shouldWaitForAll) {
            bakeFarAwayFuture();
            return farAwayFuture.get().isDone();
        } else {
            return super.isDone();
        }
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        if (shouldWaitForAll) {
            bakeFarAwayFuture();
            return farAwayFuture.get().get();
        } else {
            return super.get();
        }
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (shouldWaitForAll) {
            bakeFarAwayFuture();
            return farAwayFuture.get().get(timeout, unit);
        } else {
            return super.get(timeout, unit);
        }
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return !shouldWaitForAll && super.completeExceptionally(ex);
    }

    @Override
    public boolean complete(Void value) {
        return !shouldWaitForAll && super.complete(value);
    }

    boolean complete(Void value, MessageType type) {
        if (shouldWaitForAll) {
            return super.complete(value);
        } else {
            AtomicInteger count = typeToCount.get(type);
            if (count != null) {
                if (count.decrementAndGet() <= 0) {
                    typeToCount.remove(type);
                }
            }

            return typeToCount.isEmpty() && super.complete(value);
        }
    }


    //
    void addFuture(CompletableFuture<Void> f) {
        assert shouldWaitForAll;
        assert farAwayFuture.get() == null;
        futuresSoFar.add(f);
    }

    private void bakeFarAwayFuture() {
        farAwayFuture.compareAndSet(null, CompletableFuture.allOf(futuresSoFar.toArray(new CompletableFuture<?>[0])));
    }
}
